package device

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"net/http"
	"sync"
	"time"

	"github.com/coder/websocket"
)

func pause(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
func writeJSON(ctx context.Context, conn *websocket.Conn, value any) error {
	b, err := json.Marshal(value)
	if err != nil {
		return err
	}
	bounded, cancel := context.WithTimeout(ctx, 30*time.Millisecond)
	defer cancel()
	return conn.Write(bounded, websocket.MessageText, b)
}
func noRedirect(client *http.Client) *http.Client {
	safe := *client
	safe.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return &safe
}
func runHost(ctx context.Context, cfg Config, c *Controller, client *http.Client, token string) {
	client = noRedirect(client)
	for ctx.Err() == nil {
		id := c.Connect(time.Now())
		dialCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		conn, _, err := websocket.Dial(dialCtx, cfg.ServerURL, &websocket.DialOptions{HTTPClient: client, HTTPHeader: http.Header{"Authorization": []string{"Bearer " + token}}, CompressionMode: websocket.CompressionDisabled})
		cancel()
		if err == nil {
			hostSession(ctx, conn, c, id)
			conn.CloseNow()
		}
		c.Disconnect(id, time.Now())
		if !pause(ctx, time.Second) {
			return
		}
	}
}
func watchPong(ctx context.Context, cancel context.CancelFunc, conn *websocket.Conn) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		pulse := time.NewTicker(time.Second)
		defer pulse.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-pulse.C:
				ping, stop := context.WithTimeout(ctx, 500*time.Millisecond)
				err := conn.Ping(ping)
				stop()
				if err != nil {
					cancel()
					return
				}
			}
		}
	}()
	return done
}
func hostSession(parent context.Context, conn *websocket.Conn, c *Controller, id uint64) {
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	conn.SetReadLimit(4096)
	pingDone := watchPong(ctx, cancel, conn)
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer cancel()
		for {
			kind, b, err := conn.Read(ctx)
			if err != nil {
				return
			}
			if kind != websocket.MessageText {
				return
			}
			var m Message
			if strictJSON(b, &m) != nil {
				return
			}
			if m.Type == "ready" {
				c.Ready(id)
			} else if m.Type == "reply" || m.Type == "cancel" {
				c.Receive(id, m, time.Now())
			} else {
				return
			}
		}
	}()
	defer func() { cancel(); conn.CloseNow(); <-done; <-pingDone }()
	pulse := time.NewTicker(50 * time.Millisecond)
	defer pulse.Stop()
	started := time.Now()
	lastState := time.Time{}
	for {
		select {
		case <-ctx.Done():
			return
		case <-pulse.C:
		case <-c.changed:
		}
		now := time.Now()
		c.mu.Lock()
		if id != c.connection || !c.connected {
			c.mu.Unlock()
			return
		}
		if !c.ready && now.Sub(started) > 3*time.Second {
			c.mu.Unlock()
			return
		}
		if now.Sub(lastState) >= 250*time.Millisecond || len(c.events) > 0 {
			for _, m := range c.events {
				if err := writeJSON(ctx, conn, m); err != nil {
					c.mu.Unlock()
					return
				}
			}
			c.events = nil
			state := struct {
				Type    string `json:"type"`
				Enabled bool   `json:"enabled"`
				Blocked bool   `json:"blocked"`
				Epoch   uint64 `json:"epoch"`
			}{"state", c.enabled, c.blocked(now), c.epoch}
			if err := writeJSON(ctx, conn, state); err != nil {
				c.mu.Unlock()
				return
			}
			lastState = now
		}
		// One 100ms batch per turn. No detached snapshot can outlive cancellation.
		if c.blocked(now) {
			c.clearAudio()
		} else if len(c.audio) > 0 {
			b := c.audio[0]
			c.audio = c.audio[1:]
			c.queued -= len(b)
			bounded, stop := context.WithTimeout(ctx, 30*time.Millisecond)
			err := conn.Write(bounded, websocket.MessageBinary, b)
			stop()
			if err != nil {
				c.mu.Unlock()
				return
			}
		}
		c.mu.Unlock()
	}
}

type native struct {
	mu         sync.Mutex
	url, token string
	tokenFile  string
	client     *http.Client
	controller *Controller
	conn       *websocket.Conn
	ready      bool
	owner      uint64
	requestID  string
	pending    map[string]chan bool
}

func newNative(url, token string, client *http.Client, c *Controller) *native {
	return &native{url: url, token: token, client: noRedirect(client), controller: c, pending: make(map[string]chan bool)}
}
func (n *native) available() bool { n.mu.Lock(); defer n.mu.Unlock(); return n.ready }
func (n *native) run(ctx context.Context) {
	for ctx.Err() == nil {
		if n.tokenFile != "" {
			token, err := readToken(n.tokenFile)
			if err != nil {
				if !pause(ctx, time.Second) {
					return
				}
				continue
			}
			n.mu.Lock()
			n.token = token
			n.mu.Unlock()
		}
		dial, cancel := context.WithTimeout(ctx, 3*time.Second)
		conn, _, err := websocket.Dial(dial, n.url, &websocket.DialOptions{HTTPClient: n.client, CompressionMode: websocket.CompressionDisabled})
		cancel()
		if err == nil {
			n.session(ctx, conn)
			conn.CloseNow()
		}
		n.controller.NativeAvailable(false, time.Now())
		if !pause(ctx, time.Second) {
			return
		}
	}
}
func (n *native) session(parent context.Context, conn *websocket.Conn) {
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	conn.SetReadLimit(65536)
	pingDone := watchPong(ctx, cancel, conn)
	n.mu.Lock()
	n.conn = conn
	n.ready = false
	n.mu.Unlock()
	defer func() {
		n.mu.Lock()
		if n.conn == conn {
			n.conn = nil
			n.ready = false
			n.owner = 0
			for id, ch := range n.pending {
				ch <- false
				delete(n.pending, id)
			}
		}
		n.mu.Unlock()
	}()
	readDone := make(chan struct{})
	go func() {
		defer close(readDone)
		defer cancel()
		for {
			kind, b, err := conn.Read(ctx)
			if err != nil || kind != websocket.MessageText {
				return
			}
			var msg struct {
				RequestID string `json:"requestId"`
				Status    string `json:"status"`
				State     struct {
					AliceState string `json:"aliceState"`
					Playing    *bool  `json:"playing"`
				} `json:"state"`
			}
			if json.Unmarshal(b, &msg) != nil {
				return
			}
			n.mu.Lock()
			if ch, ok := n.pending[msg.RequestID]; ok {
				ch <- msg.Status == "SUCCESS"
				delete(n.pending, msg.RequestID)
			}
			n.mu.Unlock()
			if msg.State.Playing != nil {
				n.controller.SetMediaPlaying(*msg.State.Playing, time.Now())
			}
			if msg.State.AliceState != "" {
				n.controller.ObserveNative(msg.State.AliceState, time.Now())
			}
		}
	}()
	defer func() { cancel(); conn.CloseNow(); <-readDone; <-pingDone }()
	n.mu.Lock()
	_, auth, err := n.sendLocked(ctx, map[string]any{"command": "softwareVersion"}, true)
	n.mu.Unlock()
	if err != nil {
		return
	}
	select {
	case ok := <-auth:
		if !ok {
			return
		}
	case <-time.After(3 * time.Second):
		return
	case <-ctx.Done():
		return
	}
	n.mu.Lock()
	n.ready = true
	n.mu.Unlock()
	n.controller.NativeAvailable(true, time.Now())
	<-ctx.Done()
}
func (n *native) sendLocked(ctx context.Context, payload any, wait bool) (string, chan bool, error) {
	if n.conn == nil {
		return "", nil, errors.New("native unavailable")
	}
	id := rand.Text()
	var ack chan bool
	if wait {
		if len(n.pending) >= 2 {
			return "", nil, errors.New("native request limit")
		}
		ack = make(chan bool, 1)
		n.pending[id] = ack
	}
	env := struct {
		Token    string `json:"conversationToken"`
		ID       string `json:"id"`
		Payload  any    `json:"payload"`
		SentTime int64  `json:"sentTime"`
	}{n.token, id, payload, time.Now().UnixMilli()}
	err := writeJSON(ctx, n.conn, env)
	if err != nil {
		delete(n.pending, id)
	}
	return id, ack, err
}
func (n *native) beginSpeech(ctx context.Context, epoch uint64, text string) (<-chan bool, error) {
	if !validReply(text) {
		return nil, errors.New("invalid phrase")
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if !n.ready {
		return nil, errors.New("native unavailable")
	}
	payload := map[string]any{"command": "serverAction", "serverActionEventPayload": map[string]any{"type": "server_action", "name": "update_form", "payload": map[string]any{"form_update": map[string]any{"name": "personal_assistant.scenarios.quasar.iot.repeat_phrase", "slots": []any{map[string]any{"type": "string", "name": "phrase_to_repeat", "value": text}}}, "resubmit": true}}}
	id, ack, err := n.sendLocked(ctx, payload, true)
	if err != nil {
		return nil, err
	}
	n.owner = epoch
	n.requestID = id
	out := make(chan bool, 1)
	go func() {
		select {
		case ok := <-ack:
			out <- ok
		case <-ctx.Done():
			n.stopOwned(epoch)
			out <- false
		}
	}()
	return out, nil
}

// No queued stop: a delayed stop could stop a later direct/native response.
func (n *native) stopOwned(epoch uint64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.owner != epoch || n.conn == nil || !n.ready {
		return
	}
	n.owner = 0
	delete(n.pending, n.requestID)
	n.requestID = ""
	n.sendLocked(context.Background(), map[string]any{"command": "stop"}, false)
}
func (n *native) release(epoch uint64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.owner == epoch {
		n.owner = 0
		delete(n.pending, n.requestID)
		n.requestID = ""
	}
}
