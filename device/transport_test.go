package device

import (
	"context"

	"encoding/json"
	"encoding/pem"

	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
)

func wsClient(t *testing.T, s *httptest.Server) *http.Client {
	t.Helper()
	roots, err := hostTLS(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: s.Certificate().Raw}))
	if err != nil {
		t.Fatal(err)
	}
	client := &http.Client{Transport: &http.Transport{TLSClientConfig: roots}}
	t.Cleanup(client.CloseIdleConnections)
	return client
}
func TestHostAuthenticatedPCMAndStateEpoch(t *testing.T) {
	c, now, _ := controller(t)
	_ = now
	first := make(chan *websocket.Conn, 1)
	received := make(chan []byte, 100)
	var authorized atomic.Bool
	s := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer synthetic-host-token" {
			w.WriteHeader(401)
			return
		}
		authorized.Store(true)
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.CloseNow()
		first <- conn
		conn.Write(r.Context(), websocket.MessageText, []byte(`{"type":"ready"}`))
		for {
			kind, b, err := conn.Read(r.Context())
			if err != nil {
				return
			}
			if kind == websocket.MessageBinary {
				received <- b
			}
		}
	}))
	defer s.Close()
	cfg := Config{ServerURL: "wss" + strings.TrimPrefix(s.URL, "https") + "/v1/listen"}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { runHost(ctx, cfg, c, wsClient(t, s), "synthetic-host-token"); close(done) }()
	defer func() { cancel(); <-done }()
	select {
	case <-first:
	case <-time.After(2 * time.Second):
		t.Fatal("no connection")
	}
	deadline := time.Now().Add(2 * time.Second)
	for !c.Status(time.Now()).Ready && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if !authorized.Load() || !c.Status(time.Now()).Ready {
		t.Fatal("host not authenticated/ready")
	}
	c.AcceptPCM(make([]byte, 6400), time.Now())
	select {
	case b := <-received:
		if len(b) != 3200 {
			t.Fatalf("bad PCM batch %d", len(b))
		}
	case <-time.After(time.Second):
		t.Fatal("no PCM delivered")
	}
	// Stop reads any already-written frame, then verifies export remains off.
	c.Apply("off", time.Now())
	time.Sleep(150 * time.Millisecond)
	for len(received) > 0 {
		<-received
	}
	for i := 0; i < 5; i++ {
		c.AcceptPCM(loud(), time.Now())
		time.Sleep(20 * time.Millisecond)
	}
	select {
	case <-received:
		t.Fatal("audio exported while off")
	default:
	}
}
func TestHostNotReadyAndReconnectClearAllQueues(t *testing.T) {
	c, now, id := controller(t)
	now = silence(c, now, 1500*time.Millisecond)
	offer(t, c, id, now, "Не пережить разрыв.")
	connected := make(chan struct{}, 1)
	s := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.CloseNow()
		select {
		case connected <- struct{}{}:
		default:
		}
		for {
			if _, _, err = conn.Read(r.Context()); err != nil {
				return
			}
		}
	}))
	defer s.Close()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		runHost(ctx, Config{ServerURL: "wss" + strings.TrimPrefix(s.URL, "https") + "/v1/listen"}, c, wsClient(t, s), "synthetic-host-token")
		close(done)
	}()
	select {
	case <-connected:
	case <-time.After(time.Second):
		t.Fatal("no connection")
	}
	time.Sleep(30 * time.Millisecond)
	if c.Status(time.Now()).Ready {
		t.Fatal("connection without ready was ready")
	}
	if _, ok := c.TakeSpeech(time.Now()); ok {
		t.Fatal("old reply survived reconnect")
	}
	cancel()
	<-done
	if c.Status(time.Now()).Ready {
		t.Fatal("shutdown left ready")
	}
}
func TestNativeExactFormACKAndOwnedStop(t *testing.T) {
	commands := make(chan map[string]any, 10)
	s := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.CloseNow()
		for {
			_, b, err := conn.Read(r.Context())
			if err != nil {
				return
			}
			var env map[string]any
			json.Unmarshal(b, &env)
			if env["conversationToken"] != "synthetic-native-token" {
				return
			}
			payload := env["payload"].(map[string]any)
			commands <- payload
			answer := map[string]any{"requestId": env["id"], "status": "SUCCESS", "state": map[string]any{"aliceState": "IDLE"}}
			b, _ = json.Marshal(answer)
			conn.Write(r.Context(), websocket.MessageText, b)
		}
	}))
	defer s.Close()
	pinned, err := PinnedTLS(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: s.Certificate().Raw}))
	if err != nil {
		t.Fatal(err)
	}
	client := &http.Client{Transport: &http.Transport{TLSClientConfig: pinned}}
	defer client.CloseIdleConnections()
	c, _, _ := controller(t)
	n := newNative("wss"+strings.TrimPrefix(s.URL, "https"), "synthetic-native-token", client, c)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { n.run(ctx); close(done) }()
	defer func() { cancel(); <-done }()
	select {
	case cmd := <-commands:
		if cmd["command"] != "softwareVersion" {
			t.Fatal("missing native auth probe")
		}
	case <-time.After(time.Second):
		t.Fatal("no native session")
	}
	deadline := time.Now().Add(time.Second)
	for !n.available() && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	ack, err := n.beginSpeech(ctx, 42, "Тестовая реплика.")
	if err != nil {
		t.Fatal(err)
	}
	select {
	case ok := <-ack:
		if !ok {
			t.Fatal("native ACK failed")
		}
	case <-time.After(time.Second):
		t.Fatal("no exact ACK")
	}
	cmd := <-commands
	event := cmd["serverActionEventPayload"].(map[string]any)
	update := event["payload"].(map[string]any)["form_update"].(map[string]any)
	if cmd["command"] != "serverAction" || event["name"] != "update_form" || update["name"] != "personal_assistant.scenarios.quasar.iot.repeat_phrase" {
		t.Fatalf("wrong native form: %v", cmd)
	}
	slot := update["slots"].([]any)[0].(map[string]any)
	if slot["name"] != "phrase_to_repeat" || slot["value"] != "Тестовая реплика." || slot["type"] != "string" {
		t.Fatal("wrong phrase slot")
	}
	n.stopOwned(41)
	select {
	case <-commands:
		t.Fatal("foreign stop sent")
	default:
	}
	before := time.Now()
	n.stopOwned(42)
	if time.Since(before) > 200*time.Millisecond {
		t.Fatal("stop delayed direct")
	}
	select {
	case cmd := <-commands:
		if cmd["command"] != "stop" {
			t.Fatal(cmd)
		}
	case <-time.After(time.Second):
		t.Fatal("owned stop absent")
	}
	n.stopOwned(42)
	select {
	case <-commands:
		t.Fatal("duplicate stop")
	case <-time.After(30 * time.Millisecond):
	}
}
func TestNativePlayingFieldBlocksPCM(t *testing.T) {
	s := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.CloseNow()
		_, b, err := conn.Read(r.Context())
		if err != nil {
			return
		}
		var env struct {
			ID string `json:"id"`
		}
		json.Unmarshal(b, &env)
		b, _ = json.Marshal(map[string]any{"requestId": env.ID, "status": "SUCCESS", "state": map[string]any{"aliceState": "NONE", "playing": true}})
		conn.Write(r.Context(), websocket.MessageText, b)
		for {
			if _, _, err = conn.Read(r.Context()); err != nil {
				return
			}
		}
	}))
	defer s.Close()
	c, _, _ := controller(t)
	n := newNative("wss"+strings.TrimPrefix(s.URL, "https"), "synthetic-native-token", wsClient(t, s), c)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { n.run(ctx); close(done) }()
	defer func() { cancel(); <-done }()
	deadline := time.Now().Add(time.Second)
	for !n.available() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if !n.available() {
		t.Fatal("native unavailable")
	}
	if c.AcceptPCM(make([]byte, 3200), time.Now()) || !c.Status(time.Now()).Blocked {
		t.Fatal("native music state.playing did not block ambient export")
	}
}

func TestNativeCancelledACKClearsPending(t *testing.T) {
	commands := make(chan string, 10)
	s := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.CloseNow()
		for {
			_, b, err := conn.Read(r.Context())
			if err != nil {
				return
			}
			var env struct {
				ID      string `json:"id"`
				Payload struct {
					Command string `json:"command"`
				} `json:"payload"`
			}
			json.Unmarshal(b, &env)
			commands <- env.Payload.Command
			if env.Payload.Command == "softwareVersion" {
				b, _ = json.Marshal(map[string]any{"requestId": env.ID, "status": "SUCCESS"})
				conn.Write(r.Context(), websocket.MessageText, b)
			}
		}
	}))
	defer s.Close()
	c, _, _ := controller(t)
	n := newNative("wss"+strings.TrimPrefix(s.URL, "https"), "synthetic-native-token", wsClient(t, s), c)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { n.run(ctx); close(done) }()
	defer func() { cancel(); <-done }()
	<-commands
	deadline := time.Now().Add(time.Second)
	for !n.available() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	for epoch := uint64(1); epoch <= 3; epoch++ {
		speechCtx, stop := context.WithCancel(ctx)
		ack, err := n.beginSpeech(speechCtx, epoch, "Без подтверждения.")
		if err != nil {
			t.Fatal(err)
		}
		<-commands
		stop()
		select {
		case ok := <-ack:
			if ok {
				t.Fatal("cancellation succeeded")
			}
		case <-time.After(150 * time.Millisecond):
			t.Fatal("cancelled native ACK still waiting")
		}
		select {
		case command := <-commands:
			if command != "stop" {
				t.Fatal(command)
			}
		case <-time.After(time.Second):
			t.Fatal("cancel did not stop owned phrase")
		}
	}
}

func TestHostWithoutPongLosesReadiness(t *testing.T) {
	release := make(chan struct{})
	s := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.CloseNow()
		conn.Write(r.Context(), websocket.MessageText, []byte(`{"type":"ready"}`))
		<-release
	}))
	defer func() { close(release); s.Close() }()
	c, _, _ := controller(t)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		runHost(ctx, Config{ServerURL: "wss" + strings.TrimPrefix(s.URL, "https") + "/v1/listen"}, c, wsClient(t, s), "synthetic-host-token")
		close(done)
	}()
	defer func() { cancel(); <-done }()
	deadline := time.Now().Add(time.Second)
	for !c.Status(time.Now()).Ready && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if !c.Status(time.Now()).Ready {
		t.Fatal("did not become ready")
	}
	time.Sleep(1700 * time.Millisecond)
	if c.Status(time.Now()).Ready || !c.Status(time.Now()).Enabled {
		t.Fatal("dead host retained readiness or changed desired mode")
	}
}

func TestHostRedirectMustNotFollow(t *testing.T) {
	var redirected atomic.Bool
	target := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		redirected.Store(true)
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.CloseNow()
		conn.Write(r.Context(), websocket.MessageText, []byte(`{"type":"ready"}`))
		for {
			if _, _, err = conn.Read(r.Context()); err != nil {
				return
			}
		}
	}))
	defer target.Close()
	origin := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL+"/v1/listen", http.StatusTemporaryRedirect)
	}))
	defer origin.Close()
	c, _, _ := controller(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		runHost(ctx, Config{ServerURL: "wss" + strings.TrimPrefix(origin.URL, "https") + "/v1/listen"}, c, wsClient(t, origin), "synthetic-host-token")
		close(done)
	}()
	time.Sleep(150 * time.Millisecond)
	cancel()
	<-done
	if redirected.Load() {
		t.Fatal("followed authenticated host redirect")
	}
}

func TestHostRootsRejectWrongServer(t *testing.T) {
	cfg, err := hostTLS([]byte("bad"))
	if err == nil || cfg != nil {
		t.Fatal("invalid roots accepted")
	}

}
