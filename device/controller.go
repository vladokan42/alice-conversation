package device

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"
	"unicode"
	"unicode/utf8"
)

const (
	micTimeout     = 350 * time.Millisecond
	quietPeriod    = 1200 * time.Millisecond
	cooldown       = 15 * time.Second
	directPeriod   = 45 * time.Second
	replyTTL       = 8 * time.Second
	ownSpeechLimit = 30 * time.Second
	maxPCM         = 6400
	maxQueue       = 64000 // Two seconds of PCM16/16kHz/mono.
)

type Status struct {
	Enabled  bool   `json:"enabled"`
	Blocked  bool   `json:"blocked"`
	Ready    bool   `json:"ready"`
	Speaking bool   `json:"speaking"`
	Epoch    uint64 `json:"epoch"`
}
type Message struct {
	Type  string `json:"type"`
	Epoch uint64 `json:"epoch"`
	Text  string `json:"text,omitempty"`
}
type Result struct {
	Answer  string `json:"answer"`
	Enabled bool   `json:"enabled"`
}
type Speech struct {
	Epoch uint64
	Text  string
}
type pendingReply struct {
	Speech
	expires time.Time
}

// Controller owns RAM-only audio/replies. Its mutex also serializes the final
// bounded native write with cancellation; a direct request cannot overtake it.
type Controller struct {
	mu                                                                                       sync.Mutex
	actionMu                                                                                 sync.Mutex
	capture                                                                                  func(bool) error
	cfg                                                                                      Config
	enabled, ready, nativeReady, nativeBusy, nativeIdle, mic, talking, speaking, seenOwnBusy bool
	epoch, connection                                                                        uint64
	connected                                                                                bool
	mediaPlaying                                                                             bool
	lastPCM, lastLoud, lastSpoke, directUntil, ownUntil, echoUntil                           time.Time
	audio                                                                                    [][]byte
	partial                                                                                  []byte
	queued                                                                                   int
	pending                                                                                  *pendingReply
	events                                                                                   []Message
	changed                                                                                  chan struct{}
	speechCancel                                                                             context.CancelFunc
	interrupt                                                                                func(uint64) // Bounded, currently-owned stop only.
	release                                                                                  func(uint64) // Retire ownership before cancelling a completed worker.
}

func NewController(cfg Config) (*Controller, error) {
	if !filepath.IsAbs(cfg.StatePath) {
		return nil, errors.New("state_path must be absolute")
	}
	if cfg.QuietRMS == 0 {
		cfg.QuietRMS = .02
	}
	if math.IsNaN(cfg.QuietRMS) || cfg.QuietRMS < .001 || cfg.QuietRMS > .5 {
		return nil, errors.New("quiet_rms outside .001..0.5")
	}
	c := &Controller{cfg: cfg, enabled: true, epoch: 1, changed: make(chan struct{}, 1)}
	b, err := readPrivate(cfg.StatePath, 4096)
	if errors.Is(err, os.ErrNotExist) {
		err = saveEnabled(cfg.StatePath, true)
	} else if err == nil {
		var s struct {
			Enabled *bool `json:"enabled"`
		}
		err = strictJSON(b, &s)
		if err == nil {
			if s.Enabled == nil {
				err = errors.New("missing enabled")
			} else {
				c.enabled = *s.Enabled
			}
		}
	}
	if err != nil {
		return nil, errors.New("invalid or inaccessible persistent state")
	}
	return c, nil
}
func strictJSON(b []byte, dst any) error {
	d := json.NewDecoder(strings.NewReader(string(b)))
	d.DisallowUnknownFields()
	if err := d.Decode(dst); err != nil {
		return err
	}
	if err := d.Decode(new(any)); err != io.EOF {
		return errors.New("trailing JSON")
	}
	return nil
}
func readPrivate(path string, limit int64) ([]byte, error) {
	f, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	s, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if !s.Mode().IsRegular() || s.Mode().Perm() != 0600 {
		return nil, errors.New("file must be regular mode 0600")
	}
	b, err := io.ReadAll(io.LimitReader(f, limit+1))
	if int64(len(b)) > limit {
		return nil, errors.New("file too large")
	}
	return b, err
}
func privateDir(path string) error {
	if err := os.MkdirAll(path, 0700); err != nil {
		return err
	}
	s, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !s.IsDir() || s.Mode().Perm() != 0700 {
		return errors.New("directory must be mode 0700 and not a symlink")
	}
	return nil
}
func saveEnabled(path string, enabled bool) error {
	dir := filepath.Dir(path)
	if err := privateDir(dir); err != nil {
		return err
	}
	f, err := os.CreateTemp(dir, ".enabled-")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	if err := f.Chmod(0600); err != nil {
		f.Close()
		return err
	}
	b, _ := json.Marshal(struct {
		Enabled bool `json:"enabled"`
	}{enabled})
	b = append(b, '\n')
	if _, err = f.Write(b); err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	if err = os.Rename(f.Name(), path); err != nil {
		return err
	}
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
func (c *Controller) signal() {
	select {
	case c.changed <- struct{}{}:
	default:
	}
}
func (c *Controller) clearAudio() { c.audio = nil; c.partial = nil; c.queued = 0 }
func (c *Controller) invalidate(kind string, now time.Time) {
	old := c.epoch
	c.epoch++
	c.clearAudio()
	c.pending = nil
	// Reset is an unsent user intent, not an expendable audio/reply queue.
	// Keep at most one reset plus the newest cancellation until the writer flushes.
	var reset *Message
	for i := range c.events {
		if c.events[i].Type == "reset" {
			m := c.events[i]
			reset = &m
			break
		}
	}
	c.events = []Message{{Type: kind, Epoch: old}}
	if kind != "reset" && reset != nil {
		c.events = append([]Message{*reset}, c.events...)
	}
	if c.speechCancel != nil {
		c.speechCancel()
		c.speechCancel = nil
	}
	if c.speaking {
		c.lastSpoke = now
		c.echoUntil = now.Add(quietPeriod)
		if c.interrupt != nil {
			c.interrupt(old)
		}
	}
	c.speaking = false
	c.seenOwnBusy = false
	c.ownUntil = time.Time{}
	c.signal()
}
func (c *Controller) blocked(now time.Time) bool {
	return !c.enabled || !c.ready || !c.nativeReady || !c.mic || now.Sub(c.lastPCM) >= micTimeout || now.Before(c.directUntil) || now.Before(c.echoUntil) || c.nativeBusy || c.mediaPlaying || c.speaking
}
func (c *Controller) Status(now time.Time) Status {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.status(now)
}
func (c *Controller) status(now time.Time) Status {
	return Status{Enabled: c.enabled, Blocked: c.blocked(now), Ready: c.ready && c.nativeReady, Speaking: c.speaking, Epoch: c.epoch}
}
func (c *Controller) Connect(now time.Time) uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.invalidate("cancel", now)
	c.connection++
	c.connected = true
	c.ready = false
	c.lastLoud = now
	return c.connection
}
func (c *Controller) Ready(id uint64) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if id != c.connection || !c.connected {
		return false
	}
	c.ready = true
	c.signal()
	return true
}
func (c *Controller) Disconnect(id uint64, now time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if id != c.connection || !c.connected {
		return
	}
	c.ready = false
	c.connected = false
	c.invalidate("cancel", now)
}
func (c *Controller) NativeAvailable(available bool, now time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.nativeReady == available {
		return
	}
	c.nativeReady = available
	if !available {
		c.nativeIdle = false
		c.nativeBusy = false
		c.mediaPlaying = false
		c.invalidate("cancel", now)
	}
	c.signal()
}

func (c *Controller) SetMediaPlaying(playing bool, now time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.mediaPlaying != playing {
		c.mediaPlaying = playing
		c.invalidate("cancel", now)
		c.lastLoud = now
	}
}

// ObserveNative accepts technical events only. NONE is not proof that speech
// finished. Only an explicit busy -> IDLE transition ends owned speech early.
func (c *Controller) ObserveNative(state string, now time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	switch state {
	case "SPEAKING", "LISTENING", "THINKING", "BUSY", "PLAYING":
		c.nativeIdle = false
		if c.speaking {
			c.seenOwnBusy = true
			return
		}
		if !c.nativeBusy {
			c.nativeBusy = true
			c.invalidate("cancel", now)
		}
	case "IDLE":
		wasBusy := c.nativeBusy
		c.nativeBusy = false
		c.nativeIdle = true
		if c.speaking && c.seenOwnBusy {
			if c.release != nil {
				c.release(c.epoch)
			}
			c.lastSpoke = now
			c.echoUntil = now.Add(quietPeriod)
			c.speaking = false
			c.invalidate("cancel", now)
			c.lastLoud = now
		}
		if wasBusy && !c.directUntil.IsZero() {
			c.directUntil = time.Time{}
			c.invalidate("cancel", now)
			c.lastLoud = now
		}
	default:
		c.nativeIdle = false
	}
	c.signal()
}
func (c *Controller) Apply(action string, now time.Time) (Result, error) {
	if action == "on" || action == "off" {
		c.actionMu.Lock()
		defer c.actionMu.Unlock()
		enabled := action == "on"
		if enabled {
			if err := saveEnabled(c.cfg.StatePath, true); err != nil {
				return Result{Enabled: c.Status(now).Enabled}, err
			}
		}
		c.mu.Lock()
		c.enabled = enabled
		c.mic = false
		c.lastLoud = now
		c.invalidate("reset", now)
		c.mu.Unlock()
		var err error
		if c.capture != nil {
			err = c.capture(enabled)
		}
		if !enabled {
			if persistErr := saveEnabled(c.cfg.StatePath, false); persistErr != nil {
				err = persistErr
			}
		}
		answer := "Режим собеседника выключен."
		if enabled {
			answer = "Режим собеседника включён."
		}
		return Result{Answer: answer, Enabled: enabled}, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	answer := ""
	switch action {
	case "stop":
		c.invalidate("cancel", now)
		c.lastLoud = now
		answer = "Молчу."
	case "reset":
		c.invalidate("reset", now)
		c.lastLoud = now
		answer = "Новая тема."
	case "direct":
		c.invalidate("cancel", now)
		c.directUntil = now.Add(directPeriod)
		answer = "Слушаю."
	default:
		return Result{}, errors.New("unknown action")
	}
	c.signal()
	return Result{Answer: answer, Enabled: c.enabled}, nil
}
func (c *Controller) AcceptPCM(p []byte, now time.Time) bool {
	if len(p) == 0 || len(p) > maxPCM || len(p)%2 != 0 {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.enabled {
		return false
	}
	// Liveness is measured even while all inference/export is suppressed.
	if !c.mic {
		c.invalidate("cancel", now)
		c.mic = true
		c.lastLoud = now
	}
	c.lastPCM = now
	if c.blocked(now) {
		return false
	}
	sum := 0.0
	for i := 0; i < len(p); i += 2 {
		s := float64(int16(binary.LittleEndian.Uint16(p[i:]))) / 32768
		sum += s * s
	}
	if math.Sqrt(sum/float64(len(p)/2)) >= c.cfg.QuietRMS {
		if !c.talking {
			c.invalidate("cancel", now)
			c.talking = true
		}
		c.lastLoud = now
	} else if now.Sub(c.lastLoud) >= 250*time.Millisecond {
		c.talking = false
	}
	if c.queued+len(c.partial)+len(p) > maxQueue {
		c.invalidate("cancel", now)
		c.lastLoud = now
		return false
	}
	c.partial = append(c.partial, p...)
	for len(c.partial) >= 3200 {
		b := append([]byte(nil), c.partial[:3200]...)
		c.audio = append(c.audio, b)
		c.queued += len(b)
		c.partial = c.partial[3200:]
	}
	return true
}
func (c *Controller) Tick(now time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.mic && now.Sub(c.lastPCM) >= micTimeout {
		c.mic = false
		c.talking = false
		c.invalidate("cancel", now)
	}
	if c.speaking && !now.Before(c.ownUntil) {
		c.invalidate("cancel", now)
		c.lastLoud = now
	}
	if c.pending != nil && !now.Before(c.pending.expires) {
		c.pending = nil
	}
}
func validReply(text string) bool {
	if !utf8.ValidString(text) || strings.TrimSpace(text) != text || text == "" || utf8.RuneCountInString(text) > 600 || strings.ContainsAny(text, "<>[]{}\\") {
		return false
	}
	low := strings.ToLower(text)
	if strings.HasPrefix(low, "думаю") {
		return false
	}
	// ponytail: punctuation is a conservative sentence limit; abbreviations and
	// decimals may be rejected. Add a parser only if real short replies need it.
	sentences := 0
	lastEnd := false
	for _, r := range text {
		if unicode.IsControl(r) {
			return false
		}
		end := strings.ContainsRune(".!?…", r)
		if end && !lastEnd {
			sentences++
		}
		lastEnd = end
	}
	return sentences <= 2
}
func (c *Controller) Receive(id uint64, m Message, now time.Time) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if id != c.connection || !c.connected || m.Epoch != c.epoch {
		return false
	}
	if m.Type == "cancel" {
		c.invalidate("cancel", now)
		return true
	}
	if m.Type != "reply" || c.blocked(now) || !validReply(m.Text) {
		return false
	}
	c.pending = &pendingReply{Speech: Speech{Epoch: c.epoch, Text: m.Text}, expires: now.Add(replyTTL)}
	return true
}
func (c *Controller) TakeSpeech(now time.Time) (Speech, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.pending == nil || c.blocked(now) || c.pending.Epoch != c.epoch || !now.Before(c.pending.expires) || now.Sub(c.lastLoud) < quietPeriod || (!c.lastSpoke.IsZero() && now.Sub(c.lastSpoke) < cooldown) {
		return Speech{}, false
	}
	speech := c.pending.Speech
	c.pending = nil
	c.clearAudio()
	c.speaking = true
	// Reserve cooldown before delivery: a lost/cancelled ACK does not prove
	// native TTS was never audible. Successful ACK may extend it further.
	c.lastSpoke = now
	c.seenOwnBusy = false
	c.ownUntil = now.Add(ownSpeechLimit)
	c.signal()
	return speech, true
}
func (c *Controller) SpeechSent(epoch uint64, success bool, now time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if epoch != c.epoch || !c.speaking {
		return
	}
	if success {
		c.lastSpoke = now
		c.events = append(c.events, Message{Type: "spoken", Epoch: epoch})
		c.signal()
	} else {
		c.invalidate("cancel", now)
	}
}
func (c *Controller) beginSpeech(ctx context.Context, speech Speech, begin func(context.Context, uint64, string) (<-chan bool, error)) (<-chan bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if speech.Epoch != c.epoch || !c.speaking || !c.enabled || !c.ready || !c.nativeReady || c.nativeBusy || !c.mic || time.Since(c.lastPCM) >= micTimeout || time.Now().Before(c.directUntil) {
		return nil, errors.New("cancelled speech")
	}
	speechCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	c.speechCancel = cancel
	ack, err := begin(speechCtx, speech.Epoch, speech.Text)
	if err != nil {
		cancel()
		return nil, err
	}
	out := make(chan bool, 1)
	go func() {
		defer cancel()
		select {
		case ok := <-ack:
			out <- ok
		case <-speechCtx.Done():
			out <- false
		}
	}()
	return out, nil
}

func (c *Controller) DrainAudio(id uint64) [][]byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	if id != c.connection || !c.connected {
		return nil
	}
	out := c.audio
	c.audio = nil
	c.queued = 0
	return out
}
