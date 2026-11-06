package device

import (
	"context"
	"encoding/binary"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

func privateTempDir(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	return dir
}

func controller(t *testing.T) (*Controller, time.Time, uint64) {
	t.Helper()
	c, err := NewController(Config{StatePath: filepath.Join(privateTempDir(t), "enabled.json"), QuietRMS: .02})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Unix(1000, 0)
	id := c.Connect(now)
	c.Ready(id)
	c.NativeAvailable(true, now)
	c.ObserveNative("IDLE", now)
	return c, now, id
}
func silence(c *Controller, now time.Time, duration time.Duration) time.Time {
	for end := now.Add(duration); !now.After(end); now = now.Add(100 * time.Millisecond) {
		c.AcceptPCM(make([]byte, 3200), now)
		c.Tick(now)
	}
	return now.Add(-100 * time.Millisecond)
}
func loud() []byte {
	b := make([]byte, 3200)
	for i := 0; i < len(b); i += 2 {
		binary.LittleEndian.PutUint16(b[i:], 10000)
	}
	return b
}
func offer(t *testing.T, c *Controller, id uint64, now time.Time, text string) uint64 {
	t.Helper()
	epoch := c.Status(now).Epoch
	if !c.Receive(id, Message{Type: "reply", Epoch: epoch, Text: text}, now) {
		t.Fatal("fresh reply refused")
	}
	return epoch
}
func TestBootMuteRestoreAndExplicitOff(t *testing.T) {
	c, now, id := controller(t)
	if s := c.Status(now); !s.Enabled || !s.Blocked {
		t.Fatalf("boot must wait for PCM: %+v", s)
	}
	now = silence(c, now, 1500*time.Millisecond)
	old := offer(t, c, id, now, "Тихая проверка.")
	c.Tick(now.Add(400 * time.Millisecond))
	if s := c.Status(now.Add(400 * time.Millisecond)); !s.Enabled || !s.Blocked || s.Epoch == old {
		t.Fatalf("mute must invalidate, not disable: %+v", s)
	}
	now = silence(c, now.Add(time.Second), 1500*time.Millisecond)
	if c.Status(now).Blocked {
		t.Fatal("unmute did not resume")
	}
	if _, ok := c.TakeSpeech(now); ok {
		t.Fatal("muted reply survived")
	}
	result, err := c.Apply("off", now)
	if err != nil || result.Enabled {
		t.Fatalf("off: %+v %v", result, err)
	}
	b, err := os.ReadFile(c.cfg.StatePath)
	if err != nil || string(b) != "{\"enabled\":false}\n" {
		t.Fatalf("state %q %v", b, err)
	}
	stat, _ := os.Stat(c.cfg.StatePath)
	if stat.Mode().Perm() != 0600 {
		t.Fatal("state not private")
	}
	restored, err := NewController(c.cfg)
	if err != nil || restored.Status(now).Enabled {
		t.Fatalf("restore disabled: %v", err)
	}
	restored.AcceptPCM(loud(), now)
	if restored.Status(now).Enabled {
		t.Fatal("PCM changed saved choice")
	}
	if _, err = restored.Apply("on", now); err != nil {
		t.Fatal(err)
	}
	restored, err = NewController(c.cfg)
	if err != nil || !restored.Status(now).Enabled || !restored.Status(now).Blocked {
		t.Fatalf("restore enabled boot: %v", err)
	}
}
func TestStatePermissionsIgnoreRestrictiveUmask(t *testing.T) {
	path := filepath.Join(privateTempDir(t), "state.json")
	old := syscall.Umask(0777)
	defer syscall.Umask(old)
	if _, err := NewController(Config{StatePath: path}); err != nil {
		t.Fatal(err)
	}
	if _, err := NewController(Config{StatePath: path}); err != nil {
		t.Fatal("state not readable at required mode 0600:", err)
	}
}
func TestStateRejectsCorruptOrUnsafeFile(t *testing.T) {
	for _, body := range []string{"{}", "{\"enabled\":null}", "{\"enabled\":true,\"text\":\"no\"}", "{\"enabled\":true} {}", "false"} {
		t.Run(body, func(t *testing.T) {
			p := filepath.Join(privateTempDir(t), "state")
			os.WriteFile(p, []byte(body), 0600)
			if _, err := NewController(Config{StatePath: p}); err == nil {
				t.Fatal("accepted invalid state")
			}
		})
	}
	p := filepath.Join(privateTempDir(t), "state")
	os.WriteFile(p, []byte("{\"enabled\":true}"), 0644)
	os.Chmod(p, 0644) // Explicitly override a restrictive host umask.
	if _, err := NewController(Config{StatePath: p}); err == nil {
		t.Fatal("accepted public state")
	}
}
func TestLateNativeSpeechEndExtendsCooldown(t *testing.T) {
	c, now, id := controller(t)
	now = silence(c, now, 1500*time.Millisecond)
	offer(t, c, id, now, "Отложенная озвучка.")
	speech, _ := c.TakeSpeech(now)
	c.SpeechSent(speech.Epoch, true, now)
	now = silence(c, now, 20*time.Second)
	c.ObserveNative("SPEAKING", now)
	c.ObserveNative("IDLE", now)
	now = silence(c, now, 2*time.Second)
	offer(t, c, id, now, "Не сразу после неё.")
	if _, ok := c.TakeSpeech(now); ok {
		t.Fatal("late native speech end did not extend cooldown")
	}
}
func TestCancelledUnacknowledgedSpeechKeepsCooldown(t *testing.T) {
	c, now, id := controller(t)
	now = silence(c, now, 1500*time.Millisecond)
	offer(t, c, id, now, "Возможно уже началась.")
	c.TakeSpeech(now)
	// A native request may have reached TTS even if its ACK is cancelled/lost.
	c.Apply("stop", now)
	now = silence(c, now, 2*time.Second)
	offer(t, c, id, now, "Не повторять слишком рано.")
	if _, ok := c.TakeSpeech(now); ok {
		t.Fatal("cancelled/lost ACK bypassed local 15s cooldown")
	}
}
func TestQuietCooldownDirectAndNewSpeech(t *testing.T) {
	c, now, id := controller(t)
	now = silence(c, now, 1500*time.Millisecond)
	offer(t, c, id, now, "Первая проверка.")
	speech, ok := c.TakeSpeech(now)
	if !ok {
		t.Fatal("quiet fresh reply not scheduled")
	}
	c.SpeechSent(speech.Epoch, true, now)
	if c.AcceptPCM(loud(), now.Add(100*time.Millisecond)) {
		t.Fatal("own speech exported")
	}
	c.ObserveNative("NONE", now.Add(time.Second))
	if !c.Status(now.Add(time.Second)).Speaking {
		t.Fatal("NONE incorrectly ended native TTS")
	}
	c.ObserveNative("SPEAKING", now.Add(time.Second))
	c.ObserveNative("IDLE", now.Add(2*time.Second))
	now = silence(c, now.Add(2*time.Second), 2*time.Second)
	offer(t, c, id, now, "Слишком рано.")
	if _, ok := c.TakeSpeech(now); ok {
		t.Fatal("15s cooldown bypassed")
	}
	now = silence(c, now, 12*time.Second)
	offer(t, c, id, now, "Вторая проверка.")
	old := c.Status(now).Epoch
	c.AcceptPCM(loud(), now)
	if c.Status(now).Epoch == old {
		t.Fatal("human speech failed to invalidate reply")
	}
	if _, ok := c.TakeSpeech(now); ok {
		t.Fatal("spoke over human speech")
	}
	now = silence(c, now, 2*time.Second)
	offer(t, c, id, now, "Отменить прямым запросом.")
	if _, err := c.Apply("direct", now); err != nil {
		t.Fatal(err)
	}
	now = silence(c, now, 44*time.Second)
	if !c.Status(now).Blocked {
		t.Fatal("direct block too short")
	}
	now = silence(c, now, 2*time.Second)
	if c.Status(now).Blocked {
		t.Fatal("direct block stuck")
	}
	if _, ok := c.TakeSpeech(now); ok {
		t.Fatal("direct left stale reply")
	}
}
func TestStopResetReconnectAndEpochs(t *testing.T) {
	c, now, id := controller(t)
	now = silence(c, now, 1500*time.Millisecond)
	for _, action := range []string{"stop", "reset"} {
		offer(t, c, id, now, "Отмена.")
		old := c.Status(now).Epoch
		if _, err := c.Apply(action, now); err != nil {
			t.Fatal(err)
		}
		if !c.Status(now).Enabled || c.Status(now).Epoch == old {
			t.Fatal("action disabled mode or kept epoch")
		}
		if c.Receive(id, Message{Type: "reply", Epoch: old, Text: "Поздно."}, now) {
			t.Fatal("accepted stale reply")
		}
		if _, ok := c.TakeSpeech(now); ok {
			t.Fatal("pending reply survived")
		}
	}
	c.AcceptPCM(make([]byte, 3200), now)
	offer(t, c, id, now, "До переподключения.")
	old := c.Status(now).Epoch
	c.Disconnect(id, now)
	id2 := c.Connect(now)
	if c.Ready(id) || c.Receive(id, Message{Type: "reply", Epoch: old, Text: "Старая связь."}, now) {
		t.Fatal("old connection accepted")
	}
	if c.Status(now).Ready {
		t.Fatal("ready before host ready")
	}
	c.Ready(id2)
	if len(c.DrainAudio(id2)) != 0 {
		t.Fatal("reconnect retained audio")
	}
	if _, ok := c.TakeSpeech(now); ok {
		t.Fatal("reconnect retained reply")
	}
}
func TestBoundedAudioAndInvalidInput(t *testing.T) {
	c, now, id := controller(t)
	now = silence(c, now, 1500*time.Millisecond)
	c.DrainAudio(id)
	if c.AcceptPCM([]byte{1}, now) || c.AcceptPCM(make([]byte, 6402), now) {
		t.Fatal("invalid PCM accepted")
	}
	for i := 0; i < 50; i++ {
		c.AcceptPCM(make([]byte, 3200), now)
	}
	total := 0
	for _, b := range c.DrainAudio(id) {
		if len(b) > 6400 {
			t.Fatal("oversized batch")
		}
		total += len(b)
	}
	if total > 64000 {
		t.Fatalf("unbounded audio: %d", total)
	}
	c.Apply("off", now)
	if c.AcceptPCM(make([]byte, 3200), now) || len(c.DrainAudio(id)) != 0 {
		t.Fatal("off exported PCM")
	}
}
func TestReplyValidationAndExpiry(t *testing.T) {
	c, now, id := controller(t)
	now = silence(c, now, 1500*time.Millisecond)
	for _, text := range []string{"", "Думаю, нет.", "<speak>Нет.</speak>", "Один. Два. Три.", strings.Repeat("я", 601), "строка\nдругая"} {
		if c.Receive(id, Message{Type: "reply", Epoch: c.Status(now).Epoch, Text: text}, now) {
			t.Fatalf("unsafe reply accepted %q", text)
		}
	}
	offer(t, c, id, now, "Устаревает.")
	now = silence(c, now, 9*time.Second)
	if _, ok := c.TakeSpeech(now); ok {
		t.Fatal("expired reply scheduled")
	}
}

func TestNativeEndReleasesOwnershipBeforeCancel(t *testing.T) {
	c, now, id := controller(t)
	now = silence(c, now, 1500*time.Millisecond)
	offer(t, c, id, now, "Завершить.")
	speech, _ := c.TakeSpeech(now)
	released := uint64(0)
	c.release = func(epoch uint64) { released = epoch }
	c.ObserveNative("SPEAKING", now)
	c.ObserveNative("IDLE", now.Add(time.Second))
	if released != speech.Epoch {
		t.Fatal("completed speech left native ownership for a late stop")
	}
}
func TestDirectDoesNotWaitForModeIO(t *testing.T) {
	c, now, _ := controller(t)
	entered := make(chan struct{})
	release := make(chan struct{})
	offDone := make(chan struct{})
	c.capture = func(bool) error { close(entered); <-release; return nil }
	go func() { c.Apply("off", now); close(offDone) }()
	<-entered
	directDone := make(chan struct{})
	go func() { c.Apply("direct", now); close(directDone) }()
	select {
	case <-directDone:
	case <-time.After(150 * time.Millisecond):
		close(release)
		<-offDone
		<-directDone
		t.Fatal("direct waited for slow mode I/O")
	}
	close(release)
	<-offDone
}
func TestResetIntentSurvivesNewSpeech(t *testing.T) {
	c, now, _ := controller(t)
	now = silence(c, now, 1500*time.Millisecond)
	c.Apply("reset", now)
	c.AcceptPCM(loud(), now)
	found := false
	for _, m := range c.events {
		if m.Type == "reset" {
			found = true
		}
	}
	if !found {
		t.Fatal("new speech lost unsent context reset")
	}
}
func TestCancelledSpeechDropsEchoTail(t *testing.T) {
	c, now, id := controller(t)
	now = silence(c, now, 1500*time.Millisecond)
	offer(t, c, id, now, "Остановить.")
	c.TakeSpeech(now)
	c.Apply("stop", now)
	if c.AcceptPCM(loud(), now.Add(100*time.Millisecond)) {
		t.Fatal("exported cancelled TTS echo")
	}
	now = silence(c, now.Add(1500*time.Millisecond), 1500*time.Millisecond)
	if c.Status(now).Blocked {
		t.Fatal("echo guard did not expire")
	}
}
func TestOwnSpeechTimeoutSendsStop(t *testing.T) {
	c, now, id := controller(t)
	now = silence(c, now, 1500*time.Millisecond)
	offer(t, c, id, now, "Ограничить.")
	speech, _ := c.TakeSpeech(now)
	stopped := uint64(0)
	c.interrupt = func(epoch uint64) { stopped = epoch }
	silence(c, now, 31*time.Second)
	if stopped != speech.Epoch {
		t.Fatal("unobserved native speech timeout did not stop it")
	}
}
func TestDirectEndsOnlyOnObservedNativeEnd(t *testing.T) {
	c, now, _ := controller(t)
	now = silence(c, now, 1500*time.Millisecond)
	c.Apply("direct", now)
	c.ObserveNative("NONE", now.Add(time.Second))
	now = silence(c, now, 2*time.Second)
	if !c.Status(now).Blocked {
		t.Fatal("NONE released direct suppression")
	}
	c.ObserveNative("SPEAKING", now)
	c.ObserveNative("IDLE", now.Add(time.Second))
	now = silence(c, now.Add(time.Second), 2*time.Second)
	if c.Status(now).Blocked {
		t.Fatal("observed native speech end did not release direct suppression")
	}
}

func TestCancelBetweenSchedulingAndNativeWrite(t *testing.T) {
	c, now, id := controller(t)
	now = silence(c, now, 1500*time.Millisecond)
	offer(t, c, id, now, "Не отправлять.")
	speech, ok := c.TakeSpeech(now)
	if !ok {
		t.Fatal("no scheduled speech")
	}
	c.Apply("direct", now)
	nativeWrites := 0
	_, err := c.beginSpeech(context.Background(), speech, func(context.Context, uint64, string) (<-chan bool, error) {
		nativeWrites++
		return make(chan bool), nil
	})
	if err == nil || nativeWrites != 0 {
		t.Fatal("cancelled epoch reached native socket")
	}
}
func TestMuteStopsOwnedSpeechAndSynchronousCaptureHook(t *testing.T) {
	c, now, id := controller(t)
	now = silence(c, now, 1500*time.Millisecond)
	offer(t, c, id, now, "Прервать.")
	speech, _ := c.TakeSpeech(now)
	stopped := uint64(0)
	c.interrupt = func(epoch uint64) { stopped = epoch }
	c.Tick(now.Add(400 * time.Millisecond))
	if stopped != speech.Epoch {
		t.Fatal("mute did not stop owned speech")
	}
	called := false
	c.capture = func(enabled bool) error {
		called = true
		if enabled {
			t.Fatal("off enabled capture")
		}
		return nil
	}
	_, err := c.Apply("off", now)
	if err != nil || !called {
		t.Fatalf("missing synchronous capture hook: %v", err)
	}
}
