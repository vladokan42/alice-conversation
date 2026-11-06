package device

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"

	"encoding/json"
	"encoding/pem"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
)

func TestRunLoopbackControlMuteAndSpeech(t *testing.T) {
	now := time.Now()
	firstLeaf := syntheticLeaf(t, now.Add(-time.Hour), now.Add(time.Hour), x509.ExtKeyUsageServerAuth)
	nextLeaf := syntheticLeaf(t, now.Add(-time.Hour), now.Add(time.Hour), x509.ExtKeyUsageServerAuth)
	if bytes.Equal(firstLeaf.Certificate[0], nextLeaf.Certificate[0]) {
		t.Fatal("rotation requires two distinct native leaves")
	}
	var nativeTLS atomic.Pointer[tls.Config]
	nativeTLS.Store(&tls.Config{Certificates: []tls.Certificate{firstLeaf}})
	spoken := make(chan struct{}, 1)
	nativeConnections := make(chan *websocket.Conn, 8)
	nativeTokens := make(chan string, 8)
	nativeSrv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.CloseNow()
		nativeConnections <- conn
		for {
			_, b, err := conn.Read(r.Context())
			if err != nil {
				return
			}
			var env struct {
				ID      string `json:"id"`
				Token   string `json:"conversationToken"`
				Payload struct {
					Command string `json:"command"`
				} `json:"payload"`
			}
			json.Unmarshal(b, &env)
			if env.Payload.Command == "softwareVersion" {
				nativeTokens <- env.Token
			}
			if env.Payload.Command == "serverAction" {
				select {
				case spoken <- struct{}{}:
				default:
				}
			}
			b, _ = json.Marshal(map[string]any{"requestId": env.ID, "status": "SUCCESS", "state": map[string]any{"aliceState": "IDLE"}})
			conn.Write(r.Context(), websocket.MessageText, b)
		}
	}))
	listener, err := net.Listen("tcp", "127.84.32.9:1961")
	if err != nil {
		t.Fatal(err)
	}
	nativeSrv.Listener.Close()
	nativeSrv.Listener = listener
	nativeSrv.TLS = &tls.Config{GetConfigForClient: func(*tls.ClientHelloInfo) (*tls.Config, error) { return nativeTLS.Load(), nil }}
	nativeSrv.StartTLS()
	defer nativeSrv.Close()
	hostSrv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer synthetic-host-token" {
			w.WriteHeader(401)
			return
		}
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.CloseNow()
		conn.Write(r.Context(), websocket.MessageText, []byte(`{"type":"ready"}`))
		epoch := uint64(0)
		samples := 0
		replied := false
		for {
			kind, b, err := conn.Read(r.Context())
			if err != nil {
				return
			}
			if kind == websocket.MessageText {
				var m Message
				json.Unmarshal(b, &m)
				if m.Type == "state" {
					epoch = m.Epoch
				}
			} else {
				samples++
				if samples >= 17 && !replied {
					reply, _ := json.Marshal(Message{Type: "reply", Epoch: epoch, Text: "Синтетическая проверка."})
					conn.Write(r.Context(), websocket.MessageText, reply)
					replied = true
				}
			}
		}
	}))
	defer hostSrv.Close()
	dir := t.TempDir()
	sockdir, err := os.MkdirTemp(".", ".run-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(sockdir)
	os.Chmod(sockdir, 0700)
	sockdir, _ = filepath.Abs(sockdir)
	put := func(name string, b []byte) string {
		t.Helper()
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, b, 0600); err != nil {
			t.Fatal(err)
		}
		return path
	}
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	controlAddress := l.Addr().String()
	l.Close()
	cfg := Config{StatePath: filepath.Join(dir, "enabled.json"), CaptureSocket: filepath.Join(sockdir, "mic.sock"), ControlListen: controlAddress, ControlTokenFile: put("control", []byte("synthetic-control-token")), ServerURL: "wss" + strings.TrimPrefix(hostSrv.URL, "https") + "/v1/listen", ServerCAFile: put("ca.pem", pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: hostSrv.Certificate().Raw})), TokenFile: put("host", []byte("synthetic-host-token")), GlagolHost: "127.84.32.9:1961", GlagolTokenFile: put("glagol", []byte("synthetic-native-token")), GlagolCertFile: filepath.Join(dir, "pin.pem")}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- Run(ctx, cfg) }()
	defer func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(2 * time.Second):
			t.Error("shutdown stuck")
		}
	}()
	client := &http.Client{Timeout: time.Second}
	health := func() (Status, error) {
		res, err := client.Get("http://" + controlAddress + "/healthz")
		if err != nil {
			return Status{}, err
		}
		defer res.Body.Close()
		var s Status
		err = json.NewDecoder(res.Body).Decode(&s)
		return s, err
	}
	var s Status
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		s, err = health()
		if err == nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err != nil || s.Ready || !s.Enabled || !s.Blocked {
		t.Fatalf("missing pin must leave Run alive but not ready: %+v %v", s, err)
	}
	put("pin.next", pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: firstLeaf.Certificate[0]}))
	if err := os.Rename(filepath.Join(dir, "pin.next"), cfg.GlagolCertFile); err != nil {
		t.Fatal(err)
	}
	deadline = time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		s, err = health()
		if err == nil && s.Ready {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err != nil || !s.Ready || !s.Enabled || !s.Blocked {
		t.Fatalf("boot status %+v %v", s, err)
	}
	if token := <-nativeTokens; token != "synthetic-native-token" {
		t.Fatal("unexpected first native credential")
	}
	// The same endpoint rotates its leaf; an approved updater atomically replaces
	// the private pin and token. The existing Run must reread both on reconnect.
	put("pin.next", pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: nextLeaf.Certificate[0]}))
	if err := os.Rename(filepath.Join(dir, "pin.next"), cfg.GlagolCertFile); err != nil {
		t.Fatal(err)
	}
	nativeTLS.Store(&tls.Config{Certificates: []tls.Certificate{nextLeaf}})
	put("glagol.next", []byte("synthetic-refreshed-native-token"))
	if err := os.Rename(filepath.Join(dir, "glagol.next"), cfg.GlagolTokenFile); err != nil {
		t.Fatal(err)
	}
	(<-nativeConnections).CloseNow()
	select {
	case token := <-nativeTokens:
		if token != "synthetic-refreshed-native-token" {
			t.Fatal("reconnect reused expired native credential")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("native did not reconnect after approved leaf and credential rotation")
	}
	conn, err := net.DialUnix("unixgram", nil, &net.UnixAddr{Name: cfg.CaptureSocket, Net: "unixgram"})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	for i := 0; i < 5; i++ {
		conn.Write(make([]byte, 3200))
		time.Sleep(50 * time.Millisecond)
	}
	s, err = health()
	if err != nil || s.Blocked {
		t.Fatalf("PCM did not unblock %+v %v", s, err)
	}
	for i := 0; i < 20; i++ {
		conn.Write(make([]byte, 3200))
		time.Sleep(100 * time.Millisecond)
	}
	select {
	case <-spoken:
	case <-time.After(time.Second):
		t.Fatal("PCM -> host reply -> native speech path did not run")
	}
	time.Sleep(450 * time.Millisecond)
	s, _ = health()
	if !s.Blocked || !s.Enabled {
		t.Fatal("hardware loss did not pause")
	}
	post := func(action string) {
		t.Helper()
		req, _ := http.NewRequest("POST", "http://"+controlAddress+"/control", bytes.NewBufferString(`{"action":"`+action+`"}`))
		req.Header.Set("Authorization", "Bearer synthetic-control-token")
		res, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		io.Copy(io.Discard, res.Body)
		res.Body.Close()
		if res.StatusCode != 200 {
			t.Fatal(res.StatusCode)
		}
	}
	post("off")
	if _, err := os.Lstat(cfg.CaptureSocket); !os.IsNotExist(err) {
		t.Fatal("HTTP off returned before capture unlinked")
	}
	post("on")
	if _, err := os.Lstat(cfg.CaptureSocket); err != nil {
		t.Fatal("on did not reopen capture")
	}
}
