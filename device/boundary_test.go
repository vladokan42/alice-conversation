package device

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"io"
	"log"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func syntheticLeaf(t *testing.T, notBefore, notAfter time.Time, usages ...x509.ExtKeyUsage) tls.Certificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{SerialNumber: big.NewInt(1), NotBefore: notBefore, NotAfter: notAfter, KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: usages}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	leaf, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key, Leaf: leaf}
}

func TestControlAuthStrictJSONAndFastDirect(t *testing.T) {
	c, now, _ := controller(t)
	_ = now
	server := httptest.NewServer(ControlHandler(c, "synthetic-control-key"))
	defer server.Close()
	tests := []struct {
		body, auth string
		want       int
	}{
		{`{"action":"off"}`, "", 401},
		{`{"action":"off"}`, "Bearer wrong", 401},
		{`{"action":"off","text":"must not accept"}`, "Bearer synthetic-control-key", 400},
		{`{"action":"off"} {}`, "Bearer synthetic-control-key", 400},
		{`{"action":"invalid"}`, "Bearer synthetic-control-key", 400},
		{strings.Repeat("x", 2000), "Bearer synthetic-control-key", 400},
	}
	for _, tt := range tests {
		req, _ := http.NewRequest("POST", server.URL+"/control", strings.NewReader(tt.body))
		req.Header.Set("Authorization", tt.auth)
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		io.Copy(io.Discard, res.Body)
		res.Body.Close()
		if res.StatusCode != tt.want {
			t.Fatalf("want %d got %d", tt.want, res.StatusCode)
		}
	}
	before := time.Now()
	req, _ := http.NewRequest("POST", server.URL+"/control", strings.NewReader(`{"action":"direct"}`))
	req.Header.Set("Authorization", "Bearer synthetic-control-key")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	if elapsed := time.Since(before); elapsed >= 200*time.Millisecond {
		t.Fatalf("direct callback delayed %s", elapsed)
	}
	var reply Result
	if err := json.NewDecoder(res.Body).Decode(&reply); err != nil || res.StatusCode != 200 || !reply.Enabled || reply.Answer == "" {
		t.Fatalf("control response %+v %v", reply, err)
	}
	res, err = http.Get(server.URL + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(res.Body)
	res.Body.Close()
	if strings.Contains(string(b), "synthetic") || strings.Contains(string(b), "answer") || !strings.Contains(string(b), `"blocked":true`) {
		t.Fatalf("unsafe health: %s", b)
	}
}
func TestCaptureOffUnlinksAndForeignSocketSurvives(t *testing.T) {
	// A short path avoids Linux's 108-byte sun_path ceiling under long TMPDIRs.
	dir, err := os.MkdirTemp(".", ".sock-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	os.Chmod(dir, 0700)
	dir, _ = filepath.Abs(dir)
	path := filepath.Join(dir, "mic.sock")
	got := make(chan int, 1)
	capture := NewCapture(path, func(b []byte) {
		select {
		case got <- len(b):
		default:
		}
	})
	if err := capture.SetEnabled(true); err != nil {
		t.Fatal(err)
	}
	defer capture.SetEnabled(false)
	conn, err := net.DialUnix("unixgram", nil, &net.UnixAddr{Name: path, Net: "unixgram"})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	conn.Write(make([]byte, 3200))
	select {
	case n := <-got:
		if n != 3200 {
			t.Fatal(n)
		}
	case <-time.After(time.Second):
		t.Fatal("no PCM")
	}
	if err := capture.SetEnabled(false); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(path); !os.IsNotExist(err) {
		t.Fatal("off left capture listener")
	}
	os.WriteFile(path, []byte("foreign"), 0600)
	if err := capture.SetEnabled(true); err == nil {
		t.Fatal("overwrote foreign path")
	}
	b, _ := os.ReadFile(path)
	if string(b) != "foreign" {
		t.Fatal("foreign path changed")
	}
}
func TestIndicatorNeverOverridesNativeOrUnownedLED(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "led")
	other := filepath.Join(dir, "other")
	os.WriteFile(path, []byte("0"), 0600)
	os.WriteFile(other, []byte("0"), 0600)
	led := Indicator{Path: path, Others: []string{other}}
	check := func(want string) {
		t.Helper()
		b, _ := os.ReadFile(path)
		if strings.TrimSpace(string(b)) != want {
			t.Fatalf("LED %q want %q", b, want)
		}
	}
	if err := led.Update(true, true); err != nil {
		t.Fatal(err)
	}
	check("6")
	led.Update(false, true)
	check("0")
	os.WriteFile(path, []byte("3"), 0600)
	led.Update(true, true)
	check("3")
	os.WriteFile(path, []byte("0"), 0600)
	os.WriteFile(other, []byte("1"), 0600)
	led.Update(true, true)
	check("0")
	os.WriteFile(other, []byte("0"), 0600)
	led.Update(true, false)
	check("0")
	led.Update(true, true)
	check("6")
	os.WriteFile(path, []byte("4"), 0600)
	led.Update(false, true)
	check("4")
}
func TestConfigTrustBoundaries(t *testing.T) {
	if _, err := LoadConfig("config.example.json"); err != nil {
		t.Fatal("example config contract:", err)
	}
	good := Config{StatePath: "/private/state/enabled.json", CaptureSocket: "/run/private/mic.sock", ControlListen: "127.0.0.1:18862", ControlTokenFile: "/private/control.token", ServerURL: "wss://server.example:18863/v1/listen", ServerCAFile: "/private/server-ca.pem", TokenFile: "/private/host.token", GlagolHost: "192.0.2.10:1961", GlagolTokenFile: "/private/glagol.token", GlagolCertFile: "/private/glagol.pem"}
	if err := good.Validate(); err != nil {
		t.Fatal(err)
	}
	mutations := []func(*Config){
		func(c *Config) { c.ControlListen = "0.0.0.0:18862" }, func(c *Config) { c.ControlListen = "localhost:18862" },
		func(c *Config) { c.ServerURL = "ws://server.example/v1/listen" }, func(c *Config) { c.ServerURL = "wss://user:pass@server.example/v1/listen" },
		func(c *Config) { c.GlagolHost = "station.example:1961" }, func(c *Config) { c.GlagolHost = "192.0.2.10:443" },
		func(c *Config) { c.TokenFile = "relative" }, func(c *Config) { c.StatePath = c.TokenFile }, func(c *Config) { c.QuietRMS = -1 },
	}
	for i, mutate := range mutations {
		bad := good
		mutate(&bad)
		if err := bad.Validate(); err == nil {
			t.Fatalf("mutation %d accepted", i)
		}
	}
}
func TestPinnedTLSRejectsAnotherLeaf(t *testing.T) {
	s := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200) }))
	defer s.Close()
	cert := s.Certificate()
	pemBytes := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: cert.Raw})
	cfg, err := PinnedTLS(pemBytes)
	if err != nil {
		t.Fatal(err)
	}
	client := &http.Client{Transport: &http.Transport{TLSClientConfig: cfg}, Timeout: time.Second}
	res, err := client.Get(s.URL)
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	client.CloseIdleConnections()
	wrong := *cert
	wrong.Raw = append([]byte(nil), cert.Raw...)
	wrong.Raw[len(wrong.Raw)-1] ^= 1
	cfg, err = PinnedTLS(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: wrong.Raw}))
	if err != nil {
		t.Fatal(err)
	}
	client = &http.Client{Transport: &http.Transport{TLSClientConfig: cfg}, Timeout: time.Second}
	if res, err = client.Get(s.URL); err == nil {
		res.Body.Close()
		t.Fatal("accepted wrong leaf pin")
	}
	if _, err = PinnedTLS([]byte("garbage")); err == nil {
		t.Fatal("invalid certificate accepted")
	}
	// The custom verifier must still check validity, not only equality.
	expired := tls.ConnectionState{PeerCertificates: []*x509.Certificate{cert}}
	cfg, _ = PinnedTLS(pemBytes)
	copyCert := *cert
	copyCert.NotAfter = time.Unix(0, 0)
	expired.PeerCertificates = []*x509.Certificate{&copyCert}
	if err := cfg.VerifyConnection(expired); err == nil {
		t.Fatal("expired pinned leaf accepted")
	}

}

func TestPinnedFileTLSReloadsAndFailsClosed(t *testing.T) {
	now := time.Now()
	first := syntheticLeaf(t, now.Add(-time.Hour), now.Add(time.Hour), x509.ExtKeyUsageServerAuth)
	next := syntheticLeaf(t, now.Add(-time.Hour), now.Add(time.Hour), x509.ExtKeyUsageServerAuth)
	expired := syntheticLeaf(t, now.Add(-2*time.Hour), now.Add(-time.Hour), x509.ExtKeyUsageServerAuth)
	future := syntheticLeaf(t, now.Add(time.Hour), now.Add(2*time.Hour), x509.ExtKeyUsageServerAuth)
	clientOnly := syntheticLeaf(t, now.Add(-time.Hour), now.Add(time.Hour), x509.ExtKeyUsageClientAuth)
	encode := func(cert tls.Certificate) []byte {
		return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: cert.Certificate[0]})
	}
	firstPEM, nextPEM := encode(first), encode(next)
	var serverTLS atomic.Pointer[tls.Config]
	serverTLS.Store(&tls.Config{Certificates: []tls.Certificate{first}})
	s := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200) }))
	s.Config.ErrorLog = log.New(io.Discard, "", 0)
	s.TLS = &tls.Config{GetConfigForClient: func(*tls.ClientHelloInfo) (*tls.Config, error) { return serverTLS.Load(), nil }}
	s.StartTLS()
	defer s.Close()
	path := filepath.Join(t.TempDir(), "approved.pem")
	cfg := pinnedFileTLS(path)
	if cfg.MinVersion != tls.VersionTLS12 {
		t.Fatal("Glagol TLS floor changed")
	}
	check := func(t *testing.T, want bool) {
		t.Helper()
		conn, err := tls.DialWithDialer(&net.Dialer{Timeout: time.Second}, "tcp", s.Listener.Addr().String(), cfg)
		if conn != nil {
			conn.Close()
		}
		if (err == nil) != want {
			t.Fatalf("TLS handshake success=%t, want %t: %v", err == nil, want, err)
		}
	}
	install := func(t *testing.T, b []byte, mode os.FileMode) {
		t.Helper()
		staged := path + ".next"
		if err := os.WriteFile(staged, b, 0600); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(staged, mode); err != nil {
			t.Fatal(err)
		}
		if err := os.Rename(staged, path); err != nil {
			t.Fatal(err)
		}
	}
	// A missing pin must fail at connection time, not poison this TLS config.
	check(t, false)
	for _, tt := range []struct {
		name string
		leaf tls.Certificate
		pin  []byte
		mode os.FileMode
		want bool
	}{
		{"initial", first, firstPEM, 0600, true},
		{"unapproved leaf", next, firstPEM, 0600, false},
		{"approved rotation", next, nextPEM, 0600, true},
		{"malformed PEM", next, []byte("not a certificate"), 0600, false},
		{"malformed DER", next, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: []byte("not DER")}), 0600, false},
		{"multiple leaves", next, append(append([]byte(nil), nextPEM...), firstPEM...), 0600, false},
		{"unsafe permissions", next, nextPEM, 0644, false},
		{"not mode 0600", next, nextPEM, 0400, false},
		{"oversized pin", next, append(append([]byte(nil), nextPEM...), []byte(strings.Repeat(" ", 32769))...), 0600, false},
		{"expired leaf", expired, encode(expired), 0600, false},
		{"future leaf", future, encode(future), 0600, false},
		{"wrong EKU", clientOnly, encode(clientOnly), 0600, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			serverTLS.Store(&tls.Config{Certificates: []tls.Certificate{tt.leaf}})
			install(t, tt.pin, tt.mode)
			check(t, tt.want)
			// Every failed update can recover using this same client TLS config.
			if !tt.want {
				serverTLS.Store(&tls.Config{Certificates: []tls.Certificate{next}})
				install(t, nextPEM, 0600)
				check(t, true)
			}
		})
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	check(t, false)
	if err := os.Symlink(path+".target", path); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path+".target", nextPEM, 0600); err != nil {
		t.Fatal(err)
	}
	check(t, false)
	install(t, nextPEM, 0600)
	check(t, true)
	serverTLS.Store(&tls.Config{Certificates: []tls.Certificate{next}, MinVersion: tls.VersionTLS11, MaxVersion: tls.VersionTLS11})
	check(t, false)
}
