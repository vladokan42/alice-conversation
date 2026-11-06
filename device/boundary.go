package device

import (
	"crypto/sha256"
	"crypto/subtle"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"io"
	"math"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"
)

type Config struct {
	StatePath        string     `json:"state_path"`
	CaptureSocket    string     `json:"capture_socket"`
	ControlListen    string     `json:"control_listen"`
	ControlTokenFile string     `json:"control_token_file"`
	ServerURL        string     `json:"server_url"`
	ServerCAFile     string     `json:"server_ca_file"`
	TokenFile        string     `json:"token_file"`
	GlagolHost       string     `json:"glagol_host"`
	GlagolTokenFile  string     `json:"glagol_token_file"`
	GlagolCertFile   string     `json:"glagol_cert_file"`
	QuietRMS         float64    `json:"quiet_rms,omitempty"`
	Indicator        *Indicator `json:"indicator,omitempty"`
}

func LoadConfig(path string) (Config, error) {
	cfg := Config{ControlListen: "127.0.0.1:18862", QuietRMS: .02}
	b, err := os.ReadFile(path)
	if err != nil {
		return cfg, errors.New("cannot read configuration")
	}
	if len(b) > 16384 || strictJSON(b, &cfg) != nil {
		return cfg, errors.New("invalid configuration JSON")
	}
	return cfg, cfg.Validate()
}
func (c Config) Validate() error {
	host, port, err := net.SplitHostPort(c.ControlListen)
	if err != nil || (host != "127.0.0.1" && host != "::1") || !validPort(port) {
		return errors.New("control_listen must be a loopback IP and port")
	}
	host, port, err = net.SplitHostPort(c.GlagolHost)
	if err != nil || net.ParseIP(host) == nil || port != "1961" {
		return errors.New("glagol_host must be an exact IP:1961")
	}
	u, err := url.Parse(c.ServerURL)
	if err != nil || u.Scheme != "wss" || u.Hostname() == "" || u.Path != "/v1/listen" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return errors.New("server_url must be wss://host[:port]/v1/listen without credentials")
	}
	paths := []string{c.StatePath, c.CaptureSocket, c.ControlTokenFile, c.ServerCAFile, c.TokenFile, c.GlagolTokenFile, c.GlagolCertFile}
	if c.Indicator != nil {
		paths = append(paths, c.Indicator.Path)
		paths = append(paths, c.Indicator.Others...)
		if len(c.Indicator.Others) == 0 {
			return errors.New("indicator requires all other LED paths")
		}
	}
	seen := map[string]bool{}
	for _, path := range paths {
		if !filepath.IsAbs(path) || filepath.Clean(path) != path || seen[path] {
			return errors.New("paths must be distinct, absolute and clean")
		}
		seen[path] = true
	}
	if len(c.CaptureSocket) > 107 {
		return errors.New("capture_socket exceeds Unix socket path limit")
	}
	if math.IsNaN(c.QuietRMS) || math.IsInf(c.QuietRMS, 0) || (c.QuietRMS != 0 && (c.QuietRMS < .001 || c.QuietRMS > .5)) {
		return errors.New("quiet_rms outside .001..0.5")
	}
	return nil
}
func validPort(port string) bool {
	p, err := net.LookupPort("tcp", port)
	return err == nil && p > 0 && p <= 65535 && strings.IndexFunc(port, func(r rune) bool { return r < '0' || r > '9' }) < 0
}
func readToken(path string) (string, error) {
	b, err := readPrivate(path, 4096)
	if err != nil {
		return "", errors.New("cannot read private token file")
	}
	token := strings.TrimSuffix(string(b), "\n")
	if len(token) < 16 || strings.ContainsAny(token, "\r\n\t ") {
		return "", errors.New("invalid token file")
	}
	return token, nil
}

// PinnedTLS verifies the exact leaf plus its validity and server key usage.
// Glagol's device certificate may lack IP SANs: default name/CA verification is
// replaced, NOT omitted. Callers must dial only Config.GlagolHost's exact IP.
func PinnedTLS(certPEM []byte) (*tls.Config, error) {
	block, rest := pem.Decode(certPEM)
	if block == nil || block.Type != "CERTIFICATE" || len(strings.TrimSpace(string(rest))) != 0 {
		return nil, errors.New("one PEM leaf certificate required")
	}
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return nil, errors.New("invalid PEM leaf")
	}
	pin := sha256.Sum256(cert.Raw)
	cfg := &tls.Config{MinVersion: tls.VersionTLS12, InsecureSkipVerify: true} // Verified in the mandatory callback below.
	cfg.VerifyConnection = func(state tls.ConnectionState) error {
		if len(state.PeerCertificates) == 0 {
			return errors.New("missing peer certificate")
		}
		leaf := state.PeerCertificates[0]
		got := sha256.Sum256(leaf.Raw)
		if subtle.ConstantTimeCompare(pin[:], got[:]) != 1 {
			return errors.New("certificate pin mismatch")
		}
		now := time.Now()
		if now.Before(leaf.NotBefore) || now.After(leaf.NotAfter) {
			return errors.New("pinned certificate expired or not yet valid")
		}
		if len(leaf.ExtKeyUsage) > 0 {
			allowed := false
			for _, usage := range leaf.ExtKeyUsage {
				if usage == x509.ExtKeyUsageServerAuth || usage == x509.ExtKeyUsageAny {
					allowed = true
				}
			}
			if !allowed {
				return errors.New("certificate not valid for server authentication")
			}
		}
		return nil
	}
	return cfg, nil
}

// pinnedFileTLS rereads only the host-approved private leaf on every handshake.
// A missing or invalid pin blocks connections until that file is repaired.
func pinnedFileTLS(path string) *tls.Config {
	return &tls.Config{
		MinVersion:         tls.VersionTLS12,
		InsecureSkipVerify: true, // Exact leaf verification is mandatory below.
		VerifyConnection: func(state tls.ConnectionState) error {
			pin, err := readPrivate(path, 32768)
			if err != nil {
				return errors.New("cannot read glagol pin")
			}
			verifier, err := PinnedTLS(pin)
			if err != nil {
				return err
			}
			return verifier.VerifyConnection(state)
		},
	}
}

func hostTLS(certPEM []byte) (*tls.Config, error) {
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(certPEM) {
		return nil, errors.New("invalid server CA bundle")
	}
	return &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots}, nil
}
func ControlHandler(c *Controller, token string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.Header().Set("Cache-Control", "no-store")
		host, _, err := net.SplitHostPort(r.RemoteAddr)
		if err != nil || net.ParseIP(host) == nil || !net.ParseIP(host).IsLoopback() {
			http.Error(w, "loopback only", http.StatusForbidden)
			return
		}
		if r.URL.Path == "/healthz" {
			if r.Method != "GET" {
				w.WriteHeader(405)
				return
			}
			json.NewEncoder(w).Encode(c.Status(time.Now()))
			return
		}
		if r.URL.Path != "/control" {
			w.WriteHeader(404)
			return
		}
		if r.Method != "POST" {
			w.WriteHeader(405)
			return
		}
		auth := r.Header.Get("Authorization")
		if token == "" || subtle.ConstantTimeCompare([]byte(auth), []byte("Bearer "+token)) != 1 {
			w.WriteHeader(401)
			return
		}
		b, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 1024))
		if err != nil {
			w.WriteHeader(400)
			return
		}
		var req struct {
			Action string `json:"action"`
		}
		if strictJSON(b, &req) != nil {
			w.WriteHeader(400)
			return
		}
		switch req.Action {
		case "on", "off", "stop", "reset", "direct":
		default:
			w.WriteHeader(400)
			return
		}
		result, err := c.Apply(req.Action, time.Now())
		if err != nil {
			w.WriteHeader(503)
			json.NewEncoder(w).Encode(struct {
				Error   string `json:"error"`
				Enabled bool   `json:"enabled"`
			}{"control apply failed", result.Enabled})
			return
		}
		json.NewEncoder(w).Encode(result)
	})
}

// Capture owns only the socket it created. VQE sends nonblocking unixgram PCM;
// there is no network listener, file recording or replay on re-enable.
type Capture struct {
	mu      sync.Mutex
	path    string
	consume func([]byte)
	conn    *net.UnixConn
	owned   os.FileInfo
}

func NewCapture(path string, consume func([]byte)) *Capture {
	return &Capture{path: path, consume: consume}
}
func (c *Capture) SetEnabled(enabled bool) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !enabled {
		if c.conn != nil {
			c.conn.Close()
			c.conn = nil
		}
		if c.owned != nil {
			s, err := os.Lstat(c.path)
			if err == nil && os.SameFile(s, c.owned) {
				err = os.Remove(c.path)
			}
			c.owned = nil
			return err
		}
		return nil
	}
	if c.conn != nil {
		return nil
	}
	if !filepath.IsAbs(c.path) || len(c.path) > 107 {
		return errors.New("invalid capture socket path")
	}
	if err := privateDir(filepath.Dir(c.path)); err != nil {
		return err
	}
	if _, err := os.Lstat(c.path); !errors.Is(err, os.ErrNotExist) {
		return errors.New("capture socket path already exists")
	}
	conn, err := net.ListenUnixgram("unixgram", &net.UnixAddr{Name: c.path, Net: "unixgram"})
	if err != nil {
		return err
	}
	if err = os.Chmod(c.path, 0600); err != nil {
		conn.Close()
		os.Remove(c.path)
		return err
	}
	c.conn = conn
	c.owned, err = os.Lstat(c.path)
	if err != nil {
		conn.Close()
		c.conn = nil
		return err
	}
	// The reader holds the ownership lock across consume; SetEnabled(false)
	// returns only after the final old callback, so old PCM cannot enter a new run.
	go func() {
		b := make([]byte, maxPCM+1)
		for {
			n, _, flags, _, err := conn.ReadMsgUnix(b, nil)
			if err != nil {
				return
			}
			if flags&syscall.MSG_TRUNC != 0 || n == 0 || n > maxPCM || n%2 != 0 {
				continue
			}
			c.mu.Lock()
			if c.conn == conn && c.consume != nil {
				c.consume(b[:n])
			}
			c.mu.Unlock()
		}
	}()
	return nil
}

type Indicator struct {
	Path   string   `json:"path"`
	Others []string `json:"others"`
	owned  bool
}

func (i *Indicator) Update(active, nativeIdle bool) error {
	if i == nil {
		return nil
	}
	value, err := os.ReadFile(i.Path)
	if err != nil {
		i.owned = false
		return err
	}
	current := strings.TrimSpace(string(value))
	if current != "6" {
		i.owned = false
	}
	if !nativeIdle {
		return nil
	}
	for _, path := range i.Others {
		b, err := os.ReadFile(path)
		if err != nil {
			i.owned = false
			return err
		}
		if strings.TrimSpace(string(b)) != "0" {
			return nil
		}
	}
	target := ""
	if active && current == "0" {
		target = "6"
	} else if !active && i.owned && current == "6" {
		target = "0"
	}
	if target == "" {
		return nil
	}
	if err := os.WriteFile(i.Path, []byte(target), 0600); err != nil {
		i.owned = false
		return err
	}
	i.owned = target == "6"
	return nil
}
