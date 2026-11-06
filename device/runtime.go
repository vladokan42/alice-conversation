package device

import (
	"context"
	"errors"
	"io"
	"log"
	"net"
	"net/http"
	"sync"
	"time"
)

// Run starts only loopback control plus the private Unix datagram receiver.
// Errors are technical categories: callers must never log credentials,
// native response objects, reply text, audio or full HTTP/WebSocket errors.
func Run(parent context.Context, cfg Config) error {
	if err := cfg.Validate(); err != nil {
		return err
	}
	controlToken, err := readToken(cfg.ControlTokenFile)
	if err != nil {
		return err
	}
	hostToken, err := readToken(cfg.TokenFile)
	if err != nil {
		return err
	}
	nativeToken, err := readToken(cfg.GlagolTokenFile)
	if err != nil {
		return err
	}
	ca, err := readPrivate(cfg.ServerCAFile, 65536)
	if err != nil {
		return errors.New("cannot read server CA file")
	}
	serverTLS, err := hostTLS(ca)
	if err != nil {
		return err
	}
	nativeTLS := pinnedFileTLS(cfg.GlagolCertFile)
	c, err := NewController(cfg)
	if err != nil {
		return err
	}
	serverTransport := &http.Transport{TLSClientConfig: serverTLS, DialContext: (&net.Dialer{Timeout: 3 * time.Second}).DialContext, TLSHandshakeTimeout: 3 * time.Second}
	nativeTransport := &http.Transport{TLSClientConfig: nativeTLS, DialContext: (&net.Dialer{Timeout: 3 * time.Second}).DialContext, TLSHandshakeTimeout: 3 * time.Second}
	defer serverTransport.CloseIdleConnections()
	defer nativeTransport.CloseIdleConnections()
	native := newNative("wss://"+cfg.GlagolHost, nativeToken, &http.Client{Transport: nativeTransport}, c)
	// Refresh is provisioned independently; never reuse an expired credential
	// after a network/Glagol reconnect and never put account OAuth on the device.
	native.tokenFile = cfg.GlagolTokenFile
	c.interrupt = native.stopOwned
	c.release = native.release
	capture := NewCapture(cfg.CaptureSocket, func(b []byte) { c.AcceptPCM(b, time.Now()) })
	c.capture = capture.SetEnabled
	if err := capture.SetEnabled(c.Status(time.Now()).Enabled); err != nil {
		return errors.New("cannot open private capture socket")
	}
	defer capture.SetEnabled(false)
	listener, err := net.Listen("tcp", cfg.ControlListen)
	if err != nil {
		return errors.New("cannot bind loopback control")
	}
	defer listener.Close()
	server := &http.Server{Handler: ControlHandler(c, controlToken), ReadHeaderTimeout: time.Second, ReadTimeout: 2 * time.Second, WriteTimeout: 2 * time.Second, IdleTimeout: 10 * time.Second, MaxHeaderBytes: 4096, ErrorLog: log.New(io.Discard, "", 0)}
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	var workers sync.WaitGroup
	start := func(f func()) { workers.Go(f) }
	start(func() { runHost(ctx, cfg, c, &http.Client{Transport: serverTransport}, hostToken) })
	start(func() { native.run(ctx) })
	serveErrors := make(chan error, 1)
	start(func() { serveErrors <- server.Serve(listener) })
	pulse := time.NewTicker(25 * time.Millisecond)
	defer pulse.Stop()
	defer func() {
		cancel()
		c.mu.Lock()
		c.invalidate("cancel", time.Now())
		c.ready = false
		c.mu.Unlock()
		server.Close()
		capture.SetEnabled(false)
		workers.Wait()
		c.mu.Lock()
		idle := c.nativeIdle
		c.mu.Unlock()
		cfg.Indicator.Update(false, idle)
	}()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-serveErrors:
			return errors.New("loopback control stopped")
		case now := <-pulse.C:
			c.Tick(now)
			c.mu.Lock()
			idle := c.nativeIdle
			active := !c.blocked(now)
			c.mu.Unlock()
			if cfg.Indicator != nil {
				cfg.Indicator.Update(active, idle)
			}
			if speech, ok := c.TakeSpeech(now); ok {
				ack, err := c.beginSpeech(ctx, speech, native.beginSpeech)
				if err != nil {
					c.SpeechSent(speech.Epoch, false, time.Now())
					continue
				}
				start(func() {
					ok := <-ack
					c.SpeechSent(speech.Epoch, ok, time.Now())
					if !ok {
						native.stopOwned(speech.Epoch)
					}
				})
			}
		}
	}
}
