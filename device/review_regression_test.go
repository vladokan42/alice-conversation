package device

import (
	"os"
	"path/filepath"
	"testing"
)

func TestOwnedIndicatorSurvivesNativeBusyUntilSafeClear(t *testing.T) {
	d := t.TempDir()
	led := filepath.Join(d, "blue")
	other := filepath.Join(d, "red")
	os.WriteFile(led, []byte("0"), 0600)
	os.WriteFile(other, []byte("0"), 0600)
	i := &Indicator{Path: led, Others: []string{other}}
	if err := i.Update(true, true); err != nil {
		t.Fatal(err)
	}
	i.Update(true, false)
	i.Update(false, true)
	b, _ := os.ReadFile(led)
	if string(b) != "0" {
		t.Fatal("off retained own blue indicator", string(b))
	}
	i.Update(true, true)
	os.WriteFile(other, []byte("12"), 0600)
	i.Update(false, true)
	b, _ = os.ReadFile(led)
	if string(b) != "6" {
		t.Fatal("changed ring during foreign animation")
	}
	os.WriteFile(other, []byte("0"), 0600)
	i.Update(false, true)
	b, _ = os.ReadFile(led)
	if string(b) != "0" {
		t.Fatal("failed deferred clear after foreign animation")
	}
}
