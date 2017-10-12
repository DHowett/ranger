package ranger

import (
	"net/url"
	"testing"
)

func TestFailureToConnect(t *testing.T) {
	u, _ := url.Parse("http://257.0.1.258/file")
	r := &HTTPRanger{URL: u}
	err := r.Initialize(1048576)
	if err == nil {
		t.Fail()
	} else {
		t.Log(err)
	}
}
