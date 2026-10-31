package main

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestDocGet(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer secret" || r.URL.Path != "/admin/docs/cards/c1" {
			t.Error("wrong admin request")
		}
		if _, err := w.Write([]byte(`{"values":{"title":"hello"},"vv":{},"snapshot":{}}`)); err != nil {
			t.Error(err)
		}
	}))
	defer ts.Close()
	var out bytes.Buffer
	if err := docCmd([]string{"get", "--url", ts.URL, "--token", "secret", "cards/c1"}, &out); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), `"title": "hello"`) {
		t.Fatal(out.String())
	}
	if err := docCmd([]string{"get", "../c1"}, &out); err == nil {
		t.Fatal("accepted invalid key")
	}
}
