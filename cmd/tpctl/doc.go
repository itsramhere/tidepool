package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/itsramhere/tidepool/internal/crdt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
)

func docCmd(args []string, out io.Writer) error {
	if len(args) == 0 || args[0] != "get" {
		return fmt.Errorf("usage: tpctl doc get [--url URL] [--token TOKEN] coll/doc")
	}
	fs := flag.NewFlagSet("tpctl doc get", flag.ContinueOnError)
	origin := fs.String("url", "http://127.0.0.1:7101", "admin listener URL")
	token := fs.String("token", os.Getenv("TIDEPOOL_ADMIN_TOKEN"), "admin bearer token (or TIDEPOOL_ADMIN_TOKEN)")
	if err := fs.Parse(args[1:]); err != nil {
		return fmt.Errorf("doc flags: %w", err)
	}
	if fs.NArg() != 1 {
		return fmt.Errorf("doc get requires coll/doc")
	}
	parts := strings.Split(fs.Arg(0), "/")
	if len(parts) != 2 {
		return fmt.Errorf("doc get requires coll/doc")
	}
	if err := crdt.ValidateKey(crdt.DocKey{Coll: parts[0], Doc: parts[1]}); err != nil {
		return fmt.Errorf("doc get: %w", err)
	}
	req, err := http.NewRequest("GET", strings.TrimRight(*origin, "/")+"/admin/docs/"+fs.Arg(0), nil)
	if err != nil {
		return fmt.Errorf("admin request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+*token)
	client := &http.Client{Timeout: 10 * time.Second}
	res, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("admin get: %w", err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("admin get: %s", res.Status)
	}
	var body json.RawMessage
	if err := json.NewDecoder(res.Body).Decode(&body); err != nil {
		return fmt.Errorf("decode admin document: %w", err)
	}
	enc := json.NewEncoder(out)
	enc.SetIndent("", "  ")
	if err := enc.Encode(body); err != nil {
		return fmt.Errorf("print document: %w", err)
	}
	return nil
}
