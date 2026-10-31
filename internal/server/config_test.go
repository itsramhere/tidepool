package server

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadConfigDefaults(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "cfg.yaml")
	content := `
node_id: node1
listen: "127.0.0.1:7100"
data_dir: data/node1
jwt_secret: secret
rules_file: rules.yaml
`
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if cfg.Fsync != "always" {
		t.Errorf("fsync default = %q, want always", cfg.Fsync)
	}
	if cfg.MaxOpsPerPush != 1000 {
		t.Errorf("max_ops_per_push default = %d, want 1000", cfg.MaxOpsPerPush)
	}
	if cfg.SessionSendBuffer != 1024 {
		t.Errorf("session_send_buffer default = %d, want 1024", cfg.SessionSendBuffer)
	}
	if !cfg.FsyncAlways() {
		t.Error("FsyncAlways() = false, want true")
	}
}

func TestLoadConfigRejects(t *testing.T) {
	tests := []struct {
		name    string
		content string
		wantSub string
	}{
		{"missing node_id", "listen: \":1\"\ndata_dir: d\njwt_secret: s\nrules_file: r\n", "node_id"},
		{"missing listen", "node_id: n\ndata_dir: d\njwt_secret: s\nrules_file: r\n", "listen"},
		{"missing jwt_secret", "node_id: n\nlisten: \":1\"\ndata_dir: d\nrules_file: r\n", "jwt_secret"},
		{"missing rules_file", "node_id: n\nlisten: \":1\"\ndata_dir: d\njwt_secret: s\n", "rules_file"},
		{"bad fsync", "node_id: n\nlisten: \":1\"\ndata_dir: d\njwt_secret: s\nrules_file: r\nfsync: sometimes\n", "fsync"},
		{"zero max_ops", "node_id: n\nlisten: \":1\"\ndata_dir: d\njwt_secret: s\nrules_file: r\nmax_ops_per_push: -1\n", "max_ops_per_push"},
		{"duplicate peer", "node_id: n\nlisten: \":1\"\ndata_dir: d\njwt_secret: s\nrules_file: r\npeers:\n  - {id: p, url: \"http://x\"}\n  - {id: p, url: \"http://y\"}\n", "duplicate peer"},
		{"peer missing url", "node_id: n\nlisten: \":1\"\ndata_dir: d\njwt_secret: s\nrules_file: r\npeers:\n  - {id: p}\n", "peers[0].url"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "cfg.yaml")
			if err := os.WriteFile(path, []byte(tt.content), 0o644); err != nil {
				t.Fatal(err)
			}
			_, err := LoadConfig(path)
			if err == nil {
				t.Fatalf("LoadConfig = nil error, want error containing %q", tt.wantSub)
			}
			if !strings.Contains(err.Error(), tt.wantSub) {
				t.Errorf("error %q does not contain %q", err, tt.wantSub)
			}
		})
	}
}

func TestFsyncNever(t *testing.T) {
	cfg := &Config{Fsync: "never"}
	if cfg.FsyncAlways() {
		t.Error("FsyncAlways() = true for fsync never")
	}
}

// TestLoadDeployConfigs: the shipped deploy configs must stay loadable.
func TestLoadDeployConfigs(t *testing.T) {
	for _, f := range []string{"../../deploy/node1.yaml", "../../deploy/node2.yaml", "../../deploy/node3.yaml"} {
		cfg, err := LoadConfig(f)
		if err != nil {
			t.Fatalf("LoadConfig(%s): %v", f, err)
		}
		if len(cfg.Peers) != 2 {
			t.Errorf("%s: peers = %d, want 2", f, len(cfg.Peers))
		}
		for _, p := range cfg.Peers {
			if p.ID == "" || p.URL == "" || p.Secret == "" {
				t.Errorf("%s: incomplete peer %+v", f, p)
			}
		}
	}
}
