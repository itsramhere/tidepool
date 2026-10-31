package client

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sync"

	"github.com/itsramhere/tidepool/internal/crdt"
)

// PersistedState is the serializable local model a LocalStore keeps across
// client restarts: the actor identity (so a restored client keeps speaking
// as the same device) and every known document with its confirmed snapshot
// and its pending ops.
type PersistedState struct {
	Actor crdt.ActorID   `json:"actor,omitempty"`
	Docs  []PersistedDoc `json:"docs,omitempty"`
}

// PersistedDoc is one document's local state. Snapshot is the §4.6 snapshot
// JSON of the confirmed document; Pending holds local ops the server has not
// confirmed yet, in creation order.
type PersistedDoc struct {
	Coll     string          `json:"coll"`
	Doc      string          `json:"doc"`
	Snapshot json.RawMessage `json:"snapshot,omitempty"`
	Pending  []crdt.Op       `json:"pending,omitempty"`
}

// LocalStore persists the client state. Load returns a zero PersistedState
// (and no error) when nothing has been saved yet. Save may be called after
// every state change; implementations must tolerate that frequency.
type LocalStore interface {
	Load() (PersistedState, error)
	Save(state PersistedState) error
}

// MemoryStore keeps the state in process memory. Elements of a saved state
// are shared with the caller (op values are immutable once created), so the
// client never mutates them in place.
type MemoryStore struct {
	mu    sync.Mutex
	state *PersistedState
}

// NewMemoryStore returns an empty in-process LocalStore.
func NewMemoryStore() *MemoryStore { return &MemoryStore{} }

// Load returns the last saved state, or a zero state when empty.
func (m *MemoryStore) Load() (PersistedState, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.state == nil {
		return PersistedState{}, nil
	}
	return *m.state, nil
}

// Save replaces the stored state with a shallow copy.
func (m *MemoryStore) Save(state PersistedState) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	copied := state
	if state.Docs != nil {
		copied.Docs = make([]PersistedDoc, len(state.Docs))
		copy(copied.Docs, state.Docs)
	}
	m.state = &copied
	return nil
}

// FileStore persists the state as one JSON file, written atomically: the
// data lands in a temp file in the same directory which is then renamed
// over the target, so a crash never leaves a half-written state behind.
type FileStore struct {
	path string
	mu   sync.Mutex
}

// NewFileStore returns a LocalStore backed by the file at path.
func NewFileStore(path string) *FileStore { return &FileStore{path: path} }

// Load reads the state file. A missing file means a fresh install.
func (f *FileStore) Load() (PersistedState, error) {
	data, err := os.ReadFile(f.path)
	if errors.Is(err, fs.ErrNotExist) {
		return PersistedState{}, nil
	}
	if err != nil {
		return PersistedState{}, fmt.Errorf("client: read state file: %w", err)
	}
	var state PersistedState
	if err := json.Unmarshal(data, &state); err != nil {
		return PersistedState{}, fmt.Errorf("client: parse state file %s: %w", f.path, err)
	}
	return state, nil
}

// Save writes the state file atomically via a temp file and rename.
func (f *FileStore) Save(state PersistedState) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	data, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("client: marshal state: %w", err)
	}
	dir := filepath.Dir(f.path)
	tmp, err := os.CreateTemp(dir, ".tidepool-state-*.json")
	if err != nil {
		return fmt.Errorf("client: create temp state file: %w", err)
	}
	tmpName := tmp.Name()
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return fmt.Errorf("client: write state file: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return fmt.Errorf("client: sync state file: %w", err)
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmpName)
		return fmt.Errorf("client: close state file: %w", err)
	}
	if err := os.Rename(tmpName, f.path); err != nil {
		os.Remove(tmpName)
		return fmt.Errorf("client: rename state file: %w", err)
	}
	return nil
}
