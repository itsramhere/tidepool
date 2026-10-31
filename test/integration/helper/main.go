// Command helper is a tiny Go client driver for the TypeScript SDK's e2e
// test: it connects, optionally subscribes, applies the ops given on the
// command line, waits for them to sync, prints its view of the documents as
// JSON, and exits.
//
//	helpers -url ws://127.0.0.1:PORT/v1/sync -token JWT -actor AAAA... \
//	        -ops "set:cards:c1:title:from-go;inc:cards:c1:votes:2"
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/itsramhere/tidepool/internal/crdt"
	"github.com/itsramhere/tidepool/pkg/client"
)

var chaosSchema = crdt.Schema{
	"title":       crdt.FieldRegister,
	"status":      crdt.FieldRegister,
	"votes":       crdt.FieldCounter,
	"labels":      crdt.FieldSet,
	"checklist":   crdt.FieldList,
	"description": crdt.FieldText,
}

func main() {
	url := flag.String("url", "", "server sync endpoint")
	token := flag.String("token", "", "JWT")
	actor := flag.String("actor", "00000000000000aa", "actor id")
	opsFlag := flag.String("ops", "", "ops to apply: kind:coll:doc:field:value;... (kinds set/inc/add/inserttext)")
	sub := flag.String("sub", "cards", "collection to subscribe (empty = none)")
	flag.Parse()

	c, err := client.New(client.Options{
		URL:     *url,
		Token:   *token,
		Actor:   crdt.ActorID(*actor),
		Schemas: map[string]crdt.Schema{"cards": chaosSchema},
	})
	if err != nil {
		fatal("new client: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = c.Run(ctx) }()
	if *sub != "" {
		c.Subscribe(*sub, "")
		// Reconcile first so edits are stamped against the replicated
		// state (higher lamports) rather than an empty view.
		syncCtx0, syncCancel0 := context.WithTimeout(ctx, 15*time.Second)
		defer syncCancel0()
		if err := c.WaitSynced(syncCtx0); err != nil {
			fatal("initial WaitSynced: %v", err)
		}
	}

	seen := map[crdt.DocKey]struct{}{}
	for _, spec := range strings.Split(*opsFlag, ";") {
		if strings.TrimSpace(spec) == "" {
			continue
		}
		parts := strings.SplitN(spec, ":", 5)
		if len(parts) < 4 {
			fatal("bad op spec %q", spec)
		}
		kind, coll, doc, field := parts[0], parts[1], parts[2], parts[3]
		seen[crdt.DocKey{Coll: coll, Doc: doc}] = struct{}{}
		h := c.Doc(coll, doc)
		switch kind {
		case "set":
			if err := h.Set(field, parts[4]); err != nil {
				fatal("set: %v", err)
			}
		case "inc":
			n, err := strconv.ParseInt(parts[4], 10, 64)
			if err != nil {
				fatal("inc delta: %v", err)
			}
			if err := h.Inc(field, n); err != nil {
				fatal("inc: %v", err)
			}
		case "add":
			if err := h.Add(field, parts[4]); err != nil {
				fatal("add: %v", err)
			}
		case "inserttext":
			// parts[4] is "index:text" — split the text at the first colon.
			idxText := strings.SplitN(parts[4], ":", 2)
			if len(idxText) != 2 {
				fatal("inserttext spec %q wants index:text", spec)
			}
			idx, err := strconv.Atoi(idxText[0])
			if err != nil {
				fatal("inserttext index: %v", err)
			}
			if err := h.InsertText(field, idx, idxText[1]); err != nil {
				fatal("inserttext: %v", err)
			}
		default:
			fatal("unknown op kind %q", kind)
		}
	}

	syncCtx, syncCancel := context.WithTimeout(ctx, 15*time.Second)
	defer syncCancel()
	if err := c.WaitSynced(syncCtx); err != nil {
		fatal("WaitSynced: %v", err)
	}

	// Print every doc the ops touched, keyed coll/doc.
	out := map[string]map[string]any{}
	for key := range seen {
		out[key.Coll+"/"+key.Doc] = c.Doc(key.Coll, key.Doc).Values()
	}
	enc := json.NewEncoder(os.Stdout)
	if err := enc.Encode(out); err != nil {
		fatal("encode: %v", err)
	}
}

func fatal(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "helper: "+format+"\n", args...)
	os.Exit(1)
}
