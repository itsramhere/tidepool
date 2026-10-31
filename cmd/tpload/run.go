package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/itsramhere/tidepool/internal/crdt"
	"github.com/itsramhere/tidepool/internal/protocol"
	"sync"
	"time"
)

func seed(ctx context.Context, o options, run string) ([]string, error) {
	actor := crdt.NewActorID()
	conn, err := connect(ctx, o, actor)
	if err != nil {
		return nil, err
	}
	defer closeSocket(conn)
	docs := make([]string, o.Docs)
	for offset := 0; offset < o.Docs; offset += 500 {
		var ops []crdt.Op
		for i := offset; i < min(o.Docs, offset+500); i++ {
			docs[i] = fmt.Sprintf("%s-%d", run, i)
			value, err := json.Marshal(run)
			if err != nil {
				return nil, err
			}
			ops = append(ops, crdt.Op{ID: crdt.OpID{Actor: actor, Seq: 1}, Key: crdt.DocKey{Coll: "load", Doc: docs[i]}, Lamport: 1, Field: "run", Kind: crdt.KindSet, Value: value})
		}
		if err := write(ctx, conn, protocol.NewPush(uint64(offset+1), ops)); err != nil {
			return nil, err
		}
		for {
			_, data, err := conn.Read(ctx)
			if err != nil {
				return nil, fmt.Errorf("seed ack: %w", err)
			}
			var env protocol.Envelope
			if err := json.Unmarshal(data, &env); err != nil {
				return nil, err
			}
			if env.T == protocol.TypeErr {
				return nil, fmt.Errorf("seed refused: %s", data)
			}
			if env.T != protocol.TypeAck {
				continue
			}
			var ack protocol.Ack
			if err := json.Unmarshal(data, &ack); err != nil {
				return nil, err
			}
			for _, result := range ack.Results {
				if result.Status != protocol.StatusOK {
					return nil, fmt.Errorf("seed: %s %s", result.Status, result.Reason)
				}
			}
			break
		}
	}
	return docs, nil
}

func subscribe(ctx context.Context, c *loadClient, run string) error {
	filter := fmt.Sprintf("doc.run == %q", run)
	if err := write(ctx, c.conn, protocol.NewSub("load", "load", filter, nil)); err != nil {
		return err
	}
	for {
		_, data, err := c.conn.Read(ctx)
		if err != nil {
			return fmt.Errorf("subscribe: %w", err)
		}
		var env protocol.Envelope
		if err := json.Unmarshal(data, &env); err != nil {
			return err
		}
		if env.T == protocol.TypeErr {
			return fmt.Errorf("subscribe refused: %s", data)
		}
		if env.T == protocol.TypeReady {
			return nil
		}
	}
}

func run(parent context.Context, o options) (report, error) {
	if o.Clients < 1 || o.Docs < 1 || o.Rate < 1 || o.Duration <= 0 || o.Token == "" {
		return report{}, fmt.Errorf("positive clients/docs/rate/duration and a JWT token are required")
	}
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	setup, stopSetup := context.WithTimeout(ctx, 2*time.Minute)
	defer stopSetup()
	runID := "load-" + string(crdt.NewActorID())
	docs, err := seed(setup, o, runID)
	if err != nil {
		return report{}, fmt.Errorf("seed documents: %w", err)
	}
	clients := make([]*loadClient, 0, o.Clients)
	var workers, writers sync.WaitGroup
	defer func() {
		cancel()
		for _, c := range clients {
			closeSocket(c.conn)
		}
		workers.Wait()
	}()
	stats := &statistics{}
	for range o.Clients {
		actor := crdt.NewActorID()
		conn, err := connect(setup, o, actor)
		if err != nil {
			return report{}, err
		}
		c := &loadClient{conn: conn, actor: actor, jobs: make(chan string, 64), pending: map[uint64]time.Time{}}
		clients = append(clients, c)
		if err := subscribe(setup, c, runID); err != nil {
			return report{}, err
		}
	}
	for _, c := range clients {
		workers.Add(2)
		writers.Add(1)
		go func() { defer workers.Done(); defer writers.Done(); c.sendLoop(ctx, stats) }()
		go func() { defer workers.Done(); c.readLoop(ctx, stats) }()
	}
	start := time.Now()
	if err := offer(ctx, o, clients, docs, stats); err != nil {
		return report{}, err
	}
	elapsed := time.Since(start)
	for _, c := range clients {
		close(c.jobs)
	}
	writersDone := make(chan struct{})
	go func() { writers.Wait(); close(writersDone) }()
	drainStart := time.Now()
	deadline := drainStart.Add(10 * time.Second)
	for {
		stats.mu.Lock()
		expected := stats.acked * uint64(o.Clients-1)
		ready := stats.outstanding == 0 && stats.fanout.count >= expected
		stats.mu.Unlock()
		written := false
		select {
		case <-writersDone:
			written = true
		default:
		}
		if ready && written {
			break
		}
		if time.Now().After(deadline) {
			stats.error()
			break
		}
		select {
		case <-ctx.Done():
			return report{}, ctx.Err()
		case <-time.After(10 * time.Millisecond):
		}
	}
	return stats.snapshot(o, elapsed, time.Since(drainStart)), nil
}

func offer(ctx context.Context, o options, clients []*loadClient, docs []string, stats *statistics) error {
	start := time.Now()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	var offered uint64
	total := uint64(o.Duration.Seconds() * float64(o.Rate))
	for offered < total {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
		target := min(total, uint64(time.Since(start).Seconds()*float64(o.Rate)))
		for offered < target {
			c := clients[offered%uint64(len(clients))]
			doc := docs[(offered/uint64(len(clients))+offered)%uint64(len(docs))]
			select {
			case c.jobs <- doc:
			default:
				stats.error()
			}
			offered++
		}
		stats.mu.Lock()
		stats.offered = offered
		stats.mu.Unlock()
	}
	return nil
}
