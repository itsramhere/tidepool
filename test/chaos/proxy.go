// Package chaos hosts the multi-node partition/convergence tests and the
// blockable TCP proxy they run through (PLAN.md milestone M9).
package chaos

import (
	"io"
	"net"
	"sync"
	"sync/atomic"
)

// Proxy forwards TCP to a target and can be blocked: while blocked, new
// connections are refused and existing ones are closed, so clients and
// pullers see hard failures and must reconnect.
type Proxy struct {
	ln       net.Listener
	target   string
	blocked  atomic.Bool
	connsMu  sync.Mutex
	conns    map[net.Conn]struct{}
	stopOnce sync.Once
}

// StartProxy begins accepting on 127.0.0.1:0 and forwarding to target.
func StartProxy(target string) (*Proxy, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	p := &Proxy{ln: ln, target: target, conns: make(map[net.Conn]struct{})}
	go p.acceptLoop()
	return p, nil
}

// Addr returns the proxy's local address.
func (p *Proxy) Addr() string { return p.ln.Addr().String() }

// Close stops the proxy for good.
func (p *Proxy) Close() { p.stopOnce.Do(func() { _ = p.ln.Close() }) }

// Block refuses new connections and closes existing ones.
func (p *Proxy) Block() {
	if !p.blocked.CompareAndSwap(false, true) {
		return
	}
	p.connsMu.Lock()
	defer p.connsMu.Unlock()
	for c := range p.conns {
		_ = c.Close()
	}
	p.conns = make(map[net.Conn]struct{})
}

// Unblock resumes forwarding.
func (p *Proxy) Unblock() { p.blocked.Store(false) }

func (p *Proxy) acceptLoop() {
	for {
		down, err := p.ln.Accept()
		if err != nil {
			return
		}
		if p.blocked.Load() {
			_ = down.Close()
			continue
		}
		go p.handle(down)
	}
}

func (p *Proxy) handle(down net.Conn) {
	up, err := net.Dial("tcp", p.target)
	if err != nil {
		_ = down.Close()
		return
	}
	p.track(down, up)
	go copyHalf(down, up)
	copyHalf(up, down)
	p.untrack(down, up)
}

// track registers the connection pair so Block can close it; untrack removes
// it when the pair ends on its own.
func (p *Proxy) track(conns ...net.Conn) {
	p.connsMu.Lock()
	defer p.connsMu.Unlock()
	for _, c := range conns {
		p.conns[c] = struct{}{}
	}
}

func (p *Proxy) untrack(conns ...net.Conn) {
	p.connsMu.Lock()
	defer p.connsMu.Unlock()
	for _, c := range conns {
		delete(p.conns, c)
	}
}

func copyHalf(src, dst net.Conn) {
	_, _ = io.Copy(dst, src)
	_ = src.Close()
	_ = dst.Close()
}
