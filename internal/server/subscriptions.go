package server

import (
	"sort"
	"sync/atomic"

	"github.com/google/cel-go/cel"
)

var sessionSeq atomic.Uint64

// subscription is one client subscription: a collection plus a CEL filter
// (compiled at subscribe time). inShape holds the doc IDs currently matching
// the subscription's shape, maintained by the committer's broadcast.
type subscription struct {
	id         string
	coll       string
	filter     string
	filterProg cel.Program
	inShape    map[string]struct{}
	sess       *session
}

// subscriptionSet tracks active subscriptions. It is owned by the committer
// goroutine — every mutation and read happens there — so it needs no lock
// (PLAN.md golden rule 10).
type subscriptionSet struct {
	bySession map[*session]map[string]*subscription
	byColl    map[string][]*subscription
}

func newSubscriptionSet() *subscriptionSet {
	return &subscriptionSet{
		bySession: make(map[*session]map[string]*subscription),
		byColl:    make(map[string][]*subscription),
	}
}

// add registers a sub. The caller must have removed any previous sub with
// the same (session, id).
func (ss *subscriptionSet) add(sub *subscription) {
	if ss.bySession[sub.sess] == nil {
		ss.bySession[sub.sess] = make(map[string]*subscription)
	}
	ss.bySession[sub.sess][sub.id] = sub
	ss.byColl[sub.coll] = append(ss.byColl[sub.coll], sub)
}

// remove unregisters the sub with the given id; ok reports whether it
// existed.
func (ss *subscriptionSet) remove(sess *session, id string) bool {
	subs, ok := ss.bySession[sess]
	if !ok {
		return false
	}
	sub, ok := subs[id]
	if !ok {
		return false
	}
	delete(subs, id)
	if len(subs) == 0 {
		delete(ss.bySession, sess)
	}
	list := ss.byColl[sub.coll]
	for i, s := range list {
		if s == sub {
			ss.byColl[sub.coll] = append(list[:i], list[i+1:]...)
			break
		}
	}
	if len(ss.byColl[sub.coll]) == 0 {
		delete(ss.byColl, sub.coll)
	}
	return true
}

// removeAll unregisters every sub of the session and reports how many were
// removed.
func (ss *subscriptionSet) removeAll(sess *session) int {
	subs, ok := ss.bySession[sess]
	if !ok {
		return 0
	}
	n := 0
	for _, sub := range subs {
		list := ss.byColl[sub.coll]
		for i, s := range list {
			if s == sub {
				ss.byColl[sub.coll] = append(list[:i], list[i+1:]...)
				break
			}
		}
		if len(ss.byColl[sub.coll]) == 0 {
			delete(ss.byColl, sub.coll)
		}
		n++
	}
	delete(ss.bySession, sess)
	return n
}

// forColl returns the subs of the collection sorted by (session id, sub id),
// so fan-out order never depends on Go's map iteration.
func (ss *subscriptionSet) forColl(coll string) []*subscription {
	list := make([]*subscription, len(ss.byColl[coll]))
	copy(list, ss.byColl[coll])
	sort.Slice(list, func(i, j int) bool {
		if list[i].sess.id != list[j].sess.id {
			return list[i].sess.id < list[j].sess.id
		}
		return list[i].id < list[j].id
	})
	return list
}
