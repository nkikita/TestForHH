package main

import (
	"container/list"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Broker struct {
	mu     sync.Mutex
	queues map[string]*queue
}

type queue struct {
	msgs    []string
	head    int
	waiters list.List
}

type waiter struct {
	ch       chan delivery
	deadline time.Time
	elem     *list.Element
}

type delivery struct {
	msg string
	ok  bool
}

func NewBroker() *Broker { return &Broker{queues: make(map[string]*queue)} }

func (b *Broker) Put(name, msg string) {
	b.mu.Lock()
	q := b.queues[name]
	if q == nil {
		q = &queue{}
		b.queues[name] = q
	}
	for q.waiters.Len() > 0 {
		w := q.waiters.Remove(q.waiters.Front()).(*waiter)
		w.elem = nil
		if !time.Now().Before(w.deadline) {
			w.ch <- delivery{ok: false}
			continue
		}
		w.ch <- delivery{msg: msg, ok: true}
		b.cleanup(name, q)
		b.mu.Unlock()
		return
	}
	q.msgs = append(q.msgs, msg)
	b.mu.Unlock()
}

func (b *Broker) Get(name string, timeout time.Duration) (string, bool) {
	b.mu.Lock()
	q := b.queues[name]
	if q != nil && q.hasMsg() {
		msg := q.popMsg()
		b.cleanup(name, q)
		b.mu.Unlock()
		return msg, true
	}
	if timeout <= 0 {
		b.mu.Unlock()
		return "", false
	}
	if q == nil {
		q = &queue{}
		b.queues[name] = q
	}
	w := &waiter{ch: make(chan delivery, 1), deadline: time.Now().Add(timeout)}
	w.elem = q.waiters.PushBack(w)
	b.mu.Unlock()

	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case d := <-w.ch:
		return d.msg, d.ok
	case <-timer.C:
		b.mu.Lock()
		if w.elem != nil {
			q.waiters.Remove(w.elem)
			w.elem = nil
			b.cleanup(name, q)
			b.mu.Unlock()
			return "", false
		}
		b.mu.Unlock()
		d := <-w.ch
		return d.msg, d.ok
	}
}

func (q *queue) hasMsg() bool { return q.head < len(q.msgs) }

func (q *queue) popMsg() string {
	msg := q.msgs[q.head]
	q.msgs[q.head] = ""
	q.head++
	switch {
	case q.head == len(q.msgs):
		q.msgs = q.msgs[:0]
		q.head = 0
	case q.head > 64 && q.head*2 >= len(q.msgs):
		n := copy(q.msgs, q.msgs[q.head:])
		q.msgs = q.msgs[:n]
		q.head = 0
	}
	return msg
}

func (b *Broker) cleanup(name string, q *queue) {
	if !q.hasMsg() && q.waiters.Len() == 0 {
		delete(b.queues, name)
	}
}

func (b *Broker) handle(w http.ResponseWriter, r *http.Request) {
	name := strings.TrimPrefix(r.URL.Path, "/")
	if name == "" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	switch r.Method {
	case http.MethodPut:
		vals, ok := r.URL.Query()["v"]
		if !ok {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		msg := ""
		if len(vals) > 0 {
			msg = vals[0]
		}
		b.Put(name, msg)
		w.WriteHeader(http.StatusOK)
	case http.MethodGet:
		timeout, ok := parseTimeout(r)
		if !ok {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		msg, found := b.Get(name, timeout)
		if !found {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		_, _ = w.Write([]byte(msg))
	default:
		w.Header().Set("Allow", "GET, PUT")
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func parseTimeout(r *http.Request) (time.Duration, bool) {
	vals, ok := r.URL.Query()["timeout"]
	if !ok {
		return 0, true
	}
	if len(vals) == 0 {
		return 0, false
	}
	n, err := strconv.ParseInt(vals[0], 10, 64)
	if err != nil || n < 0 || n > int64(1<<63-1)/int64(time.Second) {
		return 0, false
	}
	return time.Duration(n) * time.Second, true
}

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: queue-broker <port>")
		os.Exit(1)
	}
	addr := os.Args[1]
	if !strings.HasPrefix(addr, ":") {
		addr = ":" + addr
	}
	if err := http.ListenAndServe(addr, http.HandlerFunc(NewBroker().handle)); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
