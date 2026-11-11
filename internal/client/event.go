package client

import "sync"

type Event struct {
	Seq  int64  `json:"seq"`
	Type string `json:"type"` // "chat", "dm", "users", "status", "error"
	Data any    `json:"data"`
}

type EventLog struct {
	mu    sync.Mutex
	seq   int64
	items []Event
}

func NewEventLog() *EventLog {
	return &EventLog{items: make([]Event, 0, 100)}
}

func (l *EventLog) Add(eventType string, data any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.seq++
	l.items = append(l.items, Event{Seq: l.seq, Type: eventType, Data: data})
	if len(l.items) > 2000 { // Xóa bớt event cũ tránh tràn RAM
		l.items = l.items[len(l.items)-2000:]
	}
}

func (l *EventLog) Since(s int64) []Event {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := []Event{}
	for _, e := range l.items {
		if e.Seq > s {
			out = append(out, e)
		}
	}
	return out
}