package client

import "sync"

// Event là một việc UI cần hiển thị. Seq tăng dần nên trình duyệt chỉ cần hỏi "các event sau seq N":
// tải lại trang hay mở nhiều tab đều không mất và không trùng.
type Event struct {
	Seq  int64  `json:"seq"`
	Type string `json:"type"` // chat | dm | users | error | status | file_start | file_progress | file_done | file_error
	Data any    `json:"data"`
}

// maxEvents: chỉ giữ 2000 event gần nhất để bộ nhớ không tăng mãi.
const maxEvents = 2000

// EventLog là hàng đợi event có giới hạn, an toàn cho nhiều goroutine (listenLoop ghi, Web UI/CLI đọc).
type EventLog struct {
	mu    sync.Mutex
	seq   int64
	items []Event
}

// NewEventLog tạo log rỗng.
func NewEventLog() *EventLog {
	return &EventLog{items: make([]Event, 0, 100)}
}

// Add thêm một event với số thứ tự kế tiếp.
func (l *EventLog) Add(eventType string, data any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.seq++
	l.items = append(l.items, Event{Seq: l.seq, Type: eventType, Data: data})
	if len(l.items) > maxEvents {
		l.items = l.items[len(l.items)-maxEvents:]
	}
}

// Since trả về các event có Seq > s (dùng cho GET /api/events?since=s).
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
