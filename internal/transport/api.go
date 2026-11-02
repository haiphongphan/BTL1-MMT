package transport

// Delivery is an ordered byte stream in 1 stream
type Delivery struct {
	StreamID uint32
	Data     []byte
	Fin      bool // true -> stream ends
}

type Stats struct {
	RTT      time.Duration
	Cwnd     int // byte
	InFlight int
	LostPkts uint64
	SentPkts uint64
	State    string
}

// Endpoint is what app layer needs from transport layer
type Endpoint interface {
	SendMessage(b []byte) error            // 1 message = 1 high-priority stream
	OpenStream(urgent bool) io.WriteCloser // long stram, Close() = send FIN
	Recv() <-chan Delivery                 // received data
	Done() <-chan struct{}                 // close when losing connect/timeout
	Stats() Stats
	Close() error
}
