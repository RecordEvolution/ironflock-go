module github.com/RecordEvolution/ironflock-go

go 1.25.0

require github.com/gammazero/nexus/v3 v3.3.0

require (
	github.com/gammazero/deque v1.2.1 // indirect
	github.com/gorilla/websocket v1.5.3 // indirect
	github.com/ugorji/go/codec v1.3.1 // indirect
	golang.org/x/crypto v0.53.0 // indirect
)

// The WAMP library is RecordEvolution's nexus fork (github.com/RecordEvolution/nexus,
// branch v4-contrib), the same code ironflock-router is built on. The fork keeps the
// upstream module path, so it is wired in with a replace pinned to a commit. Its
// exported client API matches upstream v3.3.0; internally it adds dead-writer and
// write-deadline handling, and wamp.Peer has a Done() method.
replace github.com/gammazero/nexus/v3 => github.com/RecordEvolution/nexus/v3 v3.0.0-20261001140357-5a989b085bbb
