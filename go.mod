module github.com/RecordEvolution/ironflock-go

go 1.25.0

require (
	github.com/gammazero/nexus/v3 v3.3.0
	github.com/ugorji/go/codec v1.3.1
)

require (
	github.com/gammazero/deque v1.2.1 // indirect
	github.com/gorilla/websocket v1.5.3 // indirect
	golang.org/x/crypto v0.53.0 // indirect
)

// The WAMP library is RecordEvolution's nexus fork (github.com/RecordEvolution/nexus,
// branch v4-contrib), the code ironflock-router is built on. The fork keeps the
// upstream module path, so it is wired in with a replace pinned to a commit.
//
// The replace is required, here and in every module that imports this SDK: Go
// applies replace directives only in the main module, and this SDK does not build
// against upstream nexus. The fork's wamp package extends upstream v3.3.0's API:
// wamp.Unregistered has a Details field, which package wamp reads to notice a
// registration the router revokes (without the fork the build fails there with
// "m.Details undefined (type *"github.com/gammazero/nexus/v3/wamp".Unregistered
// has no field or method Details)"), and wamp.Peer has a Done method, which the
// fork's client relies on and package wamp's peer wrapper implements. The fork's
// client also gives up sends to a dead connection, and its WebSocket writes have
// a deadline. Keep the pinned commit in the README's replace line in step.
replace github.com/gammazero/nexus/v3 => github.com/RecordEvolution/nexus/v3 v3.0.0-20261001140357-5a989b085bbb
