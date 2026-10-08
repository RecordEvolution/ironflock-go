package crossbar

import (
	"testing"

	"github.com/gammazero/nexus/v3/transport/serialize"
	"github.com/gammazero/nexus/v3/wamp"
	"github.com/ugorji/go/codec"
)

// TestWireArgsEncodesEmptyListBeforeKwargs pins the workaround for nexus
// encoding a nil Arguments field as nil when ArgumentsKw follows: Crossbar
// rejects that and closes the session, so the args slot must be [].
func TestWireArgsEncodesEmptyListBeforeKwargs(t *testing.T) {
	kwargs := map[string]any{"x": int64(1)}
	data, err := (&serialize.MessagePackSerializer{}).Serialize(&wamp.Publish{
		Request:     1,
		Options:     wamp.Dict{},
		Topic:       "t",
		Arguments:   wireArgs(nil, kwargs),
		ArgumentsKw: kwargs,
	})
	if err != nil {
		t.Fatal(err)
	}
	var frame []any
	if err := codec.NewDecoderBytes(data, &codec.MsgpackHandle{}).Decode(&frame); err != nil {
		t.Fatal(err)
	}
	if len(frame) != 6 {
		t.Fatalf("frame %#v", frame)
	}
	if args, ok := frame[4].([]any); !ok || len(args) != 0 {
		t.Fatalf("args slot is %#v, want an empty list", frame[4])
	}

	if got := wireArgs(nil, nil); got != nil {
		t.Fatalf("no kwargs: got %#v, want nil (trailing fields are trimmed)", got)
	}
	if got := wireArgs([]any{1}, kwargs); len(got) != 1 {
		t.Fatalf("args kept: %#v", got)
	}
	if res := invokeResult(&Result{Kwargs: map[string]any{"k": 1}}, nil); res.Args == nil || len(res.Args) != 0 {
		t.Fatalf("yield args %#v", res.Args)
	}
	if res := invokeResult(nil, &WampError{URI: "app.error", Kwargs: map[string]any{"k": 1}}); res.Args == nil {
		t.Fatalf("error args %#v", res.Args)
	}
}
