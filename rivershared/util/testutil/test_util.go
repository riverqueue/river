package testutil

import "fmt"

// See docs on PanicTB.
type panicTB struct {
	SuppressOutput bool
}

// PanicTB is an implementation for testing.TB that panics when an error is
// logged or FailNow is called. This is useful to inject into test helpers in
// example tests where no *testing.T is available.
//
// Doesn't fully implement testing.TB. Functions where it's used should take the
// more streamlined TestingTB instead.
func PanicTB() *panicTB {
	return &panicTB{SuppressOutput: true}
}

func (tb *panicTB) Errorf(format string, args ...any) {
	panic(fmt.Sprintf(format, args...))
}

func (tb *panicTB) FailNow() {
	panic("FailNow invoked")
}

func (tb *panicTB) Helper() {}

func (tb *panicTB) Log(args ...any) {
	if !tb.SuppressOutput {
		fmt.Println(args...)
	}
}

func (tb *panicTB) Logf(format string, args ...any) {
	if !tb.SuppressOutput {
		fmt.Printf(format+"\n", args...)
	}
}

// TestingT is an interface wrapper around *testing.T that's implemented by all
// of *testing.T, *testing.F, and *testing.B.
//
// It's used internally to verify that River's test assertions are working as
// expected.
type TestingTB interface {
	Errorf(format string, args ...any)
	FailNow()
	Helper()
	Log(args ...any)
	Logf(format string, args ...any)
}
