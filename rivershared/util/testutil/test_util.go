package testutil

import (
	"bytes"
	"fmt"
)

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

// MockT mocks TestingTB. It's used to let us verify our test helpers.
type MockT struct {
	Failed    bool
	logOutput bytes.Buffer
	tb        TestingTB
}

// NewMockT initializes a new MockT. It takes another TestingTB which is usually
// something like a *testing.T and where logs are emitted to along with being
// internalized and retrievable on LogOutput.
func NewMockT(tb TestingTB) *MockT {
	tb.Helper()
	return &MockT{tb: tb}
}

func (t *MockT) Errorf(format string, args ...any) {
	// Errorf is equivalent to Log + Fail
	t.logOutput.WriteString(fmt.Sprintf(format, args...))
	t.logOutput.WriteString("\n")
	t.Failed = true
}

func (t *MockT) FailNow() {
	t.Failed = true
}

func (t *MockT) Helper() {}

func (t *MockT) Log(args ...any) {
	t.tb.Log(args...)

	t.logOutput.WriteString(fmt.Sprint(args...))
	t.logOutput.WriteString("\n")
}

func (t *MockT) Logf(format string, args ...any) {
	t.tb.Logf(format, args...)

	t.logOutput.WriteString(fmt.Sprintf(format, args...))
	t.logOutput.WriteString("\n")
}

func (t *MockT) LogOutput() string {
	return t.logOutput.String()
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
