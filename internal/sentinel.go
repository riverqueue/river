package internal

// Sentinel provides a type that's internal to River (because this package is
// internal to River). Its purpose to prevent public code from implementing
// interfaces that shouldn't be user-implemented.
type Sentinel struct{}
