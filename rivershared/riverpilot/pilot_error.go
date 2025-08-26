package riverpilot

type FetchErrorWithWarningLevel struct {
	error
}

func (e *FetchErrorWithWarningLevel) Is(target error) bool {
	_, ok := target.(*FetchErrorWithWarningLevel)
	return ok
}
