package componentstatus

type Status int

const (
	Uninitialized Status = iota
	Initializing
	Healthy
	Unhealthy
	ShuttingDown
	Stopped
)

func (cs Status) String() string {
	componentStatusStrings := map[Status]string{
		Uninitialized: "uninitialized",
		Initializing:  "initializing",
		Healthy:       "healthy",
		Unhealthy:     "unhealthy",
		ShuttingDown:  "shutting_down",
		Stopped:       "stopped",
	}

	return componentStatusStrings[cs]
}

type ElectorStatus int

const (
	ElectorNonLeader ElectorStatus = iota
	ElectorLeader
	ElectorResigning
)

type ClientSnapshot struct {
	Elector   ElectorStatus
	Notifier  Status
	Producers map[string]Status
}

func (chs *ClientSnapshot) Healthy() bool {
	allProducersHealthy := true
	for _, status := range chs.Producers {
		if status != Healthy {
			allProducersHealthy = false
			break
		}
	}
	return allProducersHealthy && chs.Notifier == Healthy
}

func (chs *ClientSnapshot) Copy() ClientSnapshot {
	copied := ClientSnapshot{
		Elector:   chs.Elector,
		Notifier:  chs.Notifier,
		Producers: make(map[string]Status),
	}
	for queue, status := range chs.Producers {
		copied.Producers[queue] = status
	}
	return copied
}
