package river

import (
	"github.com/riverqueue/river/rivershared/baseservice"
	"github.com/riverqueue/river/rivershared/riverpilot"
	"github.com/riverqueue/river/rivershared/startstop"
)

// A plugin API that drivers may implement to extend a River client. Driver
// plugins may, for example, add additional maintenance services.
//
// This should be considered a River internal API and its stability is not
// guaranteed. DO NOT USE.
type driverPlugin[TTx any] interface {
	// PluginInit initializes a plugin with an archetype and client. It's
	// invoked on Client.NewClient.
	PluginInit(archetype *baseservice.Archetype, client *Client[TTx])

	// PluginMaintenanceServices returns additional maintenance services (will
	// only run on an elected leader) for a River client.
	PluginMaintenanceServices() []startstop.Service

	PluginPilot() riverpilot.Pilot

	// PluginServices returns additional non-maintenance services (will run on
	// all clients) for a River client.
	PluginServices() []startstop.Service
}
