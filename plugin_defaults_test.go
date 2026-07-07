package river

import "github.com/riverqueue/river/rivertype"

var (
	_ rivertype.Hook       = &PluginDefaults{}
	_ rivertype.Middleware = &PluginDefaults{}
	_ rivertype.Plugin     = &PluginDefaults{}
	_ rivertype.Plugin     = &HookDefaults{}
	_ rivertype.Plugin     = &MiddlewareDefaults{}
)
