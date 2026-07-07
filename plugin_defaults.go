package river

// PluginDefaults should be embedded on plugin implementations. It helps
// identify a struct as both hook and middleware, and guarantees forward
// compatibility in case additions are necessary to the rivertype.Hook or
// rivertype.Middleware interfaces.
type PluginDefaults struct {
	HookDefaults
	MiddlewareDefaults
}

func (d *PluginDefaults) IsPlugin() bool { return true }
