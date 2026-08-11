package riverdriver

import "strings"

// PostgresCapabilities describes database features detected by PostgreSQL drivers.
// Drivers cache a successful detection for their lifetime.
type PostgresCapabilities struct {
	SupportsListenNotify bool
	UniqueInsertMode     UniqueInsertMode
}

// NewPostgresCapabilities detects features from the server's product, version,
// and settings.
func NewPostgresCapabilities(product string, version int32, ybListenNotifyEnabled bool) *PostgresCapabilities {
	// Yugabyte's native notifications require 2025.2.3 or later with
	// ysql_yb_enable_listen_notify=true on both Masters and TServers. The
	// yb_enable_listen_notify setting is false when absent on older versions,
	// so clients automatically fall back to polling even with PollOnly false.
	// Capabilities are cached per driver; a new driver is needed after changing
	// the setting to detect newly enabled notification support.
	return &PostgresCapabilities{
		SupportsListenNotify: !postgresProductIsYugabyte(product) || ybListenNotifyEnabled,
		UniqueInsertMode:     UniqueInsertModeFromProductAndVersion(product, version),
	}
}

func postgresProductIsYugabyte(product string) bool {
	productLower := strings.ToLower(product)
	return strings.Contains(productLower, "-yb") || strings.Contains(productLower, "yugabyte")
}
