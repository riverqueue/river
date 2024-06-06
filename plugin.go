package river

import (
	"log/slog"

	"github.com/riverqueue/river/rivertype"
)

type Pluginable[TTx any] interface {
	Plugin(client *Client[TTx], logger *slog.Logger) rivertype.Plugin
}
