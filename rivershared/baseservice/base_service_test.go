package baseservice

import (
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestInit(t *testing.T) {
	t.Parallel()

	archetype := archetype()

	myService := Init(archetype, &MyService{})
	require.NotNil(t, myService.Logger)
	require.Equal(t, "baseservice.MyService", myService.Name)
	require.WithinDuration(t, time.Now().UTC(), myService.Time.NowUTC(), 2*time.Second)
}

type MyService struct {
	BaseService
}

func archetype() *Archetype {
	return &Archetype{
		Logger: slog.New(slog.NewTextHandler(os.Stdout, nil)),
		Time:   &UnStubbableTimeGenerator{},
	}
}

func TestLastPkgPathSegmentIfNotRiver(t *testing.T) {
	t.Parallel()

	require.Empty(t, lastPkgPathSegmentIfNotRiver("github.com/riverqueue/river"))
	require.Equal(t, "riverlog.", lastPkgPathSegmentIfNotRiver("github.com/riverqueue/river/riverlog"))
	require.Equal(t, "riverui.", lastPkgPathSegmentIfNotRiver("github.com/riverqueue/riverui"))
	require.Empty(t, lastPkgPathSegmentIfNotRiver(""))
	require.Empty(t, lastPkgPathSegmentIfNotRiver("/"))
}

func TestSimplifyLogName(t *testing.T) {
	t.Parallel()

	require.Equal(t, "NotGeneric", simplifyLogName("NotGeneric"))

	// Simplified for use during debugging. Real generics will tend to have
	// fully qualified paths and not look like this.
	require.Equal(t, "Simple[int]", simplifyLogName("Simple[int]"))
	require.Equal(t, "Simple[*int]", simplifyLogName("Simple[*int]"))
	require.Equal(t, "Simple[[]int]", simplifyLogName("Simple[[]int]"))
	require.Equal(t, "Simple[[]*int]", simplifyLogName("Simple[[]*int]"))

	// More realistic examples.
	require.Equal(t, "QueryCacher[dbsqlc.JobCountByStateRow]", simplifyLogName("QueryCacher[github.com/riverqueue/riverui/internal/dbsqlc.JobCountByStateRow]"))
	require.Equal(t, "QueryCacher[*dbsqlc.JobCountByStateRow]", simplifyLogName("QueryCacher[*github.com/riverqueue/riverui/internal/dbsqlc.JobCountByStateRow]"))
	require.Equal(t, "QueryCacher[[]dbsqlc.JobCountByStateRow]", simplifyLogName("QueryCacher[[]github.com/riverqueue/riverui/internal/dbsqlc.JobCountByStateRow]"))
	require.Equal(t, "QueryCacher[[]*dbsqlc.JobCountByStateRow]", simplifyLogName("QueryCacher[[]*github.com/riverqueue/riverui/internal/dbsqlc.JobCountByStateRow]"))
}
