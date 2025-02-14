package rivertest

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/rivertype"
)

// Ensure that TimeStub implements rivertype.TimeGenerator.
var _ rivertype.TimeGenerator = &TimeStub{}

func TestTimeStub(t *testing.T) {
	t.Parallel()

	stub := &TimeStub{}

	now := time.Now().UTC()

	require.WithinDuration(t, now, stub.NowUTC(), 2*time.Second)
	require.Nil(t, stub.NowUTCOrNil())

	stub.StubNowUTC(now)
	require.Equal(t, now, stub.NowUTC())
	require.Equal(t, &now, stub.NowUTCOrNil())
}
