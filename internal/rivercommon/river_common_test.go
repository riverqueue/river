package rivercommon

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestJobKindRE(t *testing.T) {
	t.Parallel()

	require.Regexp(t, UserSpecifiedIDOrKindRE, "kind")
	require.Regexp(t, UserSpecifiedIDOrKindRE, "kind123")
	require.Regexp(t, UserSpecifiedIDOrKindRE, "with.dot")
	require.Regexp(t, UserSpecifiedIDOrKindRE, "with:colon")
	require.Regexp(t, UserSpecifiedIDOrKindRE, "with+plus")
	require.Regexp(t, UserSpecifiedIDOrKindRE, "with-hyphen")
	require.Regexp(t, UserSpecifiedIDOrKindRE, "with_underscore")
	require.Regexp(t, UserSpecifiedIDOrKindRE, "with[brackets]")
	require.Regexp(t, UserSpecifiedIDOrKindRE, "with<triangle_brackets>")
	require.Regexp(t, UserSpecifiedIDOrKindRE, "with/slash")
	require.Regexp(t, UserSpecifiedIDOrKindRE, "JobArgsReflectKind[github.com/riverqueue/river.JobArgs·12]")

	require.NotRegexp(t, UserSpecifiedIDOrKindRE, "with space")
	require.NotRegexp(t, UserSpecifiedIDOrKindRE, "with,comma")
	require.NotRegexp(t, UserSpecifiedIDOrKindRE, ":no_leading_special_characters")
}

func TestQueueMetadataWithoutReserved(t *testing.T) {
	t.Parallel()

	require.JSONEq(t,
		`{"user":"value"}`,
		string(QueueMetadataWithoutReserved([]byte(`{"river:internal":{"state":1},"user":"value"}`))),
	)
	require.Equal(t, []byte(`not json`), QueueMetadataWithoutReserved([]byte(`not json`)))
	require.JSONEq(t, `[1,2]`, string(QueueMetadataWithoutReserved([]byte(`{"river:user_metadata":[1,2],"river:internal":{}}`))))
	require.JSONEq(t, `{"user":"value"}`, string(QueueMetadataForUserWrite([]byte(`{"river:internal":{},"user":"value"}`))))
}
