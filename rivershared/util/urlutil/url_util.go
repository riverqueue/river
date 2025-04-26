package urlutil

import "net/url"

// DatabaseSQLCompatibleURL returns databaseURL, but with any known pgx-specific
// URL parameters removed so that it'll be parseable by `database/sql`.
func DatabaseSQLCompatibleURL(databaseURL string) string {
	parsedURL, err := url.Parse(databaseURL)
	if err != nil {
		panic(err)
	}

	query := parsedURL.Query()
	query.Del("pool_max_conns")

	parsedURL.RawQuery = query.Encode()

	return parsedURL.String()
}
