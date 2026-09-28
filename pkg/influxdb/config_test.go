package influxdb

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.k6.io/k6/v2/lib/types"
	"gopkg.in/guregu/null.v3"
)

func TestParseURL(t *testing.T) {
	t.Parallel()
	cases := map[string]Config{
		"":                               {},
		"database":                       {Database: null.StringFrom("database")},
		"/database":                      {Database: null.StringFrom("database")},
		"http://localhost:8181":          {Addr: null.StringFrom("http://localhost:8181")},
		"http://localhost:8181/database": {Addr: null.StringFrom("http://localhost:8181"), Database: null.StringFrom("database")},
	}
	for input, want := range cases {
		t.Run(input, func(t *testing.T) {
			t.Parallel()
			got, err := parseURL(input)
			require.NoError(t, err)
			assert.Equal(t, want, got)
		})
	}
}

func TestGetConsolidatedConfig(t *testing.T) {
	t.Parallel()
	env := map[string]string{
		"K6_INFLUXDB_ADDR":              "http://test-url",
		"K6_INFLUXDB_DATABASE":          "env-db",
		"K6_INFLUXDB_TOKEN":             "test-token",
		"K6_INFLUXDB_INSECURE":          "true",
		"K6_INFLUXDB_PUSH_INTERVAL":     "2s",
		"K6_INFLUXDB_CONCURRENT_WRITES": "8",
		"K6_INFLUXDB_PRECISION":         "1ms",
		"K6_INFLUXDB_WRITE_TIMEOUT":     "10s",
		"K6_INFLUXDB_TAGS_AS_FIELDS":    "vu:int,url",
	}
	conf, err := GetConsolidatedConfig(json.RawMessage(`{"database":"json-db","token":"json-token"}`), env, "http://override:8181/url-db")
	require.NoError(t, err)
	assert.Equal(t, null.StringFrom("http://override:8181"), conf.Addr)
	assert.Equal(t, null.StringFrom("url-db"), conf.Database)
	assert.Equal(t, null.StringFrom("test-token"), conf.Token)
	assert.Equal(t, null.BoolFrom(true), conf.InsecureSkipTLSVerify)
	assert.Equal(t, types.NullDurationFrom(2*time.Second), conf.PushInterval)
	assert.Equal(t, null.IntFrom(8), conf.ConcurrentWrites)
	assert.Equal(t, types.NullDurationFrom(time.Millisecond), conf.Precision)
	assert.Equal(t, types.NullDurationFrom(10*time.Second), conf.WriteTimeout)
	assert.Equal(t, []string{"vu:int", "url"}, conf.TagsAsFields)
}

func TestDatabaseConfigPrecedence(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		raw  string
		env  map[string]string
		url  string
		want string
	}{
		{name: "JSON", raw: `{"database":"json-db"}`, want: "json-db"},
		{name: "environment", env: map[string]string{"K6_INFLUXDB_DATABASE": "env-db"}, want: "env-db"},
		{name: "environment overrides JSON", raw: `{"database":"json-db"}`, env: map[string]string{"K6_INFLUXDB_DATABASE": "env-db"}, want: "env-db"},
		{name: "URL overrides environment", env: map[string]string{"K6_INFLUXDB_DATABASE": "env-db"}, url: "http://localhost:8181/url-db", want: "url-db"},
		{name: "URL without path preserves database", env: map[string]string{"K6_INFLUXDB_DATABASE": "env-db"}, url: "http://localhost:8181", want: "env-db"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var raw json.RawMessage
			if tc.raw != "" {
				raw = json.RawMessage(tc.raw)
			}
			conf, err := GetConsolidatedConfig(raw, tc.env, tc.url)
			require.NoError(t, err)
			assert.Equal(t, null.StringFrom(tc.want), conf.Database)
		})
	}
}
