package influxdb

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.k6.io/k6/v2/metrics"
	"go.k6.io/k6/v2/output"
)

func TestNew(t *testing.T) {
	t.Parallel()
	logger := logrus.New()

	t.Run("DatabaseRequired", func(t *testing.T) {
		t.Parallel()
		_, err := New(output.Params{
			Logger:         logger,
			ConfigArgument: "/",
		})
		require.Error(t, err)
		require.Contains(t, err.Error(), "Database option is required")
	})
	t.Run("ConcurrentWrites", func(t *testing.T) {
		t.Parallel()

		t.Run("FailWithNegative", func(t *testing.T) {
			t.Parallel()
			tests := []string{"0", "-2"}
			for _, tc := range tests {
				_, err := New(output.Params{
					Logger:     logger,
					JSONConfig: json.RawMessage(fmt.Sprintf(`{"database":"b","concurrentWrites":%q}`, tc)),
				})
				require.Error(t, err)
				require.Equal(t, "the ConcurrentWrites option must be a positive number", err.Error())
			}
		})

		t.Run("SuccessWithPositive", func(t *testing.T) {
			t.Parallel()

			_, err := New(output.Params{
				Logger:     logger,
				JSONConfig: json.RawMessage(`{"database":"b","concurrentWrites":"2"}`),
			})
			require.NoError(t, err)
		})
	})
}

func TestInvalidConfig(t *testing.T) {
	t.Parallel()
	cases := []struct{ config, message string }{
		{`{"bucket":"legacy"}`, "Database option is required"},
		{`{"database":"test","precision":"1m"}`, "Precision option must be"},
		{`{"database":"test","precision":"0s"}`, "Precision option must be"},
		{`{"database":"test","writeTimeout":"0s"}`, "WriteTimeout option must be positive"},
		{`{"database":"test","writeTimeout":"-1s"}`, "WriteTimeout option must be positive"},
		{`{"database":"test","addr":"localhost:8181"}`, "Addr option must be"},
		{`{"database":"test","addr":"ftp://localhost"}`, "Addr option must be"},
		{`{"database":"test","tagsAsFields":["value"]}`, "value is reserved"},
	}
	for _, tc := range cases {
		t.Run(tc.config, func(t *testing.T) {
			t.Parallel()
			_, err := New(output.Params{Logger: logrus.New(), JSONConfig: json.RawMessage(tc.config)})
			require.ErrorContains(t, err, tc.message)
		})
	}
}

func TestExtractTagsToValues(t *testing.T) {
	t.Parallel()
	o, err := New(output.Params{
		Logger:     logrus.New(),
		JSONConfig: []byte(`{"database":"mybucket","tagsAsFields":["stringField","stringField2:string","boolField:bool","floatField:float","intField:int"]}`),
	})
	require.NoError(t, err)
	tags := map[string]string{
		"stringField":  "string",
		"stringField2": "string2",
		"boolField":    "true",
		"floatField":   "3.14",
		"intField":     "12345",
	}
	values := o.extractTagsToValues(tags, map[string]any{})

	require.Equal(t, "string", values["stringField"])
	require.Equal(t, "string2", values["stringField2"])
	require.Equal(t, true, values["boolField"])
	require.Equal(t, 3.14, values["floatField"])
	require.Equal(t, int64(12345), values["intField"])
}

func testOutputCycle(t testing.TB, handler http.HandlerFunc, body func(testing.TB, *Output)) {
	ts := httptest.NewServer(handler)
	defer ts.Close()

	c, err := New(output.Params{
		Logger:         logrus.New(),
		ConfigArgument: fmt.Sprintf("%s/testdatabase", ts.URL),
	})
	require.NoError(t, err)

	require.NoError(t, c.Start())
	body(t, c)

	require.NoError(t, c.Stop())
}

func TestOutputFlushMetrics(t *testing.T) {
	t.Parallel()

	var samplesRead int
	defer func() {
		require.Equal(t, 20, samplesRead)
	}()

	registry := metrics.NewRegistry()

	testOutputCycle(t, func(rw http.ResponseWriter, r *http.Request) {
		b := bytes.NewBuffer(nil)
		_, _ = io.Copy(b, r.Body)
		for {
			s, err := b.ReadString('\n')
			if len(s) > 0 {
				samplesRead++
			}
			if err != nil {
				break
			}
		}
		rw.WriteHeader(http.StatusNoContent)
	}, func(tb testing.TB, c *Output) {
		samples := make(metrics.Samples, 10)
		for i := range samples {
			metric, err := registry.NewMetric("test_gauge", metrics.Gauge)
			require.NoError(tb, err)
			samples[i] = metrics.Sample{
				TimeSeries: metrics.TimeSeries{
					Metric: metric,
					Tags: registry.RootTagSet().WithTagsFromMap(map[string]string{
						"something": "else",
						"VU":        "21",
						"else":      "something",
					}),
				},
				Time:  time.Now(),
				Value: 2.0,
			}
		}
		c.AddMetricSamples([]metrics.SampleContainer{samples})
		c.AddMetricSamples([]metrics.SampleContainer{samples})
	})
}

// Exercise the native v3 API and final flush at every supported precision.
func TestOutputWriteV3(t *testing.T) {
	t.Parallel()
	cases := []struct{ precision, apiPrecision, timestamp string }{
		{"1ns", "nanosecond", "1700000000123456789"},
		{"1us", "microsecond", "1700000000123456"},
		{"1ms", "millisecond", "1700000000123"},
		{"1s", "second", "1700000000"},
	}
	for _, tc := range cases {
		t.Run(tc.precision, func(t *testing.T) {
			t.Parallel()
			received := make(chan string, 1)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				assert.Equal(t, http.MethodPost, r.Method)
				assert.Equal(t, "/api/v3/write_lp", r.URL.Path)
				assert.Equal(t, "test-db", r.URL.Query().Get("db"))
				assert.False(t, r.URL.Query().Has("bucket"))
				assert.False(t, r.URL.Query().Has("org"))
				assert.Equal(t, tc.apiPrecision, r.URL.Query().Get("precision"))
				assert.Equal(t, "false", r.URL.Query().Get("accept_partial"))
				assert.Equal(t, "false", r.URL.Query().Get("no_sync"))
				assert.Equal(t, "Bearer test-token", r.Header.Get("Authorization"))
				assert.Equal(t, "text/plain; charset=utf-8", r.Header.Get("Content-Type"))
				body, err := io.ReadAll(r.Body)
				assert.NoError(t, err)
				received <- string(body)
				w.WriteHeader(http.StatusNoContent)
			}))
			defer server.Close()
			out, err := New(output.Params{
				Logger: logrus.New(), ConfigArgument: server.URL,
				Environment: map[string]string{
					"K6_INFLUXDB_DATABASE": "test-db", "K6_INFLUXDB_TOKEN": "test-token",
					"K6_INFLUXDB_PUSH_INTERVAL": "1h", "K6_INFLUXDB_PRECISION": tc.precision,
				},
			})
			require.NoError(t, err)
			require.NoError(t, out.Start())
			registry := metrics.NewRegistry()
			metric, err := registry.NewMetric("test_gauge", metrics.Gauge)
			require.NoError(t, err)
			out.AddMetricSamples([]metrics.SampleContainer{metrics.Samples{{
				TimeSeries: metrics.TimeSeries{
					Metric: metric,
					Tags: registry.RootTagSet().WithTagsFromMap(map[string]string{
						"scenario": "default", "group": "", "vu": "21", "iter": "2", "url": "https://example.com",
					}),
				},
				Time: time.Unix(1700000000, 123456789), Value: 2,
			}}})
			require.NoError(t, out.Stop())
			select {
			case body := <-received:
				assert.Equal(t, "test_gauge,scenario=default iter=2i,url=\"https://example.com\",value=2,vu=21i "+tc.timestamp+"\n", body)
			default:
				t.Fatal("Stop did not flush the buffered metric")
			}
		})
	}
}

func TestOutputReportsWriteFailure(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "schema conflict", http.StatusBadRequest)
	}))
	defer server.Close()
	out, err := New(output.Params{Logger: logrus.New(), ConfigArgument: server.URL + "/test"})
	require.NoError(t, err)
	require.NoError(t, out.Start())
	registry := metrics.NewRegistry()
	metric, err := registry.NewMetric("gauge", metrics.Gauge)
	require.NoError(t, err)
	out.AddMetricSamples([]metrics.SampleContainer{metrics.Samples{{
		TimeSeries: metrics.TimeSeries{Metric: metric, Tags: registry.RootTagSet()},
		Value:      1, Time: time.Now(),
	}}})
	err = out.Stop()
	require.ErrorContains(t, err, "400 Bad Request")
	require.ErrorContains(t, err, "schema conflict")
}

func TestMakeFieldKinds(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		tagsAsFields []string
		expErr       bool
		expFields    map[string]FieldKind
	}{
		{
			name:         "Success",
			tagsAsFields: []string{"vu", "boolField:bool", "floatField:float", "intField:int"},
			expErr:       false,
			expFields:    map[string]FieldKind{"vu": String, "boolField": Bool, "floatField": Float, "intField": Int},
		},
		{
			name:         "Success without seprator",
			tagsAsFields: []string{"iter;bool"}, // this is detected as a string type
			expErr:       false,
			expFields:    map[string]FieldKind{"iter;bool": String},
		},
		{
			name:         "Duplicated field",
			tagsAsFields: []string{"vu", "iter", "url", "boolField:bool", "boolField:bool"},
			expErr:       true,
			expFields:    nil,
		},
		{
			name:         "Duplicated field with different kinds",
			tagsAsFields: []string{"vu", "boolField:bool", "boolField:float"},
			expErr:       true,
			expFields:    nil,
		},
		{
			name:         "Bad type",
			tagsAsFields: []string{"boolField:book"},
			expErr:       true,
			expFields:    nil,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			conf := NewConfig()
			conf.TagsAsFields = tc.tagsAsFields
			fieldKinds, err := makeFieldKinds(conf)
			if tc.expErr {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			assert.Equal(t, tc.expFields, fieldKinds)
		})
	}
}
