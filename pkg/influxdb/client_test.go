package influxdb

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.k6.io/k6/v2/lib/types"
	"gopkg.in/guregu/null.v3"
)

func TestWriteClientErrors(t *testing.T) {
	t.Parallel()
	for _, status := range []int{http.StatusBadRequest, http.StatusUnauthorized, http.StatusInternalServerError} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			t.Parallel()
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				http.Error(w, "write rejected", status)
			}))
			defer server.Close()
			conf := NewConfig()
			conf.Addr = null.StringFrom(server.URL)
			client, err := newWriteClient(conf)
			require.NoError(t, err)
			defer client.httpClient.CloseIdleConnections()
			err = client.Write(context.Background(), []byte("gauge value=1\n"))
			require.ErrorContains(t, err, http.StatusText(status))
			require.ErrorContains(t, err, "write rejected")
		})
	}
}

func TestWriteTimeout(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-time.After(time.Second):
		}
	}))
	defer server.Close()
	conf := NewConfig()
	conf.Addr = null.StringFrom(server.URL)
	conf.WriteTimeout = types.NullDurationFrom(20 * time.Millisecond)
	client, err := newWriteClient(conf)
	require.NoError(t, err)
	defer client.httpClient.CloseIdleConnections()
	err = client.Write(context.Background(), []byte("gauge value=1\n"))
	require.True(t, errors.Is(err, context.DeadlineExceeded), "expected a timeout, got %v", err)
}

func TestWriteTLSAndURLPrefix(t *testing.T) {
	t.Parallel()
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/influx/api/v3/write_lp", r.URL.Path)
		assert.Empty(t, r.Header.Get("Authorization"))
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	conf := NewConfig()
	conf.Addr = null.StringFrom(server.URL + "/influx/")
	client, err := newWriteClient(conf)
	require.NoError(t, err)
	defer client.httpClient.CloseIdleConnections()
	require.Error(t, client.Write(context.Background(), []byte("gauge value=1\n")))
	conf.InsecureSkipTLSVerify = null.BoolFrom(true)
	client, err = newWriteClient(conf)
	require.NoError(t, err)
	defer client.httpClient.CloseIdleConnections()
	require.NoError(t, client.Write(context.Background(), []byte("gauge value=1\n")))
}
