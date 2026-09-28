package influxdb

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/influxdata/line-protocol/v2/lineprotocol"
)

// writeClient sends line protocol to the native InfluxDB 3 write endpoint.
type writeClient struct {
	httpClient *http.Client
	url        string
	token      string
	precision  lineprotocol.Precision
}

func newWriteClient(conf Config) (*writeClient, error) {
	base, err := url.Parse(conf.Addr.String)
	if err != nil {
		return nil, fmt.Errorf("invalid Addr: %w", err)
	}
	validScheme := base.Scheme == "http" || base.Scheme == "https"
	if !validScheme || base.Host == "" || base.User != nil || base.RawQuery != "" || base.Fragment != "" {
		return nil, fmt.Errorf(
			"the Addr option must be an HTTP(S) server URL without credentials, query parameters, or a fragment",
		)
	}
	var precision lineprotocol.Precision
	var precisionName string
	switch time.Duration(conf.Precision.Duration) {
	case time.Nanosecond:
		precision, precisionName = lineprotocol.Nanosecond, "nanosecond"
	case time.Microsecond:
		precision, precisionName = lineprotocol.Microsecond, "microsecond"
	case time.Millisecond:
		precision, precisionName = lineprotocol.Millisecond, "millisecond"
	case time.Second:
		precision, precisionName = lineprotocol.Second, "second"
	default:
		return nil, fmt.Errorf("the Precision option must be 1ns, 1us, 1ms, or 1s")
	}
	if conf.WriteTimeout.Duration <= 0 {
		return nil, fmt.Errorf("the WriteTimeout option must be positive")
	}
	endpoint := base.JoinPath("api/v3/write_lp")
	endpoint.RawQuery = url.Values{
		"db":             {conf.Database.String},
		"precision":      {precisionName},
		"accept_partial": {"false"},
		"no_sync":        {"false"},
	}.Encode()
	defaultTransport, ok := http.DefaultTransport.(*http.Transport)
	if !ok {
		return nil, fmt.Errorf("the default HTTP transport must be an *http.Transport")
	}
	transport := defaultTransport.Clone()
	transport.TLSClientConfig = &tls.Config{
		InsecureSkipVerify: conf.InsecureSkipTLSVerify.Bool, //nolint:gosec
	}
	return &writeClient{
		httpClient: &http.Client{
			Transport: transport,
			Timeout:   time.Duration(conf.WriteTimeout.Duration),
			CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
		url: endpoint.String(), token: conf.Token.String, precision: precision,
	}, nil
}

func (c *writeClient) Write(ctx context.Context, batch []byte) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.url, bytes.NewReader(batch))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "text/plain; charset=utf-8")
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	response, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("writing to InfluxDB 3: %w", err)
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		body, readErr := io.ReadAll(io.LimitReader(response.Body, 4096))
		if readErr != nil {
			return fmt.Errorf("writing to InfluxDB 3 returned %s (reading error response: %w)", response.Status, readErr)
		}
		return fmt.Errorf("writing to InfluxDB 3 returned %s: %s", response.Status, strings.TrimSpace(string(body)))
	}
	_, err = io.Copy(io.Discard, response.Body)
	return err
}
