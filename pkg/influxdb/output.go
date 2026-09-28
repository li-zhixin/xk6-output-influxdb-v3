// Package influxdb sends k6 metrics to the native InfluxDB 3 write API.
package influxdb

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/influxdata/line-protocol/v2/lineprotocol"
	"github.com/sirupsen/logrus"
	"go.k6.io/k6/v2/metrics"
	"go.k6.io/k6/v2/output"
)

// FieldKind defines Enum for tag-to-field type conversion
type FieldKind int

const (
	// String denotes string datatype
	String FieldKind = iota
	// Int denotes integer datatype
	Int
	// Float denotes float datatype
	Float
	// Bool denotes a boolean datatype
	Bool
)

var _ output.Output = new(Output)

// Output is the influxdb Output struct
type Output struct {
	output.SampleBuffer

	config Config

	params          output.Params
	periodicFlusher *output.PeriodicFlusher
	logger          logrus.FieldLogger
	fieldKinds      map[string]FieldKind
	writer          *writeClient
	semaphoreCh     chan struct{}
	wg              sync.WaitGroup
	errMu           sync.Mutex
	flushErr        error
}

// New returns new InfluxDB Output
func New(params output.Params) (*Output, error) {
	logger := params.Logger.WithFields(logrus.Fields{"output": "InfluxDBv3"})

	conf, err := GetConsolidatedConfig(params.JSONConfig, params.Environment, params.ConfigArgument)
	if err != nil {
		return nil, err
	}
	if conf.Database.String == "" {
		return nil, fmt.Errorf("the Database option is required")
	}
	if conf.ConcurrentWrites.Int64 <= 0 {
		return nil, fmt.Errorf("the ConcurrentWrites option must be a positive number")
	}
	fldKinds, err := makeFieldKinds(conf)
	if err != nil {
		return nil, err
	}
	writer, err := newWriteClient(conf)
	if err != nil {
		return nil, err
	}
	return &Output{
		params:      params,
		logger:      logger,
		config:      conf,
		fieldKinds:  fldKinds,
		writer:      writer,
		semaphoreCh: make(chan struct{}, conf.ConcurrentWrites.Int64),
		wg:          sync.WaitGroup{},
	}, nil
}

// Description returns a human-readable description of the output.
func (o *Output) Description() string {
	return fmt.Sprintf("InfluxDBv3 (%s)", o.config.Addr.String)
}

// Start initializes the SampleBuffer for collect samples.
func (o *Output) Start() error {
	o.logger.Debug("Starting...")
	pf, err := output.NewPeriodicFlusher(time.Duration(o.config.PushInterval.Duration), o.flushMetrics)
	if err != nil {
		return err
	}
	o.logger.Debug("Started")
	o.periodicFlusher = pf
	return nil
}

// Stop flushes any remaining metrics and stops the goroutine.
func (o *Output) Stop() error {
	o.logger.Debug("Stopping...")
	o.periodicFlusher.Stop()
	o.wg.Wait()
	o.writer.httpClient.CloseIdleConnections()
	o.logger.Debug("Stopped")
	return o.flushErr
}

func (o *Output) extractTagsToValues(tags map[string]string, values map[string]any) map[string]any {
	for tag, kind := range o.fieldKinds {
		if val, ok := tags[tag]; ok {
			var v any
			var err error
			switch kind {
			case String:
				v = val
			case Bool:
				v, err = strconv.ParseBool(val)
			case Float:
				v, err = strconv.ParseFloat(val, 64)
			case Int:
				v, err = strconv.ParseInt(val, 10, 64)
			}
			if err == nil {
				values[tag] = v
			} else {
				values[tag] = val
			}
			delete(tags, tag)
		}
	}
	return values
}

func (o *Output) batchFromSamples(containers []metrics.SampleContainer) ([]byte, error) {
	type cacheItem struct {
		tags   map[string]string
		values map[string]any
	}
	cache := map[*metrics.TagSet]cacheItem{}

	var encoder lineprotocol.Encoder
	encoder.SetPrecision(o.writer.precision)
	for _, container := range containers {
		samples := container.GetSamples()
		for _, sample := range samples {
			var tags map[string]string
			values := make(map[string]any)
			if cached, ok := cache[sample.Tags]; ok {
				tags = cached.tags
				maps.Copy(values, cached.values)
			} else {
				tags = sample.Tags.Map()
				o.extractTagsToValues(tags, values)
				cache[sample.Tags] = cacheItem{tags, values}
			}
			values["value"] = sample.Value
			if _, exists := tags["value"]; exists {
				return nil, fmt.Errorf("metric %s has a tag named value that conflicts with the metric field", sample.Metric.Name)
			}
			encoder.StartLine(sample.Metric.Name)
			for _, key := range slices.Sorted(maps.Keys(tags)) {
				if tags[key] != "" {
					encoder.AddTag(key, tags[key])
				}
			}
			for _, key := range slices.Sorted(maps.Keys(values)) {
				value, ok := lineprotocol.NewValue(values[key])
				if !ok {
					return nil, fmt.Errorf("metric %s has an invalid value for field %s", sample.Metric.Name, key)
				}
				encoder.AddField(key, value)
			}
			encoder.EndLine(sample.Time)
			if err := encoder.Err(); err != nil {
				return nil, fmt.Errorf("encoding metric %s: %w", sample.Metric.Name, err)
			}
		}
	}

	return encoder.Bytes(), nil
}

func (o *Output) flushMetrics() {
	samples := o.GetBufferedSamples()
	if len(samples) == 0 {
		return
	}

	o.wg.Add(1)
	o.semaphoreCh <- struct{}{}
	go func() {
		defer func() {
			<-o.semaphoreCh
			o.wg.Done()
		}()

		start := time.Now()
		batch, err := o.batchFromSamples(samples)
		if err == nil && len(batch) == 0 {
			return
		}
		if err == nil {
			o.logger.WithField("bytes", len(batch)).Debug("Sending metrics points...")
			err = o.writer.Write(context.Background(), batch)
		}
		if err != nil {
			o.errMu.Lock()
			if o.flushErr == nil {
				o.flushErr = err
			}
			o.errMu.Unlock()
			o.logger.WithError(err).
				WithField("elapsed", time.Since(start)).
				Error("Couldn't send metrics points")
			return
		}

		d := time.Since(start)
		o.logger.WithField("elapsed", d).Debug("Metrics points have been sent")
		if d > time.Duration(o.config.PushInterval.Duration) {
			msg := "The flush operation took higher than the expected set push interval. " +
				"If you see this message multiple times then the setup or configuration " +
				"need to be adjusted to achieve a sustainable rate."
			o.logger.WithField("t", d).Warn(msg)
		}
	}()
}

// MakeFieldKinds reads the Config and returns a lookup map of tag names to
// the field type their values should be converted to.
func makeFieldKinds(conf Config) (map[string]FieldKind, error) {
	fieldKinds := make(map[string]FieldKind)
	for _, tag := range conf.TagsAsFields {
		var fieldName, fieldType string
		s := strings.SplitN(tag, ":", 2)
		if len(s) == 1 {
			fieldName, fieldType = s[0], "string"
		} else {
			fieldName, fieldType = s[0], s[1]
		}
		if fieldName == "value" {
			return nil, fmt.Errorf("value is reserved for the metric field and cannot be used in TagsAsFields")
		}

		err := checkDuplicatedTypeDefinitions(fieldKinds, fieldName)
		if err != nil {
			return nil, err
		}

		switch fieldType {
		case "string":
			fieldKinds[fieldName] = String
		case "bool":
			fieldKinds[fieldName] = Bool
		case "float":
			fieldKinds[fieldName] = Float
		case "int":
			fieldKinds[fieldName] = Int
		default:
			return nil, fmt.Errorf("an invalid type (%s) is specified for an InfluxDB field (%s)",
				fieldType, fieldName)
		}
	}

	return fieldKinds, nil
}

func checkDuplicatedTypeDefinitions(fieldKinds map[string]FieldKind, tag string) error {
	if _, found := fieldKinds[tag]; found {
		return fmt.Errorf("a tag name (%s) shows up more than once in InfluxDB field type configurations", tag)
	}
	return nil
}
