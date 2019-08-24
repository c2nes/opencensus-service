// Copyright 2018, Honeycomb, Hound Technology, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package honeycomb contains a trace exporter for Honeycomb
package honeycomb

import (
	"time"

	libhoney "github.com/honeycombio/libhoney-go"
	"go.opencensus.io/trace"
)

// Exporter is an implementation of trace.Exporter that uploads a span to Honeycomb
type Exporter struct {
	Builder        *libhoney.Builder
	SampleFraction float64
	// Service Name identifies your application. While optional, setting this
	// field is extremely valuable when you instrument multiple services. If set
	// it will be added to all events as `service_name`
	ServiceName string
}

// Annotation represents an annotation with a value and a timestamp.
type Annotation struct {
	Name      string    `json:"name"`
	TraceID   string    `json:"trace.trace_id"`
	ParentID  string    `json:"trace.parent_id"`
	Timestamp time.Time `json:"timestamp"`
}

// Span is the format of trace events that Honeycomb accepts
type Span struct {
	TraceID    string    `json:"trace.trace_id"`
	Name       string    `json:"name"`
	ID         string    `json:"trace.span_id"`
	ParentID   string    `json:"trace.parent_id,omitempty"`
	DurationMs float64   `json:"duration_ms"`
	Timestamp  time.Time `json:"timestamp,omitempty"`
}

// Close waits for all in-flight messages to be sent. You should
// call Close() before app termination.
func (e *Exporter) Close() {
	libhoney.Close()
}

// NewExporter returns an implementation of trace.Exporter that uploads spans to Honeycomb
//
// writeKey is your Honeycomb writeKey (also known as your API key)
// dataset is the name of your Honeycomb dataset to send trace events to
//
// Don't have a Honeycomb account? Sign up at https://ui.honeycomb.io/signup
func NewExporter(writeKey, dataset string) *Exporter {
	// Developer note: bump this with each release
	versionStr := "1.0.1"
	libhoney.UserAgentAddition = "Honeycomb-OpenCensus-exporter/" + versionStr

	libhoney.Init(libhoney.Config{
		WriteKey: writeKey,
		Dataset:  dataset,
	})
	builder := libhoney.NewBuilder()
	// default sample reate is 1: aka no sampling.
	// set sampleRate on the exporter to be the sample rate given to the
	// ProbabilitySampler if used.
	return &Exporter{
		Builder:        builder,
		SampleFraction: 1,
		ServiceName:    "",
	}
}

// ExportSpan exports a span to Honeycomb
func (e *Exporter) ExportSpan(sd *trace.SpanData) {
	ev := e.Builder.NewEvent()
	if e.SampleFraction != 0 {
		ev.SampleRate = uint(1 / e.SampleFraction)
	}
	if e.ServiceName != "" {
		ev.AddField("service_name", e.ServiceName)
	}
	ev.Timestamp = sd.StartTime
	hs := honeycombSpan(sd)
	ev.Add(hs)

	// Add an event field for each attribute
	if len(sd.Attributes) != 0 {
		for key, value := range sd.Attributes {
			ev.AddField(key, value)
		}
	}

	// Add an event field for status code and status message
	if sd.Status.Code != 0 {
		ev.AddField("status_code", sd.Status.Code)
	}
	if sd.Status.Message != "" {
		ev.AddField("status_description", sd.Status.Message)
	}
	ev.SendPresampled()

	// Send annotations
	for _, a := range sd.Annotations {
		e.exportAnnotation(sd, &a)
	}
}

func honeycombSpan(s *trace.SpanData) Span {
	sc := s.SpanContext
	hcSpan := Span{
		TraceID:   sc.TraceID.String(),
		ID:        sc.SpanID.String(),
		Name:      s.Name,
		Timestamp: s.StartTime,
	}

	if s.ParentSpanID != (trace.SpanID{}) {
		hcSpan.ParentID = s.ParentSpanID.String()
	}

	if s, e := s.StartTime, s.EndTime; !s.IsZero() && !e.IsZero() {
		hcSpan.DurationMs = float64(e.Sub(s)) / float64(time.Millisecond)
	}

	// TODO: Re-implement MessageEvent handling as needed

	return hcSpan
}

func (e *Exporter) exportAnnotation(sd *trace.SpanData, a *trace.Annotation) {
	ev := e.Builder.NewEvent()
	if e.SampleFraction != 0 {
		ev.SampleRate = uint(1 / e.SampleFraction)
	}
	if e.ServiceName != "" {
		ev.AddField("service_name", e.ServiceName)
	}
	ev.Timestamp = a.Time

	ev.Add(Annotation{
		Name:      a.Message,
		TraceID:   sd.TraceID.String(),
		ParentID:  sd.SpanID.String(),
		Timestamp: a.Time,
	})

	// Mark the event as a trace annotation
	ev.AddField("trace.annotation", true)

	// Add an event field for each attribute
	if len(a.Attributes) != 0 {
		for key, value := range a.Attributes {
			ev.AddField(key, value)
		}
	}

	ev.SendPresampled()
}
