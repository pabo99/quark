package main

import (
	"testing"
	"time"

	"github.com/omegaup/quark/common"
	"github.com/omegaup/quark/grader"
)

type recordedMetric struct {
	name  string
	value float64
}

type recordingMetrics struct {
	gaugeAdds           []recordedMetric
	counterAdds         []recordedMetric
	summaryObservations []recordedMetric
}

func (m *recordingMetrics) GaugeAdd(name string, value float64) {
	m.gaugeAdds = append(m.gaugeAdds, recordedMetric{name: name, value: value})
}

func (m *recordingMetrics) CounterAdd(name string, value float64) {
	m.counterAdds = append(m.counterAdds, recordedMetric{name: name, value: value})
}

func (m *recordingMetrics) SummaryObserve(name string, value float64) {
	m.summaryObservations = append(m.summaryObservations, recordedMetric{name: name, value: value})
}

func (m *recordingMetrics) findSummaryObservation(name string) (float64, bool) {
	for _, observation := range m.summaryObservations {
		if observation.name == name {
			return observation.value, true
		}
	}
	return 0, false
}

func TestQueueEventsProcessorObservesTimeToVerdict(t *testing.T) {
	metrics := &recordingMetrics{}
	globalContext.Store(&grader.Context{
		Context: common.Context{
			Metrics: metrics,
		},
	})

	events := make(chan *grader.QueueEvent, 1)
	events <- &grader.QueueEvent{
		Type:  grader.QueueEventTypeManagerRemoved,
		Delta: 2 * time.Second,
	}
	close(events)

	queueEventsProcessor(events)

	for _, name := range []string{
		"grader_queue_delay_seconds",
		"grader_time_to_verdict_seconds",
	} {
		value, ok := metrics.findSummaryObservation(name)
		if !ok {
			t.Errorf("missing summary observation for %s", name)
			continue
		}
		if value != 2 {
			t.Errorf("summary observation for %s = %f, want 2", name, value)
		}
	}
}
