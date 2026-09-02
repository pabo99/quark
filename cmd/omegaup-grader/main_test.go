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

func TestQueueEventsProcessorObservesTimeToVerdictOnManagerRemoved(t *testing.T) {
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

	value, ok := metrics.findSummaryObservation("grader_time_to_verdict_seconds")
	if !ok {
		t.Fatal("missing summary observation for grader_time_to_verdict_seconds")
	}
	if value != 2 {
		t.Errorf("summary observation for grader_time_to_verdict_seconds = %f, want 2", value)
	}
	if _, observed := metrics.findSummaryObservation("grader_queue_delay_seconds"); observed {
		t.Fatal("grader_queue_delay_seconds must not be observed on ManagerRemoved")
	}
}

func TestQueueEventsProcessorObservesQueueDelayOnQueueRemoved(t *testing.T) {
	metrics := &recordingMetrics{}
	globalContext.Store(&grader.Context{
		Context: common.Context{
			Metrics: metrics,
		},
	})

	events := make(chan *grader.QueueEvent, 1)
	events <- &grader.QueueEvent{
		Type:     grader.QueueEventTypeQueueRemoved,
		Priority: grader.QueuePriorityNormal,
		Delta:    2 * time.Second,
	}
	close(events)

	queueEventsProcessor(events)

	value, ok := metrics.findSummaryObservation("grader_queue_delay_seconds")
	if !ok {
		t.Fatal("missing summary observation for grader_queue_delay_seconds")
	}
	if value != 2 {
		t.Errorf("summary observation for grader_queue_delay_seconds = %f, want 2", value)
	}
	if _, observed := metrics.findSummaryObservation("grader_time_to_verdict_seconds"); observed {
		t.Fatal("grader_time_to_verdict_seconds must not be observed on QueueRemoved")
	}
}
