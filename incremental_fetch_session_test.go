package sarama

import (
	"math"
	"testing"
	"time"
)

func sessionRequest(offsets ...int64) *FetchRequest {
	r := &FetchRequest{Version: 11}
	for p, o := range offsets {
		r.AddBlock("t", int32(p), o, 1024, 1)
	}
	return r
}
func sessionResponse(id int32, partitions ...int32) *FetchResponse {
	r := &FetchResponse{Version: 11, SessionID: id}
	for _, p := range partitions {
		r.AddError("t", p, ErrNoError)
	}
	return r
}
func TestIncrementalFetchChanges(t *testing.T) {
	var s incrementalFetchSession
	r := sessionRequest(0, 0, 0)
	s.prepare(r)
	if r.SessionID != 0 || r.SessionEpoch != 0 || len(r.blocks["t"]) != 3 {
		t.Fatal("initial full request", r)
	}
	// Encoding mutates request blocks; the session must own normalized copies.
	if _, err := encode(r, nil); err != nil {
		t.Fatal(err)
	}
	if !s.accept(r, sessionResponse(42, 0, 1, 2)) {
		t.Fatal("initial response")
	}
	r = sessionRequest(0, 0, 0)
	s.prepare(r)
	if len(r.blocks) != 0 || r.SessionID != 42 || r.SessionEpoch != 1 {
		t.Fatal("unchanged request must still be sent", r)
	}
	if !s.accept(r, sessionResponse(42)) {
		t.Fatal("empty incremental response")
	}
	r = sessionRequest(4, 0)
	s.prepare(r)
	if len(r.blocks["t"]) != 1 || r.blocks["t"][0].fetchOffset != 4 || len(r.forgotten["t"]) != 1 || r.forgotten["t"][0] != 2 {
		t.Fatal("delta/removal", r)
	}
	if !s.accept(r, sessionResponse(42, 0)) {
		t.Fatal("partial response")
	}
	r = sessionRequest(4, 0, 0)
	s.prepare(r)
	if len(r.blocks["t"]) != 1 || r.blocks["t"][2] == nil {
		t.Fatal("readded partition", r)
	}
	s.accept(r, sessionResponse(42, 2))
	r = sessionRequest(4, 0, 0)
	r.blocks["t"][1].maxBytes = 2048
	r.blocks["t"][2].currentLeaderEpoch = 2
	s.prepare(r)
	if len(r.blocks["t"]) != 2 {
		t.Fatal("size and leader epoch changes")
	}
}
func TestIncrementalFetchRecovery(t *testing.T) {
	for _, tc := range []struct {
		name     string
		response *FetchResponse
		valid    bool
		id       int32
		epoch    int32
	}{
		{"evicted", &FetchResponse{ErrorCode: int16(ErrFetchSessionIDNotFound)}, false, 0, 0},
		{"epoch", &FetchResponse{ErrorCode: int16(ErrInvalidFetchSessionEpoch)}, false, 42, 0},
		{"unknown-error", &FetchResponse{ErrorCode: int16(ErrUnknown)}, false, 42, 0},
		{"extra", sessionResponse(42, 9), false, 42, 0},
		{"wrong-session", sessionResponse(99), false, 42, 0},
		{"closed", sessionResponse(0, 0), true, 0, 0},
		{"empty", sessionResponse(42), true, 42, 2},
		{"partition-error", func() *FetchResponse {
			r := sessionResponse(42)
			r.AddError("t", 0, ErrNotLeaderForPartition)
			return r
		}(), true, 42, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var s incrementalFetchSession
			r := sessionRequest(7)
			s.prepare(r)
			s.accept(r, sessionResponse(42, 0))
			r = sessionRequest(7)
			s.prepare(r)
			if s.accept(r, tc.response) != tc.valid || s.id != tc.id || s.epoch != tc.epoch {
				t.Fatalf("state %+v", s)
			}
			if !tc.valid || s.id == 0 {
				r = sessionRequest(7)
				s.prepare(r)
				if r.blocks["t"][0].fetchOffset != 7 || r.SessionEpoch != 0 {
					t.Fatal("must retry full without advancing offset")
				}
			}
		})
	}
}
func TestIncrementalFetchFullValidationAndWrap(t *testing.T) {
	for _, response := range []*FetchResponse{sessionResponse(42), sessionResponse(42, 0, 9), {Version: 11, ThrottleTime: time.Second}} {
		var s incrementalFetchSession
		r := sessionRequest(0)
		s.prepare(r)
		if s.accept(r, response) {
			t.Fatal("accepted invalid/throttled full response")
		}
		r = sessionRequest(0)
		s.prepare(r)
		if len(r.blocks["t"]) != 1 || r.SessionEpoch != 0 {
			t.Fatal("full retry missing")
		}
	}
	var s incrementalFetchSession
	r := sessionRequest(0)
	s.prepare(r)
	if !s.accept(r, sessionResponse(0, 0)) || s.id != 0 || s.epoch != 0 {
		t.Fatal("broker refused session")
	}
	s.accept(r, sessionResponse(42, 0))
	s.epoch = math.MaxInt32
	r = sessionRequest(0)
	s.prepare(r)
	if !s.accept(r, sessionResponse(42)) || s.epoch != 1 {
		t.Fatal("epoch overflow")
	}
	r = sessionRequest(0)
	r.Version = 7
	s.prepare(r)
	if r.SessionEpoch != 0 || r.SessionID != 42 || len(r.blocks["t"]) != 1 {
		t.Fatal("protocol change must rebuild")
	}
}

func TestIncrementalFetchBrokerLoop(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		t.Run(map[bool]string{false: "legacy", true: "enabled"}[enabled], func(t *testing.T) {
			cfg := NewTestConfig()
			cfg.Version = V2_3_0_0
			cfg.Consumer.Fetch.IncrementalSessions = enabled
			b := NewMockBroker(t, 0)
			defer b.Close()
			broker := NewBroker(b.Addr())
			if err := broker.Open(cfg); err != nil {
				t.Fatal(err)
			}
			defer broker.Close()
			first := sessionResponse(42, 0, 1)
			empty := sessionResponse(42)
			changed := sessionResponse(42, 0)
			b.SetHandlerByMap(map[string]MockResponse{"FetchRequest": NewMockSequence(first, empty, changed, sessionResponse(0))})
			child0 := &partitionConsumer{conf: cfg, topic: "t", partition: 0, offset: 0, fetchSize: 1024, leaderEpoch: 1, dying: make(chan none)}
			child1 := &partitionConsumer{conf: cfg, topic: "t", partition: 1, offset: 0, fetchSize: 1024, leaderEpoch: 1, dying: make(chan none)}
			bc := &brokerConsumer{consumer: &consumer{conf: cfg}, broker: broker, subscriptions: map[*partitionConsumer]*brokerSubscription{child0: nil, child1: nil}}
			for i := 0; i < 3; i++ {
				if i == 2 {
					child0.offset = 5
					child1.Pause()
				}
				if _, err := bc.fetchNewMessages(); err != nil {
					t.Fatal(err)
				}
			}
			if enabled {
				bc.closeFetchSession()
			}
			history := b.History()
			r0 := history[0].Request.(*FetchRequest)
			r1 := history[1].Request.(*FetchRequest)
			r2 := history[2].Request.(*FetchRequest)
			if enabled {
				if r0.SessionEpoch != 0 || r1.SessionEpoch != 1 || r1.SessionID != 42 || len(r1.blocks) != 0 {
					t.Fatal("session not used on wire")
				}
				if len(r2.blocks["t"]) != 1 || len(r2.forgotten["t"]) != 1 || r2.forgotten["t"][0] != 1 {
					t.Fatal("pause not forgotten on wire")
				}
				last := history[3].Request.(*FetchRequest)
				if last.SessionID != 42 || last.SessionEpoch != -1 {
					t.Fatal("session not closed")
				}
			} else {
				for _, r := range []*FetchRequest{r0, r1, r2} {
					if r.SessionEpoch != -1 || r.SessionID != 0 {
						t.Fatal("legacy changed")
					}
				}
			}
		})
	}
}

func TestIncrementalFetchOldBrokerAndAllPaused(t *testing.T) {
	for _, version := range []KafkaVersion{V1_0_0_0, V2_3_0_0} {
		cfg := NewTestConfig()
		cfg.Version = version
		cfg.Consumer.Fetch.IncrementalSessions = true
		b := NewMockBroker(t, 0)
		broker := NewBroker(b.Addr())
		if err := broker.Open(cfg); err != nil {
			t.Fatal(err)
		}
		response := sessionResponse(42, 0)
		if version == V1_0_0_0 {
			response.Version = 6
			response.SessionID = 0
		}
		b.SetHandlerByMap(map[string]MockResponse{"FetchRequest": NewMockSequence(response, sessionResponse(0))})
		child := &partitionConsumer{conf: cfg, topic: "t", dying: make(chan none)}
		bc := &brokerConsumer{consumer: &consumer{conf: cfg}, broker: broker, subscriptions: map[*partitionConsumer]*brokerSubscription{child: nil}}
		if _, err := bc.fetchNewMessages(); err != nil {
			t.Fatal(err)
		}
		child.Pause()
		if r, err := bc.fetchNewMessages(); r != nil || err != nil {
			t.Fatal("pause result", err)
		}
		count := len(b.History())
		if _, err := bc.fetchNewMessages(); err != nil {
			t.Fatal(err)
		}
		if len(b.History()) != count || bc.fetchSession.id != 0 {
			t.Fatal("paused loop repeatedly fetched")
		}
		if version == V1_0_0_0 && count != 1 {
			t.Fatal("old broker session request")
		}
		broker.Close()
		b.Close()
	}
}
