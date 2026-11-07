package sarama

import "math"

// State belongs to one brokerConsumer, not the shared broker connection or group.
// Offset commits and message processing are deliberately outside this state.
type incrementalFetchSession struct {
	id, epoch  int32
	version    int16
	partitions map[string]map[int32]fetchRequestBlock
}

func (s *incrementalFetchSession) prepare(r *FetchRequest) {
	if s.version != r.Version {
		s.epoch = 0 // close any old session and establish a full one
	}
	r.SessionID, r.SessionEpoch = s.id, s.epoch
	next := make(map[string]map[int32]fetchRequestBlock, len(r.blocks))
	for topic, blocks := range r.blocks {
		next[topic] = make(map[int32]fetchRequestBlock, len(blocks))
		for partition, block := range blocks {
			value := *block
			value.Version = r.Version // encode mutates Version; compare normalized copies
			next[topic][partition] = value
			if old, exists := s.partitions[topic][partition]; s.epoch > 0 && exists && old == value {
				delete(blocks, partition)
			}
		}
		if len(blocks) == 0 {
			delete(r.blocks, topic)
		}
	}
	if s.epoch > 0 {
		for topic, blocks := range s.partitions {
			for partition := range blocks {
				if _, exists := next[topic][partition]; !exists {
					if r.forgotten == nil {
						r.forgotten = make(map[string][]int32)
					}
					r.forgotten[topic] = append(r.forgotten[topic], partition)
				}
			}
		}
	}
	s.partitions, s.version = next, r.Version
}

// Invalid responses are discarded in their entirety: no offset may advance.
func (s *incrementalFetchSession) accept(r *FetchRequest, response *FetchResponse) bool {
	if response.ErrorCode != 0 {
		if KError(response.ErrorCode) == ErrFetchSessionIDNotFound {
			s.id = 0
		}
		s.epoch = 0
		return false
	}
	if r.SessionEpoch == 0 && len(response.Blocks) == 0 && response.ThrottleTime > 0 {
		s.id, s.epoch = 0, 0
		return false
	}
	valid := true
	for topic, blocks := range response.Blocks {
		for partition := range blocks {
			if _, exists := s.partitions[topic][partition]; !exists {
				valid = false
			}
		}
	}
	if r.SessionEpoch == 0 {
		for topic, blocks := range s.partitions {
			for partition := range blocks {
				if response.Blocks[topic][partition] == nil {
					valid = false
				}
			}
		}
	} else if response.SessionID != 0 && response.SessionID != s.id {
		valid = false
	}
	if !valid {
		if r.SessionEpoch == 0 {
			s.id = response.SessionID
		}
		s.epoch = 0
		return false
	}
	s.id = response.SessionID
	if s.id == 0 {
		s.epoch = 0
	} else if s.epoch == math.MaxInt32 {
		s.epoch = 1
	} else {
		s.epoch++
	}
	return true
}

// Called only by the fetch loop, including when all partitions are paused.
// Closing is best effort and cannot mark or commit records.
func (bc *brokerConsumer) closeFetchSession() {
	s := &bc.fetchSession
	if s.id != 0 {
		_, _ = bc.broker.Fetch(&FetchRequest{Version: s.version, SessionID: s.id, SessionEpoch: -1})
	}
	*s = incrementalFetchSession{}
}
