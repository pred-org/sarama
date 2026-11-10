package sarama

import (
	"fmt"
	"os"
	"testing"
	"time"
)

// Explicit opt-in: creates a unique disposable topic on the supplied TEST broker.
func TestIncrementalFetchIntegration(t *testing.T) {
	addr := os.Getenv("SARAMA_INCREMENTAL_TEST_BROKER")
	if addr == "" {
		t.Skip("set SARAMA_INCREMENTAL_TEST_BROKER to a disposable broker")
	}
	cfg := NewConfig()
	cfg.Version = V3_9_0_0
	cfg.Consumer.Fetch.IncrementalSessions = true
	cfg.Consumer.MaxWaitTime = 100 * time.Millisecond
	cfg.Producer.Return.Successes = true
	cfg.Producer.Partitioner = NewManualPartitioner
	admin, err := NewClusterAdmin([]string{addr}, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	topic := fmt.Sprintf("incremental-test-%d", time.Now().UnixNano())
	if err = admin.CreateTopic(topic, &TopicDetail{NumPartitions: 3, ReplicationFactor: 1}, false); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := admin.DeleteTopic(topic); err != nil {
			t.Errorf("delete test topic: %v", err)
		}
	}()
	producer, err := NewSyncProducer([]string{addr}, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()
	for p := int32(0); p < 3; p++ {
		for n := 0; n < 10; n++ {
			if _, _, err = producer.SendMessage(&ProducerMessage{Topic: topic, Partition: p, Value: StringEncoder(fmt.Sprint(n))}); err != nil {
				t.Fatal(err)
			}
		}
	}
	// Verify the actual broker accepts a full session, empty delta, pause/removal,
	// re-addition, and session reset after deliberate eviction/closure.
	b := NewBroker(addr)
	if err = b.Open(cfg); err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	bc := &brokerConsumer{consumer: &consumer{conf: cfg}, broker: b, subscriptions: make(map[*partitionConsumer]*brokerSubscription)}
	children := make([]*partitionConsumer, 3)
	for p := range children {
		c := &partitionConsumer{conf: cfg, topic: topic, partition: int32(p), offset: 10, fetchSize: 1024, leaderEpoch: -1, dying: make(chan none)}
		children[p] = c
		bc.subscriptions[c] = nil
	}
	if _, err = bc.fetchNewMessages(); err != nil || bc.fetchSession.id == 0 {
		t.Fatal("broker did not establish session", err)
	}
	t.Logf("broker established fetch session %d", bc.fetchSession.id)
	if _, err = bc.fetchNewMessages(); err != nil || bc.fetchSession.epoch != 2 {
		t.Fatal("empty delta", err)
	}
	children[1].Pause()
	if _, err = bc.fetchNewMessages(); err != nil {
		t.Fatal(err)
	}
	if _, ok := bc.fetchSession.partitions[topic][1]; ok {
		t.Fatal("paused partition still cached")
	}
	children[1].Resume()
	if _, err = bc.fetchNewMessages(); err != nil {
		t.Fatal(err)
	}
	if _, ok := bc.fetchSession.partitions[topic][1]; !ok {
		t.Fatal("resumed partition absent")
	}
	_, err = b.Fetch(&FetchRequest{Version: 11, SessionID: bc.fetchSession.id, SessionEpoch: -1})
	if err != nil {
		t.Fatal(err)
	}
	if r, err := bc.fetchNewMessages(); err != nil || r != nil || bc.fetchSession.id != 0 {
		t.Fatal("evicted session not reset", err)
	}
	if _, err = bc.fetchNewMessages(); err != nil || bc.fetchSession.id == 0 {
		t.Fatal("full recovery failed", err)
	}
	bc.closeFetchSession()
	// Public consumer path: exact payload and offset ordering across every partition.
	consumer, err := NewConsumer([]string{addr}, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.Close()
	pcs := make([]PartitionConsumer, 3)
	defer func() {
		for _, pc := range pcs {
			if pc != nil {
				_ = pc.Close()
			}
		}
	}()
	for p := range pcs {
		pcs[p], err = consumer.ConsumePartition(topic, int32(p), 0)
		if err != nil {
			t.Fatal(err)
		}
	}
	for p, pc := range pcs {
		for n := 0; n < 10; n++ {
			select {
			case m := <-pc.Messages():
				if m.Offset != int64(n) || string(m.Value) != fmt.Sprint(n) {
					t.Fatalf("partition %d got %+v want %d", p, m, n)
				}
			case e := <-pc.Errors():
				t.Fatal(e)
			case <-time.After(10 * time.Second):
				t.Fatal("delivery stalled")
			}
		}
	}
}
