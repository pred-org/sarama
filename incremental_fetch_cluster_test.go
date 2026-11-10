package sarama

import (
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

// Opt-in local fault test. Launches three disposable Kafka processes and never
// connects to an externally supplied cluster. Requires Kafka 3.9.1 and a JDK.
func TestIncrementalFetchClusterRecovery(t *testing.T) {
	home := os.Getenv("SARAMA_INCREMENTAL_KAFKA_HOME")
	javaHome := os.Getenv("SARAMA_INCREMENTAL_JAVA_HOME")
	if home == "" || javaHome == "" {
		t.Skip("set SARAMA_INCREMENTAL_KAFKA_HOME and SARAMA_INCREMENTAL_JAVA_HOME")
	}
	root := t.TempDir()
	ports := make([]int, 6)
	for i := range ports {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		ports[i] = l.Addr().(*net.TCPAddr).Port
		_ = l.Close()
	}
	var voters []string
	var addrs []string
	for i := 0; i < 3; i++ {
		voters = append(voters, fmt.Sprintf("%d@127.0.0.1:%d", i+1, ports[2*i+1]))
		addrs = append(addrs, fmt.Sprintf("127.0.0.1:%d", ports[2*i]))
	}
	env := append(os.Environ(), "JAVA_HOME="+javaHome, "KAFKA_HEAP_OPTS=-Xms128m -Xmx512m")
	configs := make([]string, 3)
	processes := make([]*exec.Cmd, 3)
	files := make([]*os.File, 3)
	stop := func(i int) {
		if processes[i] != nil {
			_ = processes[i].Process.Kill()
			_ = processes[i].Wait()
			processes[i] = nil
		}
		if files[i] != nil {
			_ = files[i].Close()
			files[i] = nil
		}
	}
	t.Cleanup(func() {
		if t.Failed() {
			for i := range processes {
				data, _ := os.ReadFile(filepath.Join(root, fmt.Sprintf("broker-%d.log", i+1)))
				lines := strings.Split(string(data), "\n")
				if len(lines) > 80 {
					lines = lines[len(lines)-80:]
				}
				t.Logf("broker %d final log:\n%s", i+1, strings.Join(lines, "\n"))
			}
		}
		for i := range processes {
			stop(i)
		}
	})
	start := func(i int) {
		f, err := os.OpenFile(filepath.Join(root, fmt.Sprintf("broker-%d.log", i+1)), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
		if err != nil {
			t.Fatal(err)
		}
		cmd := exec.Command(filepath.Join(home, "bin", "kafka-server-start.sh"), configs[i]) //nolint:gosec // Explicit opt-in test installation; no shell or external input.
		cmd.Env = append(append([]string{}, env...), "LOG_DIR="+filepath.Join(root, fmt.Sprintf("logs-%d", i+1)))
		cmd.Stdout = f
		cmd.Stderr = f
		if err = cmd.Start(); err != nil {
			_ = f.Close()
			t.Fatal(err)
		}
		processes[i] = cmd
		files[i] = f
	}
	for i := range configs {
		configs[i] = filepath.Join(root, fmt.Sprintf("broker-%d.properties", i+1))
		content := fmt.Sprintf(`process.roles=broker,controller
node.id=%d
controller.quorum.voters=%s
listeners=PLAINTEXT://127.0.0.1:%d,CONTROLLER://127.0.0.1:%d
advertised.listeners=PLAINTEXT://127.0.0.1:%d
listener.security.protocol.map=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT
controller.listener.names=CONTROLLER
inter.broker.listener.name=PLAINTEXT
log.dirs=%s
num.partitions=3
log.cleaner.dedupe.buffer.size=16777216
default.replication.factor=3
min.insync.replicas=2
offsets.topic.replication.factor=3
transaction.state.log.replication.factor=3
transaction.state.log.min.isr=2
group.initial.rebalance.delay.ms=0
`, i+1, strings.Join(voters, ","), ports[2*i], ports[2*i+1], ports[2*i], filepath.Join(root, fmt.Sprintf("data-%d", i+1)))
		if err := os.WriteFile(configs[i], []byte(content), 0600); err != nil {
			t.Fatal(err)
		}
		cmd := exec.Command(filepath.Join(home, "bin", "kafka-storage.sh"), "format", "-t", "Uf1nOV7KSPmYbq3LqkYW_A", "-c", configs[i]) //nolint:gosec // Explicit opt-in test installation; generated temporary config.
		cmd.Env = env
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("format: %v %s", err, out)
		}
	}
	for i := range configs {
		start(i)
	}
	cfg := NewConfig()
	cfg.Version = V3_9_0_0
	cfg.Consumer.Fetch.IncrementalSessions = true
	cfg.Consumer.Return.Errors = true
	cfg.Net.DialTimeout = 2 * time.Second
	cfg.Net.ReadTimeout = 10 * time.Second
	cfg.Net.WriteTimeout = 10 * time.Second
	cfg.Metadata.Retry.Max = 2
	cfg.Metadata.Retry.Backoff = 100 * time.Millisecond
	cfg.Consumer.Retry.Backoff = 100 * time.Millisecond
	cfg.Producer.Return.Successes = true
	cfg.Producer.Idempotent = true
	cfg.Net.MaxOpenRequests = 1
	cfg.Producer.Partitioner = NewManualPartitioner
	cfg.Producer.RequiredAcks = WaitForAll
	cfg.Producer.Retry.Max = 20
	var admin ClusterAdmin
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		a, err := NewClusterAdmin(addrs, cfg)
		if err == nil {
			admin = a
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if admin == nil {
		t.Fatalf("cluster failed to start; logs in %s", root)
	}
	defer admin.Close()
	deadline = time.Now().Add(60 * time.Second)
	for {
		brokers, _, err := admin.DescribeCluster()
		if err == nil && len(brokers) == 3 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("all brokers did not register")
		}
		time.Sleep(200 * time.Millisecond)
	}
	topic := "incremental-cluster-recovery"
	assignment := map[int32][]int32{0: {1, 2, 3}, 1: {2, 3, 1}, 2: {3, 1, 2}}
	if err := admin.CreateTopic(topic, &TopicDetail{NumPartitions: -1, ReplicationFactor: -1, ReplicaAssignment: assignment}, false); err != nil {
		t.Fatal(err)
	}
	waitMetadata := func(label string, check func([]*PartitionMetadata) bool) {
		deadline := time.Now().Add(60 * time.Second)
		for time.Now().Before(deadline) {
			ready := true
			// Controller metadata can precede application by the data brokers.
			// Wait for every running broker to agree, without refreshing the
			// consumer under test or changing its offsets/session state.
			for i, addr := range addrs {
				if processes[i] == nil {
					continue
				}
				b := NewBroker(addr)
				err := b.Open(cfg)
				var m *MetadataResponse
				if err == nil {
					m, err = b.GetMetadata(NewMetadataRequest(cfg.Version, []string{topic}))
				}
				_ = b.Close()
				if err != nil || len(m.Topics) != 1 || len(m.Topics[0].Partitions) != 3 || !check(m.Topics[0].Partitions) {
					ready = false
					break
				}
			}
			if ready {
				return
			}
			time.Sleep(200 * time.Millisecond)
		}
		t.Fatal("timed out: " + label)
	}
	healthy := func(ps []*PartitionMetadata) bool {
		for _, p := range ps {
			if p.Err != ErrNoError || len(p.Isr) != 3 {
				return false
			}
		}
		return true
	}
	waitMetadata("initial ISR", healthy)
	producer, err := NewSyncProducer(addrs, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()
	consumer, err := NewConsumer(addrs, cfg)
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
	for i := range pcs {
		deadline := time.Now().Add(10 * time.Second)
		for {
			pcs[i], err = consumer.ConsumePartition(topic, int32(i), 0)
			if err == nil {
				break
			}
			if !errors.Is(err, ErrUnknownTopicOrPartition) || time.Now().After(deadline) {
				t.Fatal(err)
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
	deliver := func(from, to int) {
		for p := 0; p < 3; p++ {
			for n := from; n < to; n++ {
				if _, offset, err := producer.SendMessage(&ProducerMessage{Topic: topic, Partition: int32(p), Value: StringEncoder(strconv.Itoa(n))}); err != nil || offset != int64(n) {
					t.Fatalf("produce partition %d offset %d: %v", p, offset, err)
				}
			}
		}
		for p, pc := range pcs {
			for n := from; n < to; n++ {
				timer := time.NewTimer(30 * time.Second)
				received := false
				for !received {
					select {
					case m := <-pc.Messages():
						if m == nil || m.Offset != int64(n) || string(m.Value) != strconv.Itoa(n) {
							t.Fatalf("partition %d expected %d got %+v", p, n, m)
						}
						received = true
					case e := <-pc.Errors():
						t.Logf("transient failover error: %v", e)
					case <-timer.C:
						t.Fatalf("partition %d stalled at %d", p, n)
					}
				}
				timer.Stop()
			}
		}
	}
	deliver(0, 10)
	for i := 0; i < 3; i++ {
		id := int32(i + 1)
		stop(i)
		waitMetadata(fmt.Sprintf("leader moved away from %d", id), func(ps []*PartitionMetadata) bool {
			for _, p := range ps {
				if p.Err != ErrNoError || p.Leader == id || p.Leader < 1 || len(p.Isr) < 2 {
					return false
				}
			}
			return true
		})
		deliver(10+i*20, 20+i*20)
		t.Logf("broker %d killed: all partitions delivered while one replica was unavailable", id)
		start(i)
		waitMetadata("replicas recovered", healthy)
		if _, err := admin.(*clusterAdmin).refreshController(); err != nil {
			t.Fatal(err)
		}
		if _, err := admin.ElectLeaders(ElectionType(0), map[string][]int32{topic: {0, 1, 2}}); err != nil {
			t.Fatal(err)
		}
		waitMetadata("preferred leaders restored", func(ps []*PartitionMetadata) bool {
			for _, p := range ps {
				if p.Leader != assignment[p.ID][0] {
					return false
				}
			}
			return healthy(ps)
		})
		deliver(20+i*20, 30+i*20)
		t.Logf("broker %d restarted: ISR recovered and preferred leaders restored", id)
	}
}
