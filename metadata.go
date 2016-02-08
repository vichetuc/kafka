package kafka

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/optiopay/kafka/proto"
)

type clusterMetadata struct {
	conns *connectionPool
	conf  BrokerConf

	// mu protects the contents of this structure, but should only be gotten/used
	// by the clusterMetadata methods.
	mu         *sync.RWMutex
	created    time.Time
	nodes      nodeMap                  // node ID to address
	endpoints  map[topicPartition]int32 // partition to leader node ID
	partitions map[string]int32         // topic to numer of partitions
}

// cache creates new internal metadata representation using data from
// given response.
//
// Do not call with partial metadata response, this assumes we have the full
// set of metadata in the response!
func (cm *clusterMetadata) cache(resp *proto.MetadataResp) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if !cm.created.IsZero() {
		log.Debug("rewriting old metadata",
			"age", time.Now().Sub(cm.created))
	}

	cm.created = time.Now()
	cm.nodes = make(nodeMap)
	cm.endpoints = make(map[topicPartition]int32)
	cm.partitions = make(map[string]int32)

	addrs := make([]string, 0)
	debugmsg := make([]interface{}, 0)
	for _, node := range resp.Brokers {
		addr := fmt.Sprintf("%s:%d", node.Host, node.Port)
		addrs = append(addrs, addr)
		cm.nodes[node.NodeID] = addr
		debugmsg = append(debugmsg, node.NodeID, addr)
	}
	for _, topic := range resp.Topics {
		for _, part := range topic.Partitions {
			dest := topicPartition{topic.Name, part.ID}
			cm.endpoints[dest] = part.Leader
			debugmsg = append(debugmsg, dest, part.Leader)
		}
		cm.partitions[topic.Name] = int32(len(topic.Partitions))
	}
	cm.conns.InitializeAddrs(addrs)
	log.Debug("new metadata cached", debugmsg...)
}

// Refresh is requesting metadata information from any node and refresh
// internal cached representation.
func (cm *clusterMetadata) Refresh() error {
	log.Info("refreshing metadata")
	meta, err := cm.Fetch()
	if err == nil {
		cm.cache(meta)
	}
	return err
}

// Fetch is requesting metadata information from any node and return
// protocol response if successful. This will attempt to talk to every node at
// least once until one returns a successful response. We walk the nodes in
// a random order.
//
// If "topics" are specified, only fetch metadata for those topics (can be
// used to create a topic)
func (cm *clusterMetadata) Fetch(topics ...string) (*proto.MetadataResp, error) {
	// Get all addresses, then walk the array in permuted random order.
	addrs := cm.conns.GetAllAddrs()
	log.Info("addrs", "addrs", addrs)
	for _, idx := range rndPerm(len(addrs)) {
		conn, err := cm.conns.GetConnectionByAddr(addrs[idx])
		if err != nil {
			continue
		}
		defer func() { go cm.conns.Idle(conn) }()

		resp, err := conn.Metadata(&proto.MetadataReq{
			ClientID: cm.conf.ClientID,
			Topics:   topics,
		})
		if err != nil {
			log.Debug("cannot fetch metadata from node",
				"addr", addrs[idx],
				"error", err)
			conn.Close()
			continue
		}
		return resp, nil
	}

	return nil, errors.New("cannot fetch metadata")
}

// PartitionCount returns how many partitions a given topic has. If a topic
// is not known, 0 and an error are returned.
func (cm *clusterMetadata) PartitionCount(topic string) (int32, error) {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	if count, ok := cm.partitions[topic]; ok {
		return count, nil
	}
	return 0, fmt.Errorf("topic %s not found in metadata", topic)
}

// GetEndpoint returns a nodeID for a topic/partition. Returns an error if
// the topic/partition is unknown.
func (cm *clusterMetadata) GetEndpoint(tp topicPartition) (int32, error) {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	if nodeID, ok := cm.endpoints[tp]; ok {
		return nodeID, nil
	}
	return 0, errors.New("topic/partition not found in metadata")
}

// ForgetEndpoint is used to remove an endpoint that doesn't see to lead to
// a valid location.
func (cm *clusterMetadata) ForgetEndpoint(tp topicPartition) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	delete(cm.endpoints, tp)
}

// GetNodes returns a map of nodes that exist in the cluster.
func (cm *clusterMetadata) GetNodes() nodeMap {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	nodes := make(nodeMap)
	for nodeID, addr := range cm.nodes {
		nodes[nodeID] = addr
	}
	return nodes
}

// GetNodeAddress returns the address to a node if we know it.
func (cm *clusterMetadata) GetNodeAddress(nodeID int32) string {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	addr, _ := cm.nodes[nodeID]
	return addr
}
