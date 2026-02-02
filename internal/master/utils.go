package master

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

// getClusterMasterNodes 获取集群中的所有 Master 节点地址
func getClusterMasterNodes(cluster *redis.ClusterClient) ([]string, error) {
	ctx := context.Background()
	clusterSlots, err := cluster.ClusterSlots(ctx).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to get cluster slots: %w", err)
	}

	nodesMap := make(map[string]bool)
	for _, slot := range clusterSlots {
		if len(slot.Nodes) > 0 {
			masterNode := slot.Nodes[0]
			nodesMap[masterNode.Addr] = true
		}
	}

	nodes := make([]string, 0, len(nodesMap))
	for addr := range nodesMap {
		nodes = append(nodes, addr)
	}

	return nodes, nil
}
