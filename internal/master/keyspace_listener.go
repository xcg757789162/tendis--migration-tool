package master

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"tendis-migrate/internal/storage"
	"tendis-migrate/pkg/logger"
)

// KeyspaceListener Keyspace Notifications 监听器
type KeyspaceListener struct {
	taskID         string
	sourceCluster  *redis.ClusterClient
	queues         map[string]*storage.LevelDBQueue // nodeAddr -> queue
	logger         *logger.Logger
	ctx            context.Context
	cancel         context.CancelFunc
	wg             sync.WaitGroup
	running        bool
	mutex          sync.RWMutex
	eventsReceived int64
	lastEventTime  time.Time
}

// NewKeyspaceListener 创建 Keyspace Notifications 监听器
func NewKeyspaceListener(
	taskID string,
	sourceCluster *redis.ClusterClient,
	queues map[string]*storage.LevelDBQueue,
	taskLogger *logger.Logger,
) *KeyspaceListener {
	ctx, cancel := context.WithCancel(context.Background())
	return &KeyspaceListener{
		taskID:        taskID,
		sourceCluster: sourceCluster,
		queues:        queues,
		logger:        taskLogger,
		ctx:           ctx,
		cancel:        cancel,
		running:       false,
	}
}

// Start 启动监听所有源节点
func (kl *KeyspaceListener) Start() error {
	kl.mutex.Lock()
	if kl.running {
		kl.mutex.Unlock()
		return fmt.Errorf("keyspace listener already running")
	}
	kl.running = true
	kl.mutex.Unlock()

	kl.logger.Info("Starting Keyspace Notifications listener", nil)

	// 获取所有 Master 节点
	nodes, err := kl.getClusterMasterNodes()
	if err != nil {
		return fmt.Errorf("get cluster nodes failed: %w", err)
	}

	kl.logger.Info("Found cluster master nodes", map[string]interface{}{
		"count": len(nodes),
		"nodes": nodes,
	})

	// 为每个节点启动监听 goroutine
	for _, nodeAddr := range nodes {
		kl.wg.Add(1)
		go kl.listenNode(nodeAddr)
	}

	return nil
}

// Stop 停止监听
func (kl *KeyspaceListener) Stop() {
	kl.mutex.Lock()
	if !kl.running {
		kl.mutex.Unlock()
		return
	}
	kl.running = false
	kl.mutex.Unlock()

	kl.logger.Info("Stopping Keyspace Notifications listener", nil)
	kl.cancel()
	kl.wg.Wait()
	kl.logger.Info("Keyspace Notifications listener stopped", nil)
}

// GetStats 获取监听统计
func (kl *KeyspaceListener) GetStats() map[string]interface{} {
	kl.mutex.RLock()
	defer kl.mutex.RUnlock()

	return map[string]interface{}{
		"events_received": kl.eventsReceived,
		"last_event_time": kl.lastEventTime,
		"running":         kl.running,
	}
}

// getClusterMasterNodes 获取集群中的所有 Master 节点地址
func (kl *KeyspaceListener) getClusterMasterNodes() ([]string, error) {
	clusterSlots, err := kl.sourceCluster.ClusterSlots(kl.ctx).Result()
	if err != nil {
		return nil, err
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

// listenNode 监听单个节点的 keyspace 事件
func (kl *KeyspaceListener) listenNode(nodeAddr string) {
	defer kl.wg.Done()

	kl.logger.Info("Starting keyspace listener for node", map[string]interface{}{
		"node": nodeAddr,
	})

	// 创建专用连接（非 Cluster 模式）
	nodeClient := redis.NewClient(&redis.Options{
		Addr:         nodeAddr,
		DialTimeout:  5 * time.Second,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 5 * time.Second,
	})
	defer nodeClient.Close()

	// 检查连接
	if err := nodeClient.Ping(kl.ctx).Err(); err != nil {
		kl.logger.Error("Failed to connect to node", map[string]interface{}{
			"node":  nodeAddr,
			"error": err.Error(),
		})
		return
	}

	// 注意：不再自动修改源端 notify-keyspace-events 配置
	// 如果需要 keyspace 通知，请在迁移前手动配置: CONFIG SET notify-keyspace-events KEA
	// 检查当前配置
	if vals, err := nodeClient.ConfigGet(kl.ctx, "notify-keyspace-events").Result(); err == nil {
		if val, ok := vals["notify-keyspace-events"]; ok && val != "" {
			kl.logger.Info("notify-keyspace-events is configured", map[string]interface{}{
				"node":  nodeAddr,
				"value": val,
			})
		} else {
			kl.logger.Warn("notify-keyspace-events is not configured, keyspace events may not work", map[string]interface{}{
				"node": nodeAddr,
			})
			kl.logger.Warn("Please configure it manually: CONFIG SET notify-keyspace-events KEA", nil)
		}
	}

	// 订阅所有 keyspace 事件
	// Pattern: __keyspace@0__:* (监听所有 DB0 的 key 事件)
	pubsub := nodeClient.PSubscribe(kl.ctx, "__keyspace@0__:*", "__keyevent@0__:*")
	defer pubsub.Close()

	kl.logger.Info("Subscribed to keyspace events", map[string]interface{}{
		"node": nodeAddr,
	})

	// 获取对应的队列
	queue, exists := kl.queues[nodeAddr]
	if !exists {
		kl.logger.Error("Queue not found for node", map[string]interface{}{
			"node": nodeAddr,
		})
		return
	}

	// 接收消息
	ch := pubsub.Channel()
	for {
		select {
		case <-kl.ctx.Done():
			kl.logger.Info("Keyspace listener stopped for node", map[string]interface{}{
				"node": nodeAddr,
			})
			return

		case msg, ok := <-ch:
			if !ok {
				kl.logger.Warn("Keyspace channel closed for node", map[string]interface{}{
					"node": nodeAddr,
				})
				return
			}

			// 解析 keyspace 事件
			key, eventType := kl.parseKeyspaceEvent(msg.Channel, msg.Payload)
			if key == "" {
				continue
			}

			// 写入队列
			change := &storage.ChangeRecord{
				Key:       key,
				Operation: eventType,
				Timestamp: time.Now().Unix(),
				NodeID:    nodeAddr,
			}

			if err := queue.Enqueue(change); err != nil {
				kl.logger.Warn("Failed to enqueue change", map[string]interface{}{
					"node":  nodeAddr,
					"key":   key,
					"event": eventType,
					"error": err.Error(),
				})
			} else {
				kl.mutex.Lock()
				kl.eventsReceived++
				kl.lastEventTime = time.Now()
				kl.mutex.Unlock()
			}
		}
	}
}

// parseKeyspaceEvent 解析 keyspace 事件
// Channel 格式: __keyspace@0__:mykey 或 __keyevent@0__:set
// Payload: 事件类型 (set, del, expire, etc.)
func (kl *KeyspaceListener) parseKeyspaceEvent(channel, payload string) (key, eventType string) {
	// __keyspace@0__:mykey -> key=mykey, event=payload
	// __keyevent@0__:set -> key=payload, event=set
	if strings.HasPrefix(channel, "__keyspace@") {
		parts := strings.SplitN(channel, ":", 2)
		if len(parts) == 2 {
			return parts[1], payload
		}
	} else if strings.HasPrefix(channel, "__keyevent@") {
		parts := strings.SplitN(channel, ":", 2)
		if len(parts) == 2 {
			return payload, parts[1]
		}
	}
	return "", ""
}
