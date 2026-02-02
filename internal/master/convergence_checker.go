package master

import (
	"context"
	"fmt"
	"sync"
	"time"

	"tendis-migrate/internal/storage"
	"tendis-migrate/pkg/logger"
)

// ConvergenceChecker 收敛检测器
type ConvergenceChecker struct {
	taskID       string
	queues       map[string]*storage.LevelDBQueue
	listener     *KeyspaceListener
	logger       *logger.Logger
	ctx          context.Context
	cancel       context.CancelFunc
	wg           sync.WaitGroup
	running      bool
	mutex        sync.RWMutex
	
	// 收敛参数
	checkInterval    time.Duration // 检查间隔（默认 5s）
	stableWindow     time.Duration // 稳定窗口（默认 30s）
	maxQueueSize     int64         // 最大队列大小阈值（默认 100）
	
	// 状态
	converged        bool
	convergedAt      time.Time
	lastCheckTime    time.Time
	stableStartTime  time.Time
}

// NewConvergenceChecker 创建收敛检测器
func NewConvergenceChecker(
	taskID string,
	queues map[string]*storage.LevelDBQueue,
	listener *KeyspaceListener,
	taskLogger *logger.Logger,
) *ConvergenceChecker {
	ctx, cancel := context.WithCancel(context.Background())
	return &ConvergenceChecker{
		taskID:           taskID,
		queues:           queues,
		listener:         listener,
		logger:           taskLogger,
		ctx:              ctx,
		cancel:           cancel,
		checkInterval:    5 * time.Second,
		stableWindow:     30 * time.Second,
		maxQueueSize:     100,
	}
}

// Start 启动收敛检测
func (cc *ConvergenceChecker) Start() error {
	cc.mutex.Lock()
	if cc.running {
		cc.mutex.Unlock()
		return fmt.Errorf("convergence checker already running")
	}
	cc.running = true
	cc.mutex.Unlock()

	cc.logger.Info("Starting convergence checker", map[string]interface{}{
		"check_interval": cc.checkInterval,
		"stable_window":  cc.stableWindow,
		"max_queue_size": cc.maxQueueSize,
	})

	cc.wg.Add(1)
	go cc.checkLoop()

	return nil
}

// Stop 停止收敛检测
func (cc *ConvergenceChecker) Stop() {
	cc.mutex.Lock()
	if !cc.running {
		cc.mutex.Unlock()
		return
	}
	cc.running = false
	cc.mutex.Unlock()

	cc.logger.Info("Stopping convergence checker", nil)
	cc.cancel()
	cc.wg.Wait()
	cc.logger.Info("Convergence checker stopped", nil)
}

// IsConverged 是否已收敛
func (cc *ConvergenceChecker) IsConverged() bool {
	cc.mutex.RLock()
	defer cc.mutex.RUnlock()
	return cc.converged
}

// GetStats 获取统计信息
func (cc *ConvergenceChecker) GetStats() map[string]interface{} {
	cc.mutex.RLock()
	defer cc.mutex.RUnlock()

	return map[string]interface{}{
		"converged":          cc.converged,
		"converged_at":       cc.convergedAt,
		"last_check_time":    cc.lastCheckTime,
		"stable_start_time":  cc.stableStartTime,
	}
}

// checkLoop 检测循环
func (cc *ConvergenceChecker) checkLoop() {
	defer cc.wg.Done()

	ticker := time.NewTicker(cc.checkInterval)
	defer ticker.Stop()

	for {
		select {
		case <-cc.ctx.Done():
			return

		case <-ticker.C:
			cc.performCheck()
		}
	}
}

// performCheck 执行一次检测
func (cc *ConvergenceChecker) performCheck() {
	cc.mutex.Lock()
	cc.lastCheckTime = time.Now()
	cc.mutex.Unlock()

	// 1. 检查所有队列大小
	totalQueueSize := int64(0)
	for nodeAddr, queue := range cc.queues {
		stats := queue.GetStats()
		size, _ := stats["size"].(int64)
		totalQueueSize += size

		cc.logger.Debug("Queue stats", map[string]interface{}{
			"node": nodeAddr,
			"size": size,
		})
	}

	// 2. 检查 Keyspace Listener 是否有新事件
	listenerStats := cc.listener.GetStats()
	lastEventTime, ok := listenerStats["last_event_time"].(time.Time)
	if !ok {
		lastEventTime = time.Time{}
	}

	// 3. 判断是否收敛
	isStable := totalQueueSize <= cc.maxQueueSize && 
		(lastEventTime.IsZero() || time.Since(lastEventTime) > cc.checkInterval)

	cc.mutex.Lock()
	defer cc.mutex.Unlock()

	if isStable {
		// 稳定状态
		if cc.stableStartTime.IsZero() {
			// 刚开始稳定
			cc.stableStartTime = time.Now()
			cc.logger.Info("Entering stable window", map[string]interface{}{
				"queue_size":      totalQueueSize,
				"last_event_time": lastEventTime,
			})
		} else {
			// 持续稳定
			stableDuration := time.Since(cc.stableStartTime)
			if stableDuration >= cc.stableWindow && !cc.converged {
				// 收敛！
				cc.converged = true
				cc.convergedAt = time.Now()
				cc.logger.Info("🎉 Migration CONVERGED!", map[string]interface{}{
					"stable_duration": stableDuration,
					"queue_size":      totalQueueSize,
				})
			}
		}
	} else {
		// 不稳定，重置
		if !cc.stableStartTime.IsZero() {
			cc.logger.Info("Exiting stable window", map[string]interface{}{
				"queue_size":      totalQueueSize,
				"last_event_time": lastEventTime,
			})
			cc.stableStartTime = time.Time{}
		}
	}
}
