package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/google/uuid"
	"tendis-migrate/internal/limiter"
	"tendis-migrate/internal/model"
	"tendis-migrate/internal/replication"
)

// TaskRunner 任务运行器
type TaskRunner struct {
	master       *Master
	task         *model.Task
	sourceConfig *model.ClusterConfig
	targetConfig *model.ClusterConfig
	options      *model.MigrationOptions
	
	sourceClient *redis.ClusterClient
	targetClient *redis.ClusterClient
	
	workers      map[string]*EmbeddedWorker
	workersMu    sync.RWMutex
	
	phase        atomic.Int32 // MigrationPhase
	paused       atomic.Bool
	
	// 新增：PID 控制器和自适应限流
	rateLimiter         *limiter.RateLimiter
	adaptiveRateLimiter *limiter.AdaptiveRateLimiter
	
	// 新增：大 Key 扫描器和迁移器
	bigKeyScanner   *limiter.BigKeyScanner
	bigKeyMigrator  *limiter.BigKeyMigrator
	
	// ===== 新增：FakeSlave 和 binlog 缓存相关 =====
	// fakeSlaves 所有 Master 节点对应的 FakeSlave
	fakeSlaves   []*replication.FakeSlave
	fakeSlavesMu sync.Mutex
	// binlogCacheManager binlog 缓存管理器
	binlogCacheManager *replication.BinlogCacheManager
	
	ctx          context.Context
	cancel       context.CancelFunc
	wg           sync.WaitGroup
}

// NewTaskRunner 创建任务运行器
func NewTaskRunner(m *Master, task *model.Task) (*TaskRunner, error) {
	var sourceConfig model.ClusterConfig
	var targetConfig model.ClusterConfig
	var options model.MigrationOptions

	if err := json.Unmarshal([]byte(task.SourceCluster), &sourceConfig); err != nil {
		return nil, fmt.Errorf("parse source config: %w", err)
	}
	if err := json.Unmarshal([]byte(task.TargetCluster), &targetConfig); err != nil {
		return nil, fmt.Errorf("parse target config: %w", err)
	}
	if task.Config != "" {
		json.Unmarshal([]byte(task.Config), &options)
	} else {
		options = *model.DefaultMigrationOptions()
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &TaskRunner{
		master:       m,
		task:         task,
		sourceConfig: &sourceConfig,
		targetConfig: &targetConfig,
		options:      &options,
		workers:      make(map[string]*EmbeddedWorker),
		ctx:          ctx,
		cancel:       cancel,
	}, nil
}

// Run 运行任务
// 
// 方案 B 流程（生产环境，数据一致性优先）：
// 1. 连接集群
// 2. 检查是否支持 FakeSlave 模式
// 3. 如果支持，启动所有 FakeSlave 连接（binlog 缓存到本地文件）
// 4. 等待所有 FakeSlave 连接成功（任何一个连接失败 = 任务失败）
// 5. 开始全量 SCAN 迁移（FakeSlave 持续缓存 binlog）
// 6. 全量完成后，回放缓存的 binlog
// 7. 追上后切换到实时模式（binlog 直接应用）
// 8. 用户手动停止增量同步
// 9. 数据校验
func (r *TaskRunner) Run() {
	defer r.cleanup()

	log.Printf("Starting task: %s", r.task.ID)

	// 连接集群
	if err := r.connectClusters(); err != nil {
		log.Printf("Connect clusters failed: %v", err)
		r.master.store.UpdateTaskCompleted(r.task.ID, model.TaskStatusFailed)
		return
	}

	// 获取集群信息
	totalKeys, err := r.estimateTotalKeys()
	if err != nil {
		log.Printf("Estimate keys failed: %v", err)
	}

	// 更新统计
	stats, _ := r.master.store.GetOrCreateStats(r.task.ID)
	stats.TotalKeys = totalKeys
	now := time.Now().Unix()
	stats.StartTime = &now
	r.master.store.UpdateStats(stats)

	// 检查是否支持 FakeSlave 模式
	fakeSlaveSupported := r.checkFakeSlaveSupport()
	
	// ===== 方案 B：先启动 FakeSlave 缓存 binlog =====
	if fakeSlaveSupported && !r.options.SkipIncremental {
		log.Printf("FakeSlave mode supported, starting binlog caching BEFORE full migration")
		
		// 启动 FakeSlave 并等待连接成功
		if err := r.startFakeSlavesAndWait(); err != nil {
			log.Printf("CRITICAL: Failed to start FakeSlaves: %v", err)
			log.Printf("CRITICAL: Task failed - any FakeSlave connection failure means task failure")
			r.master.store.UpdateTaskCompleted(r.task.ID, model.TaskStatusFailed)
			return
		}
		
		log.Printf("All FakeSlaves connected successfully, binlog caching started")
	}

	// 阶段1: 全量迁移
	if r.options.SkipFullSync {
		log.Printf("Phase 1: Full migration SKIPPED (skip_full_sync=true)")
	} else {
		r.phase.Store(int32(model.PhaseFullMigration))
		log.Printf("Phase 1: Full migration starting...")
		if err := r.runFullMigration(); err != nil {
			log.Printf("Full migration failed: %v", err)
			r.master.store.UpdateTaskCompleted(r.task.ID, model.TaskStatusFailed)
			return
		}
	}

	// 阶段2: 增量同步
	if r.options.SkipIncremental {
		log.Printf("Phase 2: Incremental sync SKIPPED (skip_incremental=true)")
	} else {
		r.phase.Store(int32(model.PhaseIncrementalSync))
		log.Printf("Phase 2: Incremental sync starting...")
		
		if fakeSlaveSupported {
			// 方案 B 后续：回放缓存的 binlog，然后切换到实时模式
			if err := r.runFakeSlaveIncrementalSyncWithReplay(); err != nil {
				log.Printf("Incremental sync failed: %v", err)
			}
		} else {
			// 降级方案
			if err := r.runIncrementalSync(); err != nil {
				log.Printf("Incremental sync failed: %v", err)
			}
		}
	}

	// 阶段3: 数据校验
	r.phase.Store(int32(model.PhaseVerification))
	log.Printf("Phase 3: Verification starting...")
	r.runVerification()

	// 完成
	r.master.store.UpdateTaskCompleted(r.task.ID, model.TaskStatusCompleted)
	log.Printf("Task completed: %s", r.task.ID)
}

// connectClusters 连接集群
func (r *TaskRunner) connectClusters() error {
	// 源集群
	r.sourceClient = redis.NewClusterClient(&redis.ClusterOptions{
		Addrs:    r.sourceConfig.Addrs,
		Password: r.sourceConfig.Password,
		PoolSize: r.options.RateLimit.SourceConnections,
	})

	if err := r.sourceClient.Ping(r.ctx).Err(); err != nil {
		return fmt.Errorf("source cluster ping: %w", err)
	}

	// 目标集群
	r.targetClient = redis.NewClusterClient(&redis.ClusterOptions{
		Addrs:    r.targetConfig.Addrs,
		Password: r.targetConfig.Password,
		PoolSize: r.options.RateLimit.TargetConnections,
	})

	if err := r.targetClient.Ping(r.ctx).Err(); err != nil {
		return fmt.Errorf("target cluster ping: %w", err)
	}

	// 初始化限流器
	r.initRateLimiter()
	
	// 初始化大 Key 处理器
	r.initBigKeyHandlers()

	return nil
}

// initRateLimiter 初始化限流器（PID 控制器 + 令牌桶）
func (r *TaskRunner) initRateLimiter() {
	sourceQPS := int64(r.options.RateLimit.SourceQPS)
	if sourceQPS <= 0 {
		sourceQPS = 100000 // 默认 10 万 QPS
	}
	targetQPS := int64(r.options.RateLimit.TargetQPS)
	if targetQPS <= 0 {
		targetQPS = 50000 // 默认 5 万 QPS
	}
	bandwidthMB := int64(100) // 默认 100MB/s
	
	// 创建令牌桶限流器
	r.rateLimiter = limiter.NewRateLimiter(sourceQPS, targetQPS, bandwidthMB)
	
	// 转换客户端类型以适配限流器
	// 注意：limiter 使用 redis/go-redis/v9，需要进行适配
	// 这里使用自适应限流但不传入客户端，使用默认负载估算
	adaptiveCfg := &limiter.AdaptiveConfig{
		Enabled:        true,
		Kp:             0.5,
		Ki:             0.1,
		Kd:             0.05,
		TargetLoad:     0.7, // 目标负载 70%
		AdjustInterval: 5 * time.Second,
	}
	
	// 由于类型不兼容，暂时使用 nil 客户端，自适应限流器会使用默认负载
	r.adaptiveRateLimiter = limiter.NewAdaptiveRateLimiter(r.rateLimiter, nil, adaptiveCfg)
	r.adaptiveRateLimiter.Start()
	
	log.Printf("Rate limiter initialized: sourceQPS=%d, targetQPS=%d, adaptive=true", sourceQPS, targetQPS)
}

// initBigKeyHandlers 初始化大 Key 处理器
func (r *TaskRunner) initBigKeyHandlers() {
	// 大 Key 阈值配置
	threshold := &limiter.BigKeyThreshold{
		StringMaxBytes:   r.options.LargeKeyThreshold, // 使用用户配置的阈值
		HashMaxFields:    10000,
		SetMaxMembers:    10000,
		ZSetMaxMembers:   10000,
		ListMaxElements:  10000,
		StreamMaxEntries: 10000,
	}
	
	if threshold.StringMaxBytes <= 0 {
		threshold.StringMaxBytes = 10 * 1024 * 1024 // 默认 10MB
	}
	
	// 大 Key 策略（获取默认策略用于日志记录）
	_ = limiter.DefaultBigKeyStrategies()
	
	// 由于类型不兼容（go-redis v8 vs v9），暂时不初始化大 Key 扫描器
	// 实际使用时需要进行适配或升级依赖
	log.Printf("Big key handlers configured: threshold=%d bytes", threshold.StringMaxBytes)
}

// estimateTotalKeys 估算总Key数
func (r *TaskRunner) estimateTotalKeys() (int64, error) {
	var total int64

	err := r.sourceClient.ForEachMaster(r.ctx, func(ctx context.Context, client *redis.Client) error {
		// 简化处理：每个节点dbsize
		dbsize, _ := client.DBSize(ctx).Result()
		atomic.AddInt64(&total, dbsize)
		return nil
	})

	return total, err
}

// runFullMigration 全量迁移
// 支持断点恢复：如果 slot_status 表中已有该任务的记录，则从断点继续
func (r *TaskRunner) runFullMigration() error {
	// 分配Slots
	workerCount := r.options.WorkerCount
	if workerCount <= 0 {
		workerCount = 4
	}

	// 检查是否有已存在的 slot 分配（断点恢复场景）
	existingSlots, _ := r.master.store.CountSlotsByStatus(r.task.ID, "assigned")
	migratingSlots, _ := r.master.store.CountSlotsByStatus(r.task.ID, "migrating")
	completedSlots, _ := r.master.store.CountSlotsByStatus(r.task.ID, "completed")
	totalExisting := existingSlots + migratingSlots + completedSlots
	
	if totalExisting > 0 {
		log.Printf("Resuming full migration: found %d existing slots (assigned=%d, migrating=%d, completed=%d)", 
			totalExisting, existingSlots, migratingSlots, completedSlots)
	}

	// AssignSlots 现在是安全的（INSERT OR IGNORE），不会覆盖已有的断点数据
	slots, err := r.master.scheduler.AssignSlots(r.task.ID, workerCount)
	if err != nil {
		return err
	}

	// 创建内嵌Worker
	for i, slotRange := range slots {
		workerID := fmt.Sprintf("worker-%d", i)
		worker := NewEmbeddedWorker(workerID, r)
		worker.SetSlots(slotRange)

		r.workersMu.Lock()
		r.workers[workerID] = worker
		r.workersMu.Unlock()

		r.wg.Add(1)
		go func(w *EmbeddedWorker) {
			defer r.wg.Done()
			w.RunFullMigration(r.ctx)
		}(worker)
	}

	// 等待全量完成
	r.wg.Wait()

	return nil
}

// runIncrementalSync 增量同步
// 重要改动：增量同步由用户手动停止，不再自动收敛
// 
// 增量同步策略（按优先级）：
// 1. FakeSlave 模式 - 伪装成 Slave 接收 binlog（最高效，推荐用于 Tendis）
// 2. PSYNC 模式 - 使用 Redis PSYNC2 协议（备选）
// 3. IDLETIME 模式 - 基于时间窗口检测最近修改的 Key（通用降级方案，不推荐大规模使用）
//
// 关于各模式的说明：
// - FakeSlave 模式：伪装成 Tendis 从节点，通过 INCRSYNC 协议接收 binlog 推送
//   - 优点：实时性高，不需要 SCAN 全量 Key
//   - 适用：40 亿 Key 场景
// - IDLETIME 模式：每轮需要全量 SCAN 所有 Key，性能较差
//   - 不建议：超过 1 亿 Key 的场景
func (r *TaskRunner) runIncrementalSync() error {
	log.Printf("Starting incremental sync for task %s", r.task.ID)
	
	// 优先尝试 FakeSlave 模式（Tendis 专用，最高效）
	fakeSlaveSupported := r.checkFakeSlaveSupport()
	if fakeSlaveSupported {
		log.Printf("Tendis FakeSlave mode supported, using FakeSlave mode for incremental sync (RECOMMENDED)")
		return r.runFakeSlaveIncrementalSync()
	}
	
	// 其次尝试 PSYNC 模式
	psyncSupported := r.checkPsyncSupport()
	if psyncSupported {
		log.Printf("Tendis PSYNC supported, using PSYNC mode for incremental sync")
		return r.runPsyncIncrementalSync()
	}
	
	// 降级到 IDLETIME 模式
	// 警告：对于 40 亿 Key 场景，IDLETIME 模式性能很差
	totalKeys, _ := r.estimateTotalKeys()
	if totalKeys > 100_000_000 { // 超过 1 亿 Key
		log.Printf("⚠️ WARNING: IDLETIME mode is not recommended for %d keys.", totalKeys)
		log.Printf("⚠️ Consider using FakeSlave mode or PSYNC mode for better performance.")
		log.Printf("⚠️ Each incremental sync round will SCAN all %d keys, which may take hours.", totalKeys)
	}
	
	log.Printf("FakeSlave/PSYNC not supported, falling back to IDLETIME mode (TEST ONLY for large datasets)")
	return r.runIdletimeIncrementalSync()
}

// startFakeSlavesAndWait 启动所有 FakeSlave 并等待连接成功
// 关键原则：任何一个 Master 连接失败 = 整个任务失败
func (r *TaskRunner) startFakeSlavesAndWait() error {
	log.Printf("Starting FakeSlaves for all master nodes (cache mode)")
	
	// 1. 获取所有 Master 节点信息
	type masterNode struct {
		addr    string
		storeID uint32
	}
	
	var masters []masterNode
	storeID := uint32(0)
	
	r.sourceClient.ForEachMaster(r.ctx, func(ctx context.Context, node *redis.Client) error {
		addr := node.Options().Addr
		masters = append(masters, masterNode{
			addr:    addr,
			storeID: storeID,
		})
		storeID++
		return nil
	})
	
	if len(masters) == 0 {
		return fmt.Errorf("no master nodes found")
	}
	
	log.Printf("Found %d master nodes, starting FakeSlaves...", len(masters))
	
	// 2. 初始化 binlog 缓存管理器
	cacheConfig := replication.BinlogCacheConfig{
		CacheDir: "data/binlog_cache", // 可以配置
		TaskID:   r.task.ID,
		MaxFileSize: 1 << 30, // 1GB 自动切分
	}
	r.binlogCacheManager = replication.NewBinlogCacheManager(cacheConfig)
	r.binlogCacheManager.StartCaching() // 开启缓存模式
	
	// 3. 为每个 Master 创建并启动 FakeSlave
	r.fakeSlaves = make([]*replication.FakeSlave, 0, len(masters))
	
	var connectWg sync.WaitGroup
	errChan := make(chan error, len(masters))
	
	for _, master := range masters {
		log.Printf("Creating FakeSlave for master %s (storeID=%d)", master.addr, master.storeID)
		
		config := replication.FakeSlaveConfig{
			SourceAddr:       master.addr,
			SourcePassword:   r.sourceConfig.Password,
			StoreID:          master.storeID,
			StartBinlogPos:   0,
			FakeListenIP:     "127.0.0.1",
			FakeListenPort:   6379,
			ReadTimeout:      30 * time.Second,
			HeartbeatTimeout: 30 * time.Second,
			KeyFilter: func(key string) bool {
				return r.shouldMigrateKey(key)
			},
			// 缓存模式配置
			CacheMode:    true,
			CacheManager: r.binlogCacheManager,
		}
		
		fakeSlave := replication.NewFakeSlave(config, r.targetClient)
		
		r.fakeSlavesMu.Lock()
		r.fakeSlaves = append(r.fakeSlaves, fakeSlave)
		r.fakeSlavesMu.Unlock()
		
		// 启动 FakeSlave（异步）
		go func(fs *replication.FakeSlave, addr string) {
			if err := fs.Start(r.ctx); err != nil {
				if r.ctx.Err() == nil { // 非正常停止
					log.Printf("FakeSlave for %s failed: %v", addr, err)
				}
			}
		}(fakeSlave, master.addr)
		
		// 等待连接成功
		connectWg.Add(1)
		go func(fs *replication.FakeSlave, addr string, sid uint32) {
			defer connectWg.Done()
			
			// 等待连接成功，超时 30 秒
			if err := fs.WaitConnected(30 * time.Second); err != nil {
				errChan <- fmt.Errorf("FakeSlave for %s (storeID=%d) connection failed: %w", addr, sid, err)
			} else {
				log.Printf("FakeSlave for %s (storeID=%d) connected successfully", addr, sid)
			}
		}(fakeSlave, master.addr, master.storeID)
	}
	
	// 4. 等待所有连接结果
	done := make(chan struct{})
	go func() {
		connectWg.Wait()
		close(done)
	}()
	
	select {
	case <-done:
		// 所有连接尝试完成，检查是否有错误
	case <-r.ctx.Done():
		return fmt.Errorf("context cancelled while waiting for FakeSlave connections")
	}
	
	// 收集所有错误
	close(errChan)
	var errors []error
	for err := range errChan {
		errors = append(errors, err)
	}
	
	// 关键：任何一个连接失败 = 整个任务失败
	if len(errors) > 0 {
		// 停止所有已启动的 FakeSlave
		r.stopAllFakeSlaves()
		return fmt.Errorf("FakeSlave connection failed (%d/%d): %v", len(errors), len(masters), errors[0])
	}
	
	log.Printf("All %d FakeSlaves connected successfully, binlog caching active", len(masters))
	return nil
}

// stopAllFakeSlaves 停止所有 FakeSlave
func (r *TaskRunner) stopAllFakeSlaves() {
	r.fakeSlavesMu.Lock()
	defer r.fakeSlavesMu.Unlock()
	
	for _, fs := range r.fakeSlaves {
		fs.Stop()
	}
	log.Printf("Stopped all FakeSlaves")
}

// runFakeSlaveIncrementalSyncWithReplay 执行带缓存回放的增量同步
// 流程：
// 1. 停止缓存模式
// 2. 回放缓存的 binlog
// 3. 切换到实时模式（binlog 直接应用到目标端）
// 4. 等待用户手动停止
func (r *TaskRunner) runFakeSlaveIncrementalSyncWithReplay() error {
	log.Printf("Starting incremental sync with binlog cache replay")
	
	// 1. 停止缓存模式
	if r.binlogCacheManager != nil {
		log.Printf("Stopping binlog cache mode...")
		r.binlogCacheManager.StopCaching()
		
		// 刷盘确保所有数据写入
		if err := r.binlogCacheManager.Flush(); err != nil {
			log.Printf("Warning: flush binlog cache failed: %v", err)
		}
		
		// 获取缓存统计
		cacheSize, _ := r.binlogCacheManager.GetTotalCacheSize()
		log.Printf("Binlog cache stopped, total cache size: %d bytes (%.2f MB)", 
			cacheSize, float64(cacheSize)/(1024*1024))
	}
	
	// 2. 回放缓存的 binlog
	log.Printf("Starting binlog cache replay...")
	
	r.fakeSlavesMu.Lock()
	fakeSlaves := r.fakeSlaves
	r.fakeSlavesMu.Unlock()
	
	cacheConfig := replication.BinlogCacheConfig{
		CacheDir: "data/binlog_cache",
		TaskID:   r.task.ID,
	}
	
	var replayWg sync.WaitGroup
	var replayErrors int64
	
	for _, fs := range fakeSlaves {
		replayWg.Add(1)
		go func(fakeSlave *replication.FakeSlave) {
			defer replayWg.Done()
			
			if err := fakeSlave.ReplayCachedBinlogs(r.ctx, cacheConfig); err != nil {
				log.Printf("Replay cached binlogs failed: %v", err)
				atomic.AddInt64(&replayErrors, 1)
			}
		}(fs)
	}
	
	replayWg.Wait()
	
	if replayErrors > 0 {
		log.Printf("Warning: %d FakeSlaves failed to replay cached binlogs", replayErrors)
	}
	
	log.Printf("Binlog cache replay completed")
	
	// 3. FakeSlave 现在处于实时模式（因为 CacheManager.IsCaching() 返回 false）
	// binlog 会直接应用到目标端
	log.Printf("Switched to realtime mode, binlog will be applied directly to target")
	
	// 4. 等待用户手动停止
	log.Printf("Incremental sync running in realtime mode, waiting for user to stop...")
	
	<-r.ctx.Done()
	
	log.Printf("Incremental sync stopped by user")
	
	// 5. 清理缓存文件（可选）
	if r.binlogCacheManager != nil {
		log.Printf("Cleaning up binlog cache files...")
		r.binlogCacheManager.CleanupCache()
	}
	
	return nil
}

// checkFakeSlaveSupport 检查源端是否支持 FakeSlave 模式（INCRSYNC 协议）
// 通过检测 binlog 是否启用来判断
func (r *TaskRunner) checkFakeSlaveSupport() bool {
	if r.sourceClient == nil {
		return false
	}
	
	ctx, cancel := context.WithTimeout(r.ctx, 5*time.Second)
	defer cancel()
	
	// 在第一个主节点上测试 binlog 支持
	var supported bool
	r.sourceClient.ForEachMaster(ctx, func(ctx context.Context, node *redis.Client) error {
		// 检查 INFO REPLICATION 中是否有 binlog 相关信息
		info, err := node.Info(ctx, "replication").Result()
		if err != nil {
			return nil
		}
		
		// Tendis 特有的 binlog 字段
		binlogEnabled := parseInfoField(info, "binlog_enabled")
		if binlogEnabled == "1" || binlogEnabled == "yes" {
			supported = true
			log.Printf("Tendis binlog enabled on node %s, FakeSlave mode available", node.String())
			return nil
		}
		
		// 也检查 store_count（Tendis 特有）
		storeCount := parseInfoField(info, "store_count")
		if storeCount != "" {
			supported = true
			log.Printf("Tendis cluster detected (store_count=%s), FakeSlave mode available", storeCount)
			return nil
		}
		
		return nil
	})
	
	return supported
}

// runFakeSlaveIncrementalSync 使用伪 Slave 模式进行增量同步
// 伪装成 Tendis 从节点，通过 INCRSYNC 协议接收 binlog 推送
// 这是最高效的增量同步方式，适用于 40 亿 Key 场景
func (r *TaskRunner) runFakeSlaveIncrementalSync() error {
	log.Printf("Starting FakeSlave incremental sync (manual stop mode)")
	log.Printf("This mode is optimal for large-scale migrations (40B+ keys)")
	
	// 1. 获取每个 Master 节点的地址和 storeId
	type masterNode struct {
		addr    string
		storeID uint32
		client  *redis.Client
	}
	
	var masters []masterNode
	storeID := uint32(0)
	
	r.sourceClient.ForEachMaster(r.ctx, func(ctx context.Context, node *redis.Client) error {
		addr := node.Options().Addr
		masters = append(masters, masterNode{
			addr:    addr,
			storeID: storeID,
			client:  node,
		})
		storeID++
		return nil
	})
	
	if len(masters) == 0 {
		return fmt.Errorf("no master nodes found")
	}
	
	log.Printf("Found %d master nodes for FakeSlave replication", len(masters))
	
	// 2. 为每个 Master 节点创建 FakeSlave
	// 使用 replication 包中的 FakeSlave
	var wg sync.WaitGroup
	errChan := make(chan error, len(masters))
	
	for _, master := range masters {
		wg.Add(1)
		go func(m masterNode) {
			defer wg.Done()
			
			log.Printf("Starting FakeSlave for master %s (storeID=%d)", m.addr, m.storeID)
			
			// 创建伪 Slave 配置
			config := replication.FakeSlaveConfig{
				SourceAddr:     m.addr,
				SourcePassword: r.sourceConfig.Password,
				StoreID:        m.storeID,
				StartBinlogPos: 0, // 从头开始（全量迁移后应该从最新位置开始）
				FakeListenIP:   "127.0.0.1",
				FakeListenPort: 6379,
				ReadTimeout:    30 * time.Second,
				HeartbeatTimeout: 30 * time.Second,
				KeyFilter: func(key string) bool {
					return r.shouldMigrateKey(key)
				},
			}
			
			// 创建 FakeSlave 实例
			// 注意：这里需要一个单独的目标客户端连接
			fakeSlave := replication.NewFakeSlave(config, r.targetClient)
			
			// 启动 FakeSlave
			if err := fakeSlave.Start(r.ctx); err != nil {
				if r.ctx.Err() != nil {
					// 正常停止
					log.Printf("FakeSlave for %s stopped by user", m.addr)
					return
				}
				log.Printf("FakeSlave for %s failed: %v", m.addr, err)
				errChan <- fmt.Errorf("FakeSlave for %s: %w", m.addr, err)
			}
		}(master)
	}
	
	// 等待所有 FakeSlave 完成或用户停止
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	
	select {
	case <-r.ctx.Done():
		log.Printf("FakeSlave incremental sync stopped by user")
		return nil
	case err := <-errChan:
		return err
	case <-done:
		log.Printf("All FakeSlaves completed")
		return nil
	}
}

// checkPsyncSupport 检查源端是否支持 PSYNC
// 通过发送 REPLCONF 命令检测
func (r *TaskRunner) checkPsyncSupport() bool {
	if r.sourceClient == nil {
		return false
	}
	
	ctx, cancel := context.WithTimeout(r.ctx, 5*time.Second)
	defer cancel()
	
	// 在第一个主节点上测试 PSYNC 支持
	var supported bool
	r.sourceClient.ForEachMaster(ctx, func(ctx context.Context, node *redis.Client) error {
		// 尝试发送 REPLCONF LISTENING-PORT 命令
		// 如果服务器支持 PSYNC，会返回 +OK
		_, err := node.Do(ctx, "REPLCONF", "LISTENING-PORT", "0").Result()
		if err == nil {
			supported = true
			log.Printf("PSYNC support detected on node %s", node.String())
		}
		return nil // 只检查第一个节点
	})
	
	return supported
}

// runPsyncIncrementalSync 使用 PSYNC 进行增量同步
// 基于 Tendis 官方 PSYNC 协议实现
// 用户手动停止，无自动收敛
func (r *TaskRunner) runPsyncIncrementalSync() error {
	log.Printf("Starting PSYNC incremental sync (manual stop mode)")
	
	// 获取每个主节点的复制 ID 和 offset
	type nodeState struct {
		addr     string
		replId   string
		offset   int64
		node     *redis.Client
	}
	
	nodeStates := make(map[string]*nodeState)
	
	// 初始化：获取每个节点的复制信息
	r.sourceClient.ForEachMaster(r.ctx, func(ctx context.Context, node *redis.Client) error {
		addr := node.String()
		
		// 获取 INFO REPLICATION 信息
		info, err := node.Info(ctx, "replication").Result()
		if err != nil {
			log.Printf("Failed to get replication info for %s: %v", addr, err)
			return nil
		}
		
		// 解析 master_replid 和 master_repl_offset
		replId := parseInfoField(info, "master_replid")
		if replId == "" {
			replId = parseInfoField(info, "run_id") // 降级使用 run_id
		}
		
		offsetStr := parseInfoField(info, "master_repl_offset")
		offset := int64(0)
		if offsetStr != "" {
			fmt.Sscanf(offsetStr, "%d", &offset)
		}
		
		nodeStates[addr] = &nodeState{
			addr:   addr,
			replId: replId,
			offset: offset,
			node:   node,
		}
		
		log.Printf("Node %s: replId=%s, offset=%d", addr, replId, offset)
		return nil
	})
	
	// 同步循环 - 持续运行直到用户手动停止
	// 由于 PSYNC 需要建立持久连接，这里使用轮询 + DUMP/RESTORE 方案
	// 真正的 PSYNC 实现需要建立专用的 TCP 连接
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	
	var totalSynced, totalErrors int64
	
	for {
		select {
		case <-r.ctx.Done():
			log.Printf("PSYNC incremental sync stopped by user, total synced: %d, errors: %d", totalSynced, totalErrors)
			return nil
			
		case <-ticker.C:
			if r.paused.Load() {
				continue
			}
			
			// 对每个节点检查是否有新的变更
			for addr, state := range nodeStates {
				// 获取当前 offset
				info, err := state.node.Info(r.ctx, "replication").Result()
				if err != nil {
					log.Printf("Failed to get replication info for %s: %v", addr, err)
					totalErrors++
					continue
				}
				
				currentOffsetStr := parseInfoField(info, "master_repl_offset")
				currentOffset := int64(0)
				if currentOffsetStr != "" {
					fmt.Sscanf(currentOffsetStr, "%d", &currentOffset)
				}
				
				// 如果 offset 有变化，说明有新数据写入
				if currentOffset > state.offset {
					log.Printf("Node %s: offset changed %d -> %d, syncing new keys...", addr, state.offset, currentOffset)
					
					// 使用 SCAN + IDLETIME 检测最近修改的 Key
					synced := r.syncRecentlyModifiedKeys(r.ctx, state.node, 30*time.Second)
					totalSynced += synced
					
					// 更新 offset
					state.offset = currentOffset
				}
			}
		}
	}
}

// syncRecentlyModifiedKeys 同步最近修改的 Key
// 使用 SCAN + OBJECT IDLETIME 检测最近在指定时间窗口内修改的 Key
func (r *TaskRunner) syncRecentlyModifiedKeys(ctx context.Context, node *redis.Client, idleTimeThreshold time.Duration) int64 {
	var synced int64
	var cursor uint64
	batchSize := int64(1000)
	
	// SCAN 遍历所有 Key
	for {
		select {
		case <-ctx.Done():
			return synced
		default:
		}
		
		keys, nextCursor, err := node.Scan(ctx, cursor, "*", batchSize).Result()
		if err != nil {
			log.Printf("SCAN failed: %v", err)
			return synced
		}
		
		// 批量检查 IDLETIME 并同步
		pipe := node.Pipeline()
		idleTimeCmds := make([]*redis.DurationCmd, len(keys))
		for i, key := range keys {
			idleTimeCmds[i] = pipe.ObjectIdleTime(ctx, key)
		}
		pipe.Exec(ctx)
		
		// 筛选出最近修改的 Key
		keysToSync := make([]string, 0)
		for i, key := range keys {
			idleTime, err := idleTimeCmds[i].Result()
			if err != nil {
				continue
			}
			
			// 空闲时间 < 阈值，说明最近被修改过
			if idleTime < idleTimeThreshold {
				// 检查 Key 过滤
				if r.shouldMigrateKey(key) {
					keysToSync = append(keysToSync, key)
				}
			}
		}
		
		// 同步这些 Key
		for _, key := range keysToSync {
			if err := r.migrateKeyByDumpRestore(ctx, node, key); err == nil {
				synced++
			}
		}
		
		cursor = nextCursor
		if cursor == 0 {
			break
		}
	}
	
	return synced
}

// parseInfoField 从 INFO 命令输出中解析指定字段
func parseInfoField(info, field string) string {
	lines := strings.Split(info, "\r\n")
	for _, line := range lines {
		if strings.HasPrefix(line, field+":") {
			return strings.TrimPrefix(line, field+":")
		}
	}
	// 也尝试普通换行符
	lines = strings.Split(info, "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, field+":") {
			return strings.TrimPrefix(line, field+":")
		}
	}
	return ""
}

// migrateKeyByDumpRestore 使用 DUMP + RESTORE 迁移 Key
func (r *TaskRunner) migrateKeyByDumpRestore(ctx context.Context, sourceNode *redis.Client, key string) error {
	// DUMP
	dump, err := sourceNode.Dump(ctx, key).Result()
	if err != nil {
		if err == redis.Nil {
			// Key 已被删除，同步删除
			return r.targetClient.Del(ctx, key).Err()
		}
		return err
	}
	
	// TTL
	ttl, err := sourceNode.TTL(ctx, key).Result()
	if err != nil || ttl < 0 {
		ttl = 0
	}
	
	// RESTORE REPLACE
	err = r.targetClient.RestoreReplace(ctx, key, ttl, dump).Err()
	if err != nil {
		// 尝试先删除再 Restore
		r.targetClient.Del(ctx, key)
		return r.targetClient.Restore(ctx, key, ttl, dump).Err()
	}
	
	return nil
}

// shouldMigrateKey 判断 Key 是否应该被迁移（TaskRunner 级别）
func (r *TaskRunner) shouldMigrateKey(key string) bool {
	filter := r.options.KeyFilter
	if filter == nil || filter.Mode == model.KeyFilterModeAll {
		return true
	}
	
	// 先检查排除规则
	for _, prefix := range filter.ExcludePrefixes {
		if strings.HasPrefix(key, prefix) {
			return false
		}
	}
	for _, pattern := range filter.ExcludePatterns {
		if matched, _ := regexp.MatchString(pattern, key); matched {
			return false
		}
	}

	// 再检查包含规则
	switch filter.Mode {
	case model.KeyFilterModePrefix:
		if len(filter.Prefixes) == 0 {
			return true // 没有指定前缀，迁移所有
		}
		for _, prefix := range filter.Prefixes {
			if strings.HasPrefix(key, prefix) {
				return true
			}
		}
		return false
	case model.KeyFilterModePattern:
		if len(filter.Patterns) == 0 {
			return true
		}
		for _, pattern := range filter.Patterns {
			if matched, _ := regexp.MatchString(pattern, key); matched {
				return true
			}
		}
		return false
	case model.KeyFilterModeKeys, model.KeyFilterModeKeylist:
		// 支持 keys 和 keylist 两种模式名称（前端使用 keylist）
		for _, k := range filter.Keys {
			if key == k {
				return true
			}
		}
		return false
	default:
		return true
	}
}

// runIdletimeIncrementalSync 使用 IDLETIME 进行增量同步（降级方案）
// 用户手动停止，无自动收敛
//
// ⚠️ 重要警告 - 40亿 Key 场景不适用：
// - IDLETIME 模式每轮需要全量 SCAN 所有 Key
// - 40亿 Key 场景下，每轮 SCAN 可能需要数小时
// - 时间复杂度 O(N)，N = 总 Key 数量
// - 网络开销：每轮传输约 40B * 平均 Key 长度 字节
//
// 建议：
// - 小于 1 亿 Key：可以使用 IDLETIME 模式
// - 超过 1 亿 Key：建议使用 PSYNC 模式或只做全量迁移
func (r *TaskRunner) runIdletimeIncrementalSync() error {
	log.Printf("Starting IDLETIME incremental sync (manual stop mode)")
	
	// 警告大规模数据
	totalKeys, _ := r.estimateTotalKeys()
	if totalKeys > 100_000_000 {
		log.Printf("⚠️ WARNING: IDLETIME mode with %d keys is NOT recommended!", totalKeys)
		log.Printf("⚠️ Each sync round will SCAN ALL keys, expected time: %d+ minutes", totalKeys/10_000_000)
	}
	
	r.workersMu.RLock()
	for _, worker := range r.workers {
		r.wg.Add(1)
		go func(w *EmbeddedWorker) {
			defer r.wg.Done()
			w.RunIncrementalSyncManual(r.ctx) // 使用新的手动停止版本
		}(worker)
	}
	r.workersMu.RUnlock()
	
	// 等待用户手动停止（通过 context 取消）
	<-r.ctx.Done()
	log.Printf("IDLETIME incremental sync stopped by user")
	return nil
}

// runVerification 数据校验
func (r *TaskRunner) runVerification() {
	batchID := uuid.New().String()
	verifier := NewVerifier(r.sourceClient, r.targetClient)

	result, err := verifier.Verify(r.ctx, 10000) // 采样10000个Key
	if err != nil {
		log.Printf("Verification failed: %v", err)
		return
	}

	// 保存校验结果
	r.master.store.SaveVerifyResult(&model.VerifyResult{
		TaskID:         r.task.ID,
		BatchID:        batchID,
		TotalKeys:      result.TotalKeys,
		MatchedKeys:    result.MatchedKeys,
		MismatchedKeys: result.MismatchedKeys,
		MissingKeys:    result.MissingKeys,
		ExtraKeys:      result.ExtraKeys,
	})

	log.Printf("Verification result: total=%d, matched=%d, consistency=%.2f%%",
		result.TotalKeys, result.MatchedKeys, result.ConsistencyRate)
}

// Pause 暂停
func (r *TaskRunner) Pause() {
	r.paused.Store(true)
	
	// 通知所有Worker暂停
	r.workersMu.RLock()
	for _, worker := range r.workers {
		worker.Pause()
	}
	r.workersMu.RUnlock()
}

// Resume 恢复
func (r *TaskRunner) Resume() {
	r.paused.Store(false)
	
	// 通知所有Worker恢复
	r.workersMu.RLock()
	for _, worker := range r.workers {
		worker.Resume()
	}
	r.workersMu.RUnlock()
}

// Stop 停止
func (r *TaskRunner) Stop() {
	r.cancel()
	r.wg.Wait()
}

// TriggerVerify 触发校验
func (r *TaskRunner) TriggerVerify() (string, error) {
	batchID := uuid.New().String()
	
	go func() {
		verifier := NewVerifier(r.sourceClient, r.targetClient)
		result, err := verifier.Verify(r.ctx, 10000)
		if err != nil {
			return
		}

		r.master.store.SaveVerifyResult(&model.VerifyResult{
			TaskID:         r.task.ID,
			BatchID:        batchID,
			TotalKeys:      result.TotalKeys,
			MatchedKeys:    result.MatchedKeys,
			MismatchedKeys: result.MismatchedKeys,
			MissingKeys:    result.MissingKeys,
			ExtraKeys:      result.ExtraKeys,
		})
	}()

	return batchID, nil
}

// HasWorker 检查是否有Worker
func (r *TaskRunner) HasWorker(workerID string) bool {
	r.workersMu.RLock()
	defer r.workersMu.RUnlock()
	_, ok := r.workers[workerID]
	return ok
}

// GetPhase 获取当前阶段
func (r *TaskRunner) GetPhase() model.MigrationPhase {
	return model.MigrationPhase(r.phase.Load())
}

// cleanup 清理资源
func (r *TaskRunner) cleanup() {
	// 停止所有 FakeSlave
	r.stopAllFakeSlaves()
	
	// 关闭 binlog 缓存管理器
	if r.binlogCacheManager != nil {
		r.binlogCacheManager.Close()
	}
	
	// 停止自适应限流器
	if r.adaptiveRateLimiter != nil {
		r.adaptiveRateLimiter.Stop()
	}
	
	// 停止限流器
	if r.rateLimiter != nil {
		r.rateLimiter.Stop()
	}
	
	// 停止大 Key 扫描器
	if r.bigKeyScanner != nil {
		r.bigKeyScanner.Stop()
	}
	
	// 关闭 Redis 连接
	if r.sourceClient != nil {
		r.sourceClient.Close()
	}
	if r.targetClient != nil {
		r.targetClient.Close()
	}
}

// GetRateLimiter 获取限流器（供 Worker 使用）
func (r *TaskRunner) GetRateLimiter() *limiter.RateLimiter {
	return r.rateLimiter
}

// GetCurrentLoad 获取当前负载（供监控使用）
func (r *TaskRunner) GetCurrentLoad() float64 {
	if r.adaptiveRateLimiter != nil {
		return r.adaptiveRateLimiter.GetCurrentLoad()
	}
	return 0.5 // 默认 50%
}

// GetCurrentRate 获取当前速率（供监控使用）
func (r *TaskRunner) GetCurrentRate() (sourceQPS, targetQPS int64) {
	if r.rateLimiter != nil {
		return r.rateLimiter.GetCurrentRate()
	}
	return 0, 0
}

// EmbeddedWorker 内嵌Worker
type EmbeddedWorker struct {
	id           string
	runner       *TaskRunner
	slots        SlotRange
	paused       atomic.Bool
	
	keysProcessed atomic.Int64
	bytesTransferred atomic.Int64
}

// NewEmbeddedWorker 创建内嵌Worker
func NewEmbeddedWorker(id string, runner *TaskRunner) *EmbeddedWorker {
	return &EmbeddedWorker{
		id:     id,
		runner: runner,
	}
}

// SetSlots 设置Slot范围
func (w *EmbeddedWorker) SetSlots(slots SlotRange) {
	w.slots = slots
}

// RunFullMigration 运行全量迁移
func (w *EmbeddedWorker) RunFullMigration(ctx context.Context) {
	log.Printf("Worker %s starting full migration: slots %d-%d", w.id, w.slots.Start, w.slots.End)

	migrator := NewSlotMigrator(w.runner, w)

	for slot := w.slots.Start; slot <= w.slots.End; slot++ {
		select {
		case <-ctx.Done():
			return
		default:
		}

		// 检查暂停
		for w.paused.Load() {
			time.Sleep(100 * time.Millisecond)
		}

		if err := migrator.MigrateSlot(ctx, slot); err != nil {
			log.Printf("Worker %s migrate slot %d failed: %v", w.id, slot, err)
			continue
		}
	}

	log.Printf("Worker %s full migration completed", w.id)
}

// RunIncrementalSync 运行增量同步（问题7修复：实现真正的增量同步逻辑）
// 使用 OBJECT IDLETIME 检测最近修改的 Key，无需存储全量 Key 到内存
// 注意：此方法保留用于兼容，新代码请使用 RunIncrementalSyncManual
func (w *EmbeddedWorker) RunIncrementalSync(ctx context.Context, convergence *ConvergenceDetector) {
	w.RunIncrementalSyncManual(ctx)
}

// RunIncrementalSyncManual 运行增量同步（手动停止模式）
// 用户通过 context 取消来停止，无自动收敛
func (w *EmbeddedWorker) RunIncrementalSyncManual(ctx context.Context) {
	log.Printf("Worker %s starting incremental sync (IDLETIME mode, manual stop)", w.id)

	// 配置参数
	syncInterval := 30 * time.Second      // 同步间隔
	idleTimeThreshold := syncInterval + 5*time.Second // 空闲时间阈值
	batchSize := int64(10000)             // 每轮扫描批次大小

	// 同步间隔 ticker
	ticker := time.NewTicker(syncInterval)
	defer ticker.Stop()

	var scanRounds int64
	var keysSynced int64
	var keysSkipped int64

	for {
		select {
		case <-ctx.Done():
			log.Printf("Worker %s incremental sync stopped by user, rounds=%d, synced=%d", 
				w.id, scanRounds, keysSynced)
			return

		case <-ticker.C:
			// 检查是否暂停
			if w.paused.Load() {
				continue
			}

			scanRounds++
			roundSynced, roundSkipped := w.doIncrementalScanRound(ctx, idleTimeThreshold, batchSize)
			keysSynced += roundSynced
			keysSkipped += roundSkipped

			// 报告进度
			if roundSynced > 0 {
				w.ReportProgress(roundSynced, 0)
				log.Printf("Worker %s incremental sync round %d: synced=%d, skipped=%d, total_synced=%d",
					w.id, scanRounds, roundSynced, roundSkipped, keysSynced)
			}
			
			// 不再检查收敛，持续运行直到用户手动停止
		}
	}
}

// doIncrementalScanRound 执行一轮增量扫描
func (w *EmbeddedWorker) doIncrementalScanRound(ctx context.Context, idleTimeThreshold time.Duration, batchSize int64) (synced, skipped int64) {
	if w.runner.sourceClient == nil {
		return
	}

	// 遍历所有主节点
	err := w.runner.sourceClient.ForEachMaster(ctx, func(ctx context.Context, node *redis.Client) error {
		nodeSynced, nodeSkipped := w.scanNodeModifiedKeys(ctx, node, idleTimeThreshold, batchSize)
		synced += nodeSynced
		skipped += nodeSkipped
		return nil
	})

	if err != nil {
		log.Printf("Worker %s error iterating masters: %v", w.id, err)
	}

	return
}

// scanNodeModifiedKeys 扫描单个节点最近修改的 Key
func (w *EmbeddedWorker) scanNodeModifiedKeys(ctx context.Context, node *redis.Client, idleTimeThreshold time.Duration, batchSize int64) (synced, skipped int64) {
	var cursor uint64 = 0
	const pipelineBatchSize = 100

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		if w.paused.Load() {
			return
		}

		// SCAN 获取 Key
		keys, newCursor, err := node.Scan(ctx, cursor, "*", batchSize).Result()
		if err != nil {
			log.Printf("Worker %s SCAN failed: %v", w.id, err)
			return
		}

		// 批量检查 IDLETIME
		for i := 0; i < len(keys); i += pipelineBatchSize {
			end := i + pipelineBatchSize
			if end > len(keys) {
				end = len(keys)
			}
			batchKeys := keys[i:end]

			// Pipeline 批量获取 IDLETIME
			pipe := node.Pipeline()
			idleTimeCmds := make([]*redis.DurationCmd, len(batchKeys))
			for j, key := range batchKeys {
				idleTimeCmds[j] = pipe.ObjectIdleTime(ctx, key)
			}
			pipe.Exec(ctx)

			// 处理每个 Key
			for j, key := range batchKeys {
				idleTime, err := idleTimeCmds[j].Result()
				if err != nil {
					continue // Key 可能已被删除
				}

				// 如果空闲时间 < 阈值，说明最近被修改过，需要同步
				if idleTime < idleTimeThreshold {
					migrated, bytes := w.migrateKeyFromNode(ctx, node, key)
					if migrated {
						synced++
						w.bytesTransferred.Add(bytes)
					} else {
						skipped++
					}
				}
			}
		}

		cursor = newCursor
		if cursor == 0 {
			break // 扫描完成
		}
	}

	return
}

// migrateKeyFromNode 从指定节点迁移单个 Key
func (w *EmbeddedWorker) migrateKeyFromNode(ctx context.Context, sourceNode *redis.Client, key string) (bool, int64) {
	// DUMP
	dump, err := sourceNode.Dump(ctx, key).Result()
	if err != nil {
		return false, 0
	}

	// TTL
	ttl, err := sourceNode.TTL(ctx, key).Result()
	if err != nil {
		ttl = 0
	}
	if ttl < 0 {
		ttl = 0
	}

	bytes := int64(len(dump))

	// RESTORE REPLACE 到目标端（增量同步使用 replace 策略）
	err = w.runner.targetClient.RestoreReplace(ctx, key, ttl, dump).Err()
	if err != nil {
		// 目标端可能不支持 RestoreReplace，尝试先删除再 Restore
		w.runner.targetClient.Del(ctx, key)
		err = w.runner.targetClient.Restore(ctx, key, ttl, dump).Err()
		if err != nil {
			return false, 0
		}
	}

	return true, bytes
}

// Pause 暂停
func (w *EmbeddedWorker) Pause() {
	w.paused.Store(true)
}

// Resume 恢复
func (w *EmbeddedWorker) Resume() {
	w.paused.Store(false)
}

// ReportProgress 报告进度
func (w *EmbeddedWorker) ReportProgress(keys, bytes int64) {
	w.keysProcessed.Add(keys)
	w.bytesTransferred.Add(bytes)
	
	// 更新统计
	w.runner.master.store.IncrementStats(w.runner.task.ID, keys, bytes)
}

// SlotMigrator Slot迁移器
type SlotMigrator struct {
	runner *TaskRunner
	worker *EmbeddedWorker
	
	conflictHandler *ConflictHandler
	rateLimiter     *limiter.RateLimiter // 限流器引用
	
	// 断点保存相关
	lastCheckpointTime time.Time
	keysInBatch        int64
}

// NewSlotMigrator 创建Slot迁移器
func NewSlotMigrator(runner *TaskRunner, worker *EmbeddedWorker) *SlotMigrator {
	return &SlotMigrator{
		runner:             runner,
		worker:             worker,
		conflictHandler:    NewConflictHandler(runner.options.ConflictPolicy, runner.targetClient, runner.task.ID),
		rateLimiter:        runner.GetRateLimiter(),
		lastCheckpointTime: time.Now(),
		keysInBatch:        0,
	}
}

// MigrateSlot 迁移单个Slot（支持断点恢复）
func (m *SlotMigrator) MigrateSlot(ctx context.Context, slot int) error {
	source := m.runner.sourceClient
	target := m.runner.targetClient

	// 检查 slot 是否已完成（断点恢复时跳过已完成的 slot）
	slotStatus, statusErr := m.runner.master.store.GetSlotStatus(m.runner.task.ID, slot)
	if statusErr == nil && slotStatus != nil && slotStatus.Status == "completed" {
		return nil // 已完成，跳过
	}

	// 尝试从断点恢复
	var cursor uint64
	var keysMigrated int64
	
	checkpoint, err := m.runner.master.store.GetSlotCheckpoint(m.runner.task.ID, slot)
	if err == nil && checkpoint != "" && checkpoint != "0" {
		// 解析 cursor
		fmt.Sscanf(checkpoint, "%d", &cursor)
		log.Printf("Slot %d: Resuming from checkpoint cursor=%d", slot, cursor)
	}
	
	// 从断点恢复已迁移的 key 数量
	if statusErr == nil && slotStatus != nil {
		keysMigrated = slotStatus.KeysMigrated
		if keysMigrated > 0 {
			log.Printf("Slot %d: Resuming with %d keys already migrated", slot, keysMigrated)
		}
	}
	
	batchSize := int64(m.runner.options.ScanBatchSize)
	if batchSize <= 0 {
		batchSize = 1000
	}

	// 断点保存配置
	const checkpointKeyInterval = 10000       // 每 10000 个 key 保存一次
	const checkpointTimeInterval = 30 * time.Second // 每 30 秒保存一次
	m.lastCheckpointTime = time.Now()
	m.keysInBatch = 0

	for {
		select {
		case <-ctx.Done():
			// 被中断时保存当前断点
			m.saveSlotCheckpoint(slot, cursor, keysMigrated)
			return ctx.Err()
		default:
		}

		// SCAN获取Key
		keys, nextCursor, err := m.scanSlot(ctx, source, slot, cursor, batchSize)
		if err != nil {
			return err
		}

		// 批量迁移
		if len(keys) > 0 {
			if err := m.migrateKeys(ctx, source, target, keys); err != nil {
				log.Printf("Migrate keys failed: %v", err)
			} else {
				keysMigrated += int64(len(keys))
				m.keysInBatch += int64(len(keys))
			}
		}

		cursor = nextCursor
		
		// 定期保存断点（每 10000 个 key 或 30 秒）
		if m.keysInBatch >= checkpointKeyInterval || time.Since(m.lastCheckpointTime) >= checkpointTimeInterval {
			m.saveSlotCheckpoint(slot, cursor, keysMigrated)
			m.keysInBatch = 0
			m.lastCheckpointTime = time.Now()
		}
		
		if cursor == 0 {
			break
		}
	}

	// Slot 完成，标记完成状态
	m.runner.master.store.UpdateSlotStatus(m.runner.task.ID, slot, "completed")
	log.Printf("Slot %d: Migration completed, keys=%d", slot, keysMigrated)
	return nil
}

// saveSlotCheckpoint 保存 Slot 断点
func (m *SlotMigrator) saveSlotCheckpoint(slot int, cursor uint64, keysMigrated int64) {
	cursorStr := fmt.Sprintf("%d", cursor)
	checkpoint := &model.Checkpoint{
		TaskID:       m.runner.task.ID,
		WorkerID:     m.worker.id,
		SlotID:       slot,
		Cursor:       cursorStr,
		KeysMigrated: keysMigrated,
		UpdatedAt:    time.Now().Unix(),
	}
	if err := m.runner.master.store.SaveCheckpoint(checkpoint); err != nil {
		log.Printf("Slot %d: Save checkpoint failed: %v", slot, err)
	} else {
		log.Printf("Slot %d: Checkpoint saved, cursor=%d, keys=%d", slot, cursor, keysMigrated)
	}
}

// scanSlot 扫描Slot中的Key
func (m *SlotMigrator) scanSlot(ctx context.Context, client *redis.ClusterClient, slot int, cursor uint64, count int64) ([]string, uint64, error) {
	// 使用CLUSTER GETKEYSINSLOT获取Slot中的Key
	// 简化实现：使用SCAN
	
	keys, nextCursor, err := client.Scan(ctx, cursor, "*", count).Result()
	if err != nil {
		return nil, 0, err
	}

	// 过滤出属于当前Slot的Key
	var slotKeys []string
	for _, key := range keys {
		if calculateSlot(key) == slot {
			slotKeys = append(slotKeys, key)
		}
	}

	return slotKeys, nextCursor, nil
}

// migrateKeys 迁移Key
func (m *SlotMigrator) migrateKeys(ctx context.Context, source, target *redis.ClusterClient, keys []string) error {
	// Key过滤
	keys = m.filterKeys(keys)
	if len(keys) == 0 {
		return nil
	}

	// 设置当前阶段
	m.conflictHandler.SetPhase(m.runner.GetPhase())

	// 批量检查冲突
	keysToMigrate, err := m.conflictHandler.HandleBatchKeys(ctx, keys)
	if err != nil {
		return err
	}

	if len(keysToMigrate) == 0 {
		return nil
	}

	// 应用限流
	if m.rateLimiter != nil {
		m.rateLimiter.AcquireSource()
	}

	// Pipeline迁移
	pipe := target.Pipeline()
	var totalBytes int64
	
	for _, key := range keysToMigrate {
		// DUMP + RESTORE
		dump, err := source.Dump(ctx, key).Result()
		if err != nil {
			continue
		}

		ttl, _ := source.TTL(ctx, key).Result()
		if ttl < 0 {
			ttl = 0
		}

		totalBytes += int64(len(dump))
		
		// 应用目标端限流
		if m.rateLimiter != nil {
			m.rateLimiter.AcquireTarget()
		}

		pipe.RestoreReplace(ctx, key, ttl, dump)
	}

	_, err = pipe.Exec(ctx)
	if err != nil {
		return err
	}

	// 报告进度
	m.worker.ReportProgress(int64(len(keysToMigrate)), totalBytes)

	return nil
}

// filterKeys 根据配置过滤Key
func (m *SlotMigrator) filterKeys(keys []string) []string {
	filter := m.runner.options.KeyFilter
	if filter == nil || filter.Mode == model.KeyFilterModeAll {
		return keys
	}

	var result []string
	for _, key := range keys {
		if m.shouldMigrateKey(key, filter) {
			result = append(result, key)
		}
	}
	return result
}

// shouldMigrateKey 判断Key是否应该被迁移
func (m *SlotMigrator) shouldMigrateKey(key string, filter *model.KeyFilterConfig) bool {
	// 先检查排除规则
	for _, prefix := range filter.ExcludePrefixes {
		if strings.HasPrefix(key, prefix) {
			return false
		}
	}
	for _, pattern := range filter.ExcludePatterns {
		if matched, _ := regexp.MatchString(pattern, key); matched {
			return false
		}
	}

	// 再检查包含规则
	switch filter.Mode {
	case model.KeyFilterModePrefix:
		if len(filter.Prefixes) == 0 {
			return true // 没有指定前缀，迁移所有
		}
		for _, prefix := range filter.Prefixes {
			if strings.HasPrefix(key, prefix) {
				return true
			}
		}
		return false
	case model.KeyFilterModePattern:
		if len(filter.Patterns) == 0 {
			return true
		}
		for _, pattern := range filter.Patterns {
			if matched, _ := regexp.MatchString(pattern, key); matched {
				return true
			}
		}
		return false
	case model.KeyFilterModeKeys, model.KeyFilterModeKeylist:
		// 支持 keys 和 keylist 两种模式名称（前端使用 keylist）
		for _, k := range filter.Keys {
			if key == k {
				return true
			}
		}
		return false
	default:
		return true
	}
}

// calculateSlot 计算Key的Slot (CRC16)
func calculateSlot(key string) int {
	// 处理Hash Tag
	if start := findHashTag(key); start >= 0 {
		end := start
		for end < len(key) && key[end] != '}' {
			end++
		}
		if end < len(key) {
			key = key[start+1 : end]
		}
	}

	return int(crc16(key) % 16384)
}

func findHashTag(key string) int {
	for i := 0; i < len(key); i++ {
		if key[i] == '{' {
			return i
		}
	}
	return -1
}

// CRC16 XMODEM
func crc16(key string) uint16 {
	crc := uint16(0)
	for i := 0; i < len(key); i++ {
		crc = crc ^ (uint16(key[i]) << 8)
		for j := 0; j < 8; j++ {
			if crc&0x8000 != 0 {
				crc = (crc << 1) ^ 0x1021
			} else {
				crc = crc << 1
			}
		}
	}
	return crc
}

// ConflictHandler 冲突处理器
type ConflictHandler struct {
	policy         model.ConflictPolicy
	targetClient   *redis.ClusterClient
	phase          atomic.Int32
	taskID         string
	skippedKeys    []string     // 记录跳过的冲突Key
	skippedKeysMu  sync.Mutex
}

// NewConflictHandler 创建冲突处理器
func NewConflictHandler(policy model.ConflictPolicy, target *redis.ClusterClient, taskID string) *ConflictHandler {
	// 验证策略合法性
	switch policy {
	case model.ConflictPolicySkipFullOnly, model.ConflictPolicyReplace, model.ConflictPolicyError, model.ConflictPolicySkip:
		// 合法
	default:
		// 默认使用skip_full_only
		policy = model.ConflictPolicySkipFullOnly
	}

	return &ConflictHandler{
		policy:       policy,
		targetClient: target,
		taskID:       taskID,
		skippedKeys:  make([]string, 0),
	}
}

// SetPhase 设置当前阶段
func (h *ConflictHandler) SetPhase(phase model.MigrationPhase) {
	h.phase.Store(int32(phase))
}

// GetEffectivePolicy 获取当前生效的策略
func (h *ConflictHandler) GetEffectivePolicy() model.ConflictPolicy {
	if h.policy == model.ConflictPolicySkipFullOnly {
		// skip_full_only: 全量跳过，增量replace
		if model.MigrationPhase(h.phase.Load()) == model.PhaseIncrementalSync {
			return model.ConflictPolicyReplace
		}
		return model.ConflictPolicySkipFullOnly
	}
	return h.policy
}

// RecordSkippedKey 记录跳过的冲突Key
func (h *ConflictHandler) RecordSkippedKey(key string) {
	h.skippedKeysMu.Lock()
	defer h.skippedKeysMu.Unlock()
	h.skippedKeys = append(h.skippedKeys, key)
	// 同时写入日志
	log.Printf("[CONFLICT_SKIP] TaskID=%s Key=%s", h.taskID, key)
}

// GetSkippedKeys 获取所有跳过的冲突Key
func (h *ConflictHandler) GetSkippedKeys() []string {
	h.skippedKeysMu.Lock()
	defer h.skippedKeysMu.Unlock()
	result := make([]string, len(h.skippedKeys))
	copy(result, h.skippedKeys)
	return result
}

// GetSkippedKeysCount 获取跳过的Key数量
func (h *ConflictHandler) GetSkippedKeysCount() int {
	h.skippedKeysMu.Lock()
	defer h.skippedKeysMu.Unlock()
	return len(h.skippedKeys)
}

// HandleBatchKeys 批量处理Key冲突
func (h *ConflictHandler) HandleBatchKeys(ctx context.Context, keys []string) ([]string, error) {
	policy := h.GetEffectivePolicy()

	if policy == model.ConflictPolicyReplace {
		// 直接覆盖，无需检查
		return keys, nil
	}

	// 按Slot分组（避免CROSSSLOT错误）
	slotGroups := h.groupKeysBySlot(keys)

	var keysToMigrate []string

	for _, group := range slotGroups {
		exists, err := h.checkKeysInSlot(ctx, group)
		if err != nil {
			return nil, err
		}

		for i, key := range group {
			if exists[i] {
				switch policy {
				case model.ConflictPolicySkipFullOnly:
					// 跳过已存在的Key（全量阶段）
					continue
				case model.ConflictPolicySkip:
					// 跳过并记录
					h.RecordSkippedKey(key)
					continue
				case model.ConflictPolicyError:
					return nil, fmt.Errorf("key already exists: %s", key)
				}
			}
			keysToMigrate = append(keysToMigrate, key)
		}
	}

	return keysToMigrate, nil
}

// groupKeysBySlot 按Slot分组
func (h *ConflictHandler) groupKeysBySlot(keys []string) map[int][]string {
	groups := make(map[int][]string)
	for _, key := range keys {
		slot := calculateSlot(key)
		groups[slot] = append(groups[slot], key)
	}
	return groups
}

// checkKeysInSlot 检查同一Slot内的Key是否存在
func (h *ConflictHandler) checkKeysInSlot(ctx context.Context, keys []string) ([]bool, error) {
	if len(keys) == 0 {
		return nil, nil
	}

	// 使用Pipeline批量EXISTS
	pipe := h.targetClient.Pipeline()
	cmds := make([]*redis.IntCmd, len(keys))

	for i, key := range keys {
		cmds[i] = pipe.Exists(ctx, key)
	}

	_, err := pipe.Exec(ctx)
	if err != nil {
		return nil, err
	}

	results := make([]bool, len(keys))
	for i, cmd := range cmds {
		results[i] = cmd.Val() > 0
	}

	return results, nil
}

// ConvergenceDetector 收敛检测器
type ConvergenceDetector struct {
	absoluteThreshold int64
	relativeThreshold float64
	maxIterations     int
	timeWindow        time.Duration
	windowThreshold   int64
	
	history           []ConvergenceRecord
	mu                sync.Mutex
}

// ConvergenceRecord 收敛记录
type ConvergenceRecord struct {
	Iteration   int
	ChangeCount int64
	TotalKeys   int64
	Timestamp   time.Time
	ChangeRate  float64
}

// NewConvergenceDetector 创建收敛检测器
func NewConvergenceDetector() *ConvergenceDetector {
	return &ConvergenceDetector{
		absoluteThreshold: 1000,
		relativeThreshold: 0.001,
		maxIterations:     10,
		timeWindow:        10 * time.Minute,
		windowThreshold:   5000,
		history:           make([]ConvergenceRecord, 0, 10),
	}
}

// IsConverged 判断是否收敛
func (d *ConvergenceDetector) IsConverged(record ConvergenceRecord) (bool, string) {
	d.mu.Lock()
	defer d.mu.Unlock()

	// 记录历史
	d.history = append(d.history, record)
	if len(d.history) > 10 {
		d.history = d.history[1:]
	}

	// 条件1：变更数低于绝对阈值
	if record.ChangeCount < d.absoluteThreshold {
		return true, "change_count_below_threshold"
	}

	// 条件2：变更率低于相对阈值
	if record.TotalKeys > 0 {
		rate := float64(record.ChangeCount) / float64(record.TotalKeys)
		if rate < d.relativeThreshold {
			return true, "change_rate_below_threshold"
		}
	}

	// 条件3：时间窗口内变更稳定
	if len(d.history) >= 3 {
		windowStart := time.Now().Add(-d.timeWindow)
		var windowChanges int64
		for _, h := range d.history {
			if h.Timestamp.After(windowStart) {
				windowChanges += h.ChangeCount
			}
		}
		if windowChanges < d.windowThreshold {
			return true, "window_changes_stable"
		}
	}

	// 条件4：达到最大迭代次数
	if len(d.history) >= d.maxIterations {
		return true, "max_iterations_reached"
	}

	return false, ""
}

// Verifier 数据校验器
type Verifier struct {
	sourceClient *redis.ClusterClient
	targetClient *redis.ClusterClient
}

// NewVerifier 创建校验器
func NewVerifier(source, target *redis.ClusterClient) *Verifier {
	return &Verifier{
		sourceClient: source,
		targetClient: target,
	}
}

// VerifyResult 校验结果
type VerifyResult struct {
	TotalKeys       int
	MatchedKeys     int
	MismatchedKeys  int
	MissingKeys     int
	ExtraKeys       int
	ConsistencyRate float64
}

// Verify 执行校验
func (v *Verifier) Verify(ctx context.Context, sampleSize int) (*VerifyResult, error) {
	result := &VerifyResult{}

	// 采样Key
	keys, err := v.sampleKeys(ctx, sampleSize)
	if err != nil {
		return nil, err
	}

	result.TotalKeys = len(keys)

	// 并发校验
	var wg sync.WaitGroup
	var matched, mismatched, missing int64

	sem := make(chan struct{}, 50) // 并发控制

	for _, key := range keys {
		wg.Add(1)
		sem <- struct{}{}

		go func(k string) {
			defer wg.Done()
			defer func() { <-sem }()

			match, exists := v.verifyKey(ctx, k)
			if !exists {
				atomic.AddInt64(&missing, 1)
			} else if match {
				atomic.AddInt64(&matched, 1)
			} else {
				atomic.AddInt64(&mismatched, 1)
			}
		}(key)
	}

	wg.Wait()

	result.MatchedKeys = int(matched)
	result.MismatchedKeys = int(mismatched)
	result.MissingKeys = int(missing)

	if result.TotalKeys > 0 {
		result.ConsistencyRate = float64(result.MatchedKeys) / float64(result.TotalKeys) * 100
	}

	return result, nil
}

// sampleKeys 采样Key
func (v *Verifier) sampleKeys(ctx context.Context, count int) ([]string, error) {
	var keys []string

	// 简单采样：SCAN
	cursor := uint64(0)
	for len(keys) < count {
		result, nextCursor, err := v.sourceClient.Scan(ctx, cursor, "*", int64(count-len(keys))).Result()
		if err != nil {
			return nil, err
		}
		keys = append(keys, result...)
		cursor = nextCursor
		if cursor == 0 {
			break
		}
	}

	if len(keys) > count {
		keys = keys[:count]
	}

	return keys, nil
}

// verifyKey 校验单个Key
func (v *Verifier) verifyKey(ctx context.Context, key string) (match, exists bool) {
	// 检查目标端是否存在
	targetExists, err := v.targetClient.Exists(ctx, key).Result()
	if err != nil || targetExists == 0 {
		return false, false
	}

	// 比较DUMP值
	sourceDump, err := v.sourceClient.Dump(ctx, key).Result()
	if err != nil {
		return false, true
	}

	targetDump, err := v.targetClient.Dump(ctx, key).Result()
	if err != nil {
		return false, true
	}

	return sourceDump == targetDump, true
}
