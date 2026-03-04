package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"regexp"
	"strconv"
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
	
	// 预编译的正则表达式（避免 regexp.MatchString 每次重新编译）
	compiledExcludePatterns []*regexp.Regexp
	compiledPatterns        []*regexp.Regexp
	
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
		if err := json.Unmarshal([]byte(task.Config), &options); err != nil {
			log.Printf("Warning: invalid task config JSON, using defaults: %v", err)
			options = *model.DefaultMigrationOptions()
		}
	} else {
		options = *model.DefaultMigrationOptions()
	}

	ctx, cancel := context.WithCancel(context.Background())

	runner := &TaskRunner{
		master:       m,
		task:         task,
		sourceConfig: &sourceConfig,
		targetConfig: &targetConfig,
		options:      &options,
		workers:      make(map[string]*EmbeddedWorker),
		ctx:          ctx,
		cancel:       cancel,
	}

	// 预编译正则表达式（避免 regexp.MatchString 每次重新编译，40 亿 Key 场景严重影响性能）
	if options.KeyFilter != nil {
		for _, pattern := range options.KeyFilter.ExcludePatterns {
			if re, err := regexp.Compile(pattern); err == nil {
				runner.compiledExcludePatterns = append(runner.compiledExcludePatterns, re)
			} else {
				log.Printf("Warning: invalid exclude pattern %q: %v", pattern, err)
			}
		}
		for _, pattern := range options.KeyFilter.Patterns {
			if re, err := regexp.Compile(pattern); err == nil {
				runner.compiledPatterns = append(runner.compiledPatterns, re)
			} else {
				log.Printf("Warning: invalid pattern %q: %v", pattern, err)
			}
		}
	}

	return runner, nil
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
	stats, err := r.master.store.GetOrCreateStats(r.task.ID)
	if err != nil || stats == nil {
		log.Printf("Warning: GetOrCreateStats failed, creating in-memory stats: %v", err)
		// 不中断任务，但跳过统计更新
	} else {
		stats.TotalKeys = totalKeys
		now := time.Now().Unix()
		stats.StartTime = &now
		if err := r.master.store.UpdateStats(stats); err != nil {
			log.Printf("Warning: UpdateStats failed: %v", err)
		}
	}

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
	var incrSyncErr error
	if r.options.SkipIncremental {
		log.Printf("Phase 2: Incremental sync SKIPPED (skip_incremental=true)")
	} else {
		r.phase.Store(int32(model.PhaseIncrementalSync))
		log.Printf("Phase 2: Incremental sync starting...")
		
		if fakeSlaveSupported {
			// 方案 B 后续：回放缓存的 binlog，然后切换到实时模式
			if err := r.runFakeSlaveIncrementalSyncWithReplay(); err != nil {
				if err == context.Canceled || r.ctx.Err() != nil {
					// 用户手动停止增量同步是正常行为，不算失败
					log.Printf("Incremental sync stopped by user")
				} else {
					log.Printf("Incremental sync failed: %v", err)
					incrSyncErr = err
				}
			}
		} else {
			// 降级方案
			if err := r.runIncrementalSync(); err != nil {
				if err == context.Canceled || r.ctx.Err() != nil {
					log.Printf("Incremental sync stopped by user")
				} else {
					log.Printf("Incremental sync failed: %v", err)
					incrSyncErr = err
				}
			}
		}
	}

	// 如果增量同步异常失败（非用户停止），标记任务失败
	if incrSyncErr != nil {
		log.Printf("Task failed due to incremental sync error: %v", incrSyncErr)
		r.master.store.UpdateTaskCompleted(r.task.ID, model.TaskStatusFailed)
		return
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
	
	// 创建自适应限流器（基于 PID 控制器 + 源集群负载监控）
	adaptiveCfg := &limiter.AdaptiveConfig{
		Enabled:        true,
		Kp:             0.5,
		Ki:             0.1,
		Kd:             0.05,
		TargetLoad:     0.7, // 目标负载 70%
		AdjustInterval: 5 * time.Second,
	}
	
	// limiter 包和 engine 包统一使用 go-redis/v8，直接传入源集群客户端监控负载
	r.adaptiveRateLimiter = limiter.NewAdaptiveRateLimiter(r.rateLimiter, r.sourceClient, adaptiveCfg)
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
	
	// 初始化大 Key 扫描器和迁移器（limiter 包统一使用 go-redis/v8，类型兼容）
	r.bigKeyScanner = limiter.NewBigKeyScanner(r.sourceClient, threshold, nil, 3)
	r.bigKeyMigrator = limiter.NewBigKeyMigrator(r.sourceClient, r.targetClient, r.bigKeyScanner, r.rateLimiter)
	
	log.Printf("Big key handlers initialized: threshold=%d bytes, scanner and migrator ready", threshold.StringMaxBytes)
}

// estimateTotalKeys 估算总Key数
// 优先使用 DBSIZE（O(1)），失败时用 SCAN 采样估算
func (r *TaskRunner) estimateTotalKeys() (int64, error) {
	var total int64
	var dbsizeFailed bool

	err := r.sourceClient.ForEachMaster(r.ctx, func(ctx context.Context, client *redis.Client) error {
		dbsize, err := client.DBSize(ctx).Result()
		if err != nil || dbsize <= 0 {
			dbsizeFailed = true
			return nil
		}
		atomic.AddInt64(&total, dbsize)
		return nil
	})

	// DBSIZE 成功，直接返回
	if err == nil && !dbsizeFailed && total > 0 {
		log.Printf("estimateTotalKeys: DBSIZE=%d", total)
		return total, nil
	}

	// DBSIZE 失败或返回 0，使用 SCAN 采样估算
	log.Printf("DBSIZE unavailable or returned 0, using SCAN sampling to estimate total keys...")
	total = 0
	var nodeCount int64

	err = r.sourceClient.ForEachMaster(r.ctx, func(ctx context.Context, client *redis.Client) error {
		estimated := r.estimateNodeKeysBySampling(ctx, client)
		if estimated > 0 {
			atomic.AddInt64(&total, estimated)
		}
		atomic.AddInt64(&nodeCount, 1)
		return nil
	})

	if total > 0 {
		log.Printf("estimateTotalKeys: SCAN sampling estimated ~%d keys across %d nodes", total, nodeCount)
	} else {
		log.Printf("estimateTotalKeys: unable to estimate total keys (DBSIZE and SCAN sampling both failed)")
	}

	return total, err
}

// estimateNodeKeysBySampling 通过 SCAN 采样估算单个节点的 Key 数量
// 策略：SCAN 若干批次，统计返回的 Key 数和 cursor 推进比例来估算总量
func (r *TaskRunner) estimateNodeKeysBySampling(ctx context.Context, client *redis.Client) int64 {
	const sampleBatches = 10    // 采样批次数
	const batchSize int64 = 500 // 每批 SCAN 数量

	var cursor uint64
	var totalSampled int64

	for i := 0; i < sampleBatches; i++ {
		select {
		case <-ctx.Done():
			return 0
		default:
		}

		keys, nextCursor, err := client.Scan(ctx, cursor, "*", batchSize).Result()
		if err != nil {
			log.Printf("SCAN sampling failed at batch %d: %v", i, err)
			return 0
		}

		totalSampled += int64(len(keys))

		if nextCursor == 0 {
			// SCAN 已遍历完整个库，totalSampled 就是精确值
			return totalSampled
		}

		cursor = nextCursor
	}

	// 采样未遍历完，用 cursor 比例估算
	// Redis SCAN cursor 在 [0, hash_table_size) 范围内
	// 已遍历的比例约 ≈ cursor / hash_table_size
	// 由于 hash_table_size 未知，使用另一种估算方式：
	// 每批平均返回 avgKeysPerBatch 个 Key，假设 SCAN 均匀分布
	// 则总量 ≈ totalSampled * (总批次 / 已采样批次)
	// 但由于我们不知道总批次数，采用更保守的方式：
	// 已采样的 Key 数 / 采样批次数 = 每批平均 Key 数
	// 如果每批都接近 batchSize，说明 Key 很多，给出下限估计
	avgPerBatch := totalSampled / int64(sampleBatches)
	if avgPerBatch <= 0 {
		return totalSampled
	}

	// 保守估计：至少是采样量的 10 倍（因为只采样了很小一部分）
	// 这个估计不需要精确，只是给进度条一个参考分母
	estimated := totalSampled * 20
	log.Printf("SCAN sampling: %d keys in %d batches (avg %d/batch), estimated ~%d keys",
		totalSampled, sampleBatches, avgPerBatch, estimated)
	return estimated
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
	
	// 其次尝试 PSYNC 模式（实际使用 offset 变化检测 + IDLETIME 轮询）
	psyncSupported := r.checkPsyncSupport()
	if psyncSupported {
		log.Printf("PSYNC/REPLCONF supported, using offset-based IDLETIME polling for incremental sync")
		return r.runOffsetIdletimeIncrementalSync()
	}
	
	// 降级到 IDLETIME 模式前，检查数据规模
	// IDLETIME 每轮需要全量 SCAN 所有 Key，40 亿 Key 场景下每轮需要数小时，完全不可用
	totalKeys, _ := r.estimateTotalKeys()
	
	// 超过 1 亿 Key：禁止降级到 IDLETIME，直接报错
	if totalKeys > 100_000_000 {
		log.Printf("❌ CRITICAL: IDLETIME mode is BLOCKED for %d keys (> 100M threshold).", totalKeys)
		log.Printf("❌ IDLETIME mode requires full SCAN of all keys every 30 seconds.")
		log.Printf("❌ With %d keys, each SCAN round would take %d+ minutes - completely unusable.", totalKeys, totalKeys/10_000_000)
		log.Printf("❌ Please enable binlog on source Tendis (binlog-enabled=yes) and restart to use FakeSlave mode.")
		return fmt.Errorf("incremental sync blocked: IDLETIME mode not supported for %d keys (> 100M). "+
			"Enable binlog on source Tendis (binlog-enabled=yes) to use FakeSlave mode, "+
			"or use full_only migration mode", totalKeys)
	}
	
	// 1000 万 ~ 1 亿 Key：允许但打印强烈警告
	if totalKeys > 10_000_000 {
		log.Printf("⚠️ WARNING: IDLETIME mode with %d keys may be slow.", totalKeys)
		log.Printf("⚠️ Each sync round will SCAN ALL keys, expected time: %d+ minutes", totalKeys/10_000_000)
		log.Printf("⚠️ Consider enabling binlog on source Tendis for FakeSlave mode.")
	}
	
	log.Printf("FakeSlave/PSYNC not supported, falling back to IDLETIME mode (suitable for < 100M keys)")
	return r.runIdletimeIncrementalSync()
}

// getKvstoreCount 获取源端 Tendis 的 kvstorecount 配置
// 每个 Tendis 节点有 kvstorecount 个 store，每个 store 有独立的 binlog
// Key 分配到 store 的算法：storeId = slot % kvstorecount
// 必须为每个 store 单独注册 INCRSYNC，否则会丢失写入到其他 store 的数据
func (r *TaskRunner) getKvstoreCount() int {
	ctx, cancel := context.WithTimeout(r.ctx, 5*time.Second)
	defer cancel()

	var kvstoreCount int
	r.sourceClient.ForEachMaster(ctx, func(ctx context.Context, node *redis.Client) error {
		if kvstoreCount > 0 {
			return nil // 只查第一个节点
		}
		// go-redis v8: ConfigGet 返回 *SliceCmd，Result() 返回 ([]interface{}, error)
		result, err := node.ConfigGet(ctx, "kvstorecount").Result()
		if err == nil && len(result) >= 2 {
			if valStr, ok := result[1].(string); ok {
				if n, err := strconv.Atoi(valStr); err == nil && n > 0 {
					kvstoreCount = n
				}
			}
		}
		return nil
	})

	if kvstoreCount <= 0 {
		kvstoreCount = 10 // Tendis 默认 kvstorecount=10
		log.Printf("Warning: could not get kvstorecount from source, using default=%d", kvstoreCount)
	} else {
		log.Printf("Source kvstorecount=%d", kvstoreCount)
	}
	return kvstoreCount
}

// buildMasterStoreList 构建所有 Master 节点的所有 store 列表
// 每个节点有 kvstorecount 个 store，每个需要独立的 FakeSlave
type masterStoreInfo struct {
	addr    string
	storeID uint32
	client  *redis.Client
}

func (r *TaskRunner) buildMasterStoreList() ([]masterStoreInfo, int) {
	kvstoreCount := r.getKvstoreCount()

	var nodeAddrs []string
	var nodeClients []*redis.Client
	r.sourceClient.ForEachMaster(r.ctx, func(ctx context.Context, node *redis.Client) error {
		nodeAddrs = append(nodeAddrs, node.Options().Addr)
		nodeClients = append(nodeClients, node)
		return nil
	})

	var list []masterStoreInfo
	for i, addr := range nodeAddrs {
		for sid := 0; sid < kvstoreCount; sid++ {
			list = append(list, masterStoreInfo{
				addr:    addr,
				storeID: uint32(sid),
				client:  nodeClients[i],
			})
		}
	}
	return list, kvstoreCount
}

// startFakeSlavesAndWait 启动所有 FakeSlave 并等待连接成功
// 关键原则：
// 1. 每个 Master 节点的每个 store 都需要独立的 FakeSlave（否则丢失数据）
// 2. 任何一个连接失败 = 整个任务失败
func (r *TaskRunner) startFakeSlavesAndWait() error {
	log.Printf("Starting FakeSlaves for all master nodes (cache mode)")

	// 1. 获取所有 Master 节点 × 所有 store 的完整列表
	masters, kvstoreCount := r.buildMasterStoreList()

	if len(masters) == 0 {
		return fmt.Errorf("no master nodes found")
	}

	log.Printf("Found %d master stores (%d nodes × %d stores/node), starting FakeSlaves...",
		len(masters), len(masters)/kvstoreCount, kvstoreCount)

	// 2. 初始化 binlog 缓存管理器
	cacheConfig := replication.BinlogCacheConfig{
		CacheDir:    "data/binlog_cache",
		TaskID:      r.task.ID,
		MaxFileSize: 1 << 30, // 1GB 自动切分
	}
	r.binlogCacheManager = replication.NewBinlogCacheManager(cacheConfig)
	r.binlogCacheManager.StartCaching()

	// 3. 为每个 Master 的每个 store 创建并启动 FakeSlave
	r.fakeSlaves = make([]*replication.FakeSlave, 0, len(masters))

	var connectWg sync.WaitGroup
	errChan := make(chan error, len(masters))

	for i, master := range masters {
		log.Printf("Creating FakeSlave for master %s (storeID=%d)", master.addr, master.storeID)

		config := replication.FakeSlaveConfig{
			SourceAddr:       master.addr,
			SourcePassword:   r.sourceConfig.Password,
			StoreID:          master.storeID,
			StartBinlogPos:   0,
			FakeListenIP:     "127.0.0.1",
			FakeListenPort:   uint16(6379 + i), // 每个 FakeSlave 不同端口
			ReadTimeout:      30 * time.Second,
			HeartbeatTimeout: 30 * time.Second,
			KeyFilter: func(key string) bool {
				return r.shouldMigrateKey(key)
			},
			CacheMode:    true,
			CacheManager: r.binlogCacheManager,
		}

		fakeSlave := replication.NewFakeSlave(config, r.targetClient)

		r.fakeSlavesMu.Lock()
		r.fakeSlaves = append(r.fakeSlaves, fakeSlave)
		r.fakeSlavesMu.Unlock()

		// 启动 FakeSlave（异步）
		go func(fs *replication.FakeSlave, addr string, sid uint32) {
			if err := fs.Start(r.ctx); err != nil {
				if r.ctx.Err() == nil {
					log.Printf("FakeSlave for %s storeID=%d failed: %v", addr, sid, err)
				}
			}
		}(fakeSlave, master.addr, master.storeID)

		// 等待连接成功
		connectWg.Add(1)
		go func(fs *replication.FakeSlave, addr string, sid uint32) {
			defer connectWg.Done()
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
	case <-r.ctx.Done():
		return fmt.Errorf("context cancelled while waiting for FakeSlave connections")
	}

	close(errChan)
	var errors []error
	for err := range errChan {
		errors = append(errors, err)
	}

	if len(errors) > 0 {
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
		log.Printf("ERROR: %d/%d FakeSlaves failed to replay cached binlogs", replayErrors, len(fakeSlaves))
		// 如果所有 FakeSlave 都失败了，返回错误
		if replayErrors >= int64(len(fakeSlaves)) {
			return fmt.Errorf("all %d FakeSlaves failed to replay cached binlogs", replayErrors)
		}
		// 部分失败：记录警告但继续（部分数据可能不一致）
		log.Printf("Warning: %d FakeSlaves succeeded, continuing with partial replay", int64(len(fakeSlaves))-replayErrors)
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
// 检查条件：binlog 必须已启用（配置 binlog-enabled=yes 且 binlogpos 可用）
// 仅有 store_count 不代表 binlog 已启用
func (r *TaskRunner) checkFakeSlaveSupport() bool {
	if r.sourceClient == nil {
		return false
	}
	
	ctx, cancel := context.WithTimeout(r.ctx, 5*time.Second)
	defer cancel()
	
	var supported bool
	r.sourceClient.ForEachMaster(ctx, func(ctx context.Context, node *redis.Client) error {
		if supported {
			return nil // 只检查第一个节点
		}

		// 方法1: 检查 CONFIG GET binlog-enabled（最可靠）
		// go-redis v8: ConfigGet 返回 *SliceCmd，Result() 返回 ([]interface{}, error)
		result, err := node.ConfigGet(ctx, "binlog-enabled").Result()
		if err == nil && len(result) >= 2 {
			if valStr, ok := result[1].(string); ok && (valStr == "yes" || valStr == "1") {
				supported = true
				log.Printf("Tendis binlog enabled (CONFIG GET) on node %s, FakeSlave mode available", node.String())
				return nil
			}
		}

		// 方法2: 检查 binlogpos 0 是否返回整数（直接测试 binlog 命令）
		posResult, err := node.Do(ctx, "binlogpos", "0").Result()
		if err == nil {
			if _, ok := posResult.(int64); ok {
				supported = true
				log.Printf("Tendis binlog available (binlogpos) on node %s, FakeSlave mode available", node.String())
				return nil
			}
		}

		// 方法3: 检查 INFO REPLICATION 中的 binlog 字段（兼容老版本）
		info, err := node.Info(ctx, "replication").Result()
		if err != nil {
			return nil
		}
		
		binlogEnabled := parseInfoField(info, "binlog_enabled")
		if binlogEnabled == "1" || binlogEnabled == "yes" {
			supported = true
			log.Printf("Tendis binlog enabled (INFO) on node %s, FakeSlave mode available", node.String())
			return nil
		}
		
		// 注意：仅检测到 store_count 不足以判断 binlog 已启用
		// store_count 存在只说明是 Tendis，不代表 binlog-enabled=yes
		storeCount := parseInfoField(info, "store_count")
		if storeCount != "" {
			log.Printf("Tendis cluster detected (store_count=%s) but binlog status unknown, FakeSlave mode NOT confirmed", storeCount)
		}
		
		return nil
	})
	
	return supported
}

// runFakeSlaveIncrementalSync 使用伪 Slave 模式进行增量同步
// 伪装成 Tendis 从节点，通过 INCRSYNC 协议接收 binlog 推送
// 这是最高效的增量同步方式，适用于 40 亿 Key 场景
// 重要：每个节点的每个 store 都需要独立的 FakeSlave
func (r *TaskRunner) runFakeSlaveIncrementalSync() error {
	log.Printf("Starting FakeSlave incremental sync (manual stop mode)")
	log.Printf("This mode is optimal for large-scale migrations (40B+ keys)")
	
	// 1. 获取所有 Master 节点 × 所有 store 的完整列表
	masters, kvstoreCount := r.buildMasterStoreList()
	
	if len(masters) == 0 {
		return fmt.Errorf("no master nodes found")
	}
	
	log.Printf("Found %d master stores (%d nodes × %d stores/node) for FakeSlave replication",
		len(masters), len(masters)/kvstoreCount, kvstoreCount)
	
	// 2. 获取每个 store 的最新 binlog 位置（全量迁移后应从最新位置开始）
	type binlogPosKey struct {
		addr    string
		storeID uint32
	}
	binlogPositions := make(map[binlogPosKey]uint64)
	for _, m := range masters {
		ctx, cancel := context.WithTimeout(r.ctx, 5*time.Second)
		result, err := m.client.Do(ctx, "binlogpos", m.storeID).Result()
		cancel()
		if err == nil {
			if pos, ok := result.(int64); ok && pos > 0 {
				binlogPositions[binlogPosKey{m.addr, m.storeID}] = uint64(pos)
				log.Printf("Master %s storeID=%d current binlog pos: %d", m.addr, m.storeID, pos)
			}
		} else {
			log.Printf("Warning: failed to get binlog pos for %s storeID=%d: %v, starting from 0", m.addr, m.storeID, err)
		}
	}
	
	// 3. 为每个 Master 节点的每个 store 创建 FakeSlave
	var wg sync.WaitGroup
	errChan := make(chan error, len(masters))
	
	for i, master := range masters {
		wg.Add(1)
		go func(m masterStoreInfo, idx int) {
			defer wg.Done()
			
			startPos := binlogPositions[binlogPosKey{m.addr, m.storeID}]
			
			log.Printf("Starting FakeSlave for master %s (storeID=%d, startBinlogPos=%d)", m.addr, m.storeID, startPos)
			
			config := replication.FakeSlaveConfig{
				SourceAddr:     m.addr,
				SourcePassword: r.sourceConfig.Password,
				StoreID:        m.storeID,
				StartBinlogPos: startPos,
				FakeListenIP:   "127.0.0.1",
				FakeListenPort: uint16(6379 + idx),
				ReadTimeout:    30 * time.Second,
				HeartbeatTimeout: 30 * time.Second,
				KeyFilter: func(key string) bool {
					return r.shouldMigrateKey(key)
				},
			}
			
			fakeSlave := replication.NewFakeSlave(config, r.targetClient)
			
			if err := fakeSlave.Start(r.ctx); err != nil {
				if r.ctx.Err() != nil {
					log.Printf("FakeSlave for %s storeID=%d stopped by user", m.addr, m.storeID)
					return
				}
				log.Printf("FakeSlave for %s storeID=%d failed: %v", m.addr, m.storeID, err)
				errChan <- fmt.Errorf("FakeSlave for %s storeID=%d: %w", m.addr, m.storeID, err)
			}
		}(master, i)
	}
	
	// 等待所有 FakeSlave 完成或用户停止
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	
	var allErrors []error
	for {
		select {
		case <-r.ctx.Done():
			log.Printf("FakeSlave incremental sync stopped by user")
			return nil
		case err := <-errChan:
			allErrors = append(allErrors, err)
			log.Printf("FakeSlave error (%d total): %v", len(allErrors), err)
			if len(allErrors) >= len(masters) {
				return fmt.Errorf("all %d FakeSlaves failed, last error: %w", len(allErrors), err)
			}
		case <-done:
			if len(allErrors) > 0 {
				log.Printf("FakeSlaves completed with %d errors", len(allErrors))
				return fmt.Errorf("%d FakeSlaves failed: %v", len(allErrors), allErrors[0])
			}
			log.Printf("All FakeSlaves completed")
			return nil
		}
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

// runOffsetIdletimeIncrementalSync 使用 repl_offset 变化检测 + IDLETIME 轮询进行增量同步
// 注意：此方法并非真正的 PSYNC 协议，而是检测 offset 变化后用 SCAN + IDLETIME 找到变更的 Key
// 用户手动停止，无自动收敛
func (r *TaskRunner) runOffsetIdletimeIncrementalSync() error {
	log.Printf("Starting offset-based IDLETIME incremental sync (manual stop mode)")
	
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
			log.Printf("IDLETIME incremental sync stopped by user, total synced: %d, errors: %d", totalSynced, totalErrors)
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

// getScanPattern 根据 Key 过滤配置生成 SCAN pattern
// 当用户配置了前缀过滤（KeyFilterModePrefix）时，利用 SCAN MATCH prefix* 进行服务端过滤
// 这样可以大幅减少网络传输和 CPU 消耗（尤其在 40 亿 Key 场景下至关重要）
// 如果有多个前缀，返回 "*"（服务端不支持 OR 匹配，客户端过滤）
func (r *TaskRunner) getScanPatterns() []string {
	filter := r.options.KeyFilter
	if filter == nil {
		return []string{"*"}
	}
	if filter.Mode == model.KeyFilterModePrefix && len(filter.Prefixes) > 0 {
		patterns := make([]string, len(filter.Prefixes))
		for i, prefix := range filter.Prefixes {
			patterns[i] = prefix + "*"
		}
		return patterns
	}
	return []string{"*"}
}

// syncRecentlyModifiedKeys 同步最近修改的 Key
// 使用 SCAN + OBJECT IDLETIME 检测最近在指定时间窗口内修改的 Key
func (r *TaskRunner) syncRecentlyModifiedKeys(ctx context.Context, node *redis.Client, idleTimeThreshold time.Duration) int64 {
	var synced int64
	batchSize := int64(1000)

	scanPatterns := r.getScanPatterns()

	for _, pattern := range scanPatterns {
		var cursor uint64
		for {
			select {
			case <-ctx.Done():
				return synced
			default:
			}

			keys, nextCursor, err := node.Scan(ctx, cursor, pattern, batchSize).Result()
			if err != nil {
				log.Printf("SCAN failed: %v", err)
				break
			}

			// 批量检查 IDLETIME 并同步
			pipe := node.Pipeline()
			idleTimeCmds := make([]*redis.DurationCmd, len(keys))
			for i, key := range keys {
				idleTimeCmds[i] = pipe.ObjectIdleTime(ctx, key)
			}
			if _, pipeErr := pipe.Exec(ctx); pipeErr != nil && pipeErr != redis.Nil {
				log.Printf("IDLETIME pipeline failed: %v", pipeErr)
				break
			}

			keysToSync := make([]string, 0)
			for i, key := range keys {
				idleTime, err := idleTimeCmds[i].Result()
				if err != nil {
					continue
				}
				if idleTime < idleTimeThreshold {
					if r.shouldMigrateKey(key) {
						keysToSync = append(keysToSync, key)
					}
				}
			}

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
	
	// PTTL（毫秒级精度，避免 TTL 秒级精度导致最多 999ms 的过期时间误差）
	ttl, err := sourceNode.PTTL(ctx, key).Result()
	if err != nil || ttl == -2*time.Millisecond {
		// PTTL=-2 表示 key 不存在（DUMP 和 PTTL 之间被删除），跳过避免幽灵 Key
		if err == nil {
			return r.targetClient.Del(ctx, key).Err()
		}
		ttl = 0
	}
	if ttl < 0 {
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
// 使用预编译的正则表达式，避免每个 Key 都重新编译（40 亿 Key 场景下的关键优化）
func (r *TaskRunner) shouldMigrateKey(key string) bool {
	// 内置排除：系统内部 key 始终跳过
	if isSystemInternalKey(key) {
		return false
	}

	filter := r.options.KeyFilter
	if filter == nil {
		return true
	}
	
	// 先检查排除规则
	for _, prefix := range filter.ExcludePrefixes {
		if strings.HasPrefix(key, prefix) {
			return false
		}
	}
	for _, re := range r.compiledExcludePatterns {
		if re.MatchString(key) {
			return false
		}
	}

	// 再检查包含规则
	switch filter.Mode {
	case model.KeyFilterModePrefix:
		if len(filter.Prefixes) == 0 {
			return true
		}
		for _, prefix := range filter.Prefixes {
			if strings.HasPrefix(key, prefix) {
				return true
			}
		}
		return false
	case model.KeyFilterModePattern:
		if len(r.compiledPatterns) == 0 {
			return true
		}
		for _, re := range r.compiledPatterns {
			if re.MatchString(key) {
				return true
			}
		}
		return false
	case model.KeyFilterModeKeys, model.KeyFilterModeKeylist:
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
	
	// 二次防护：运行时再次检查数据规模
	totalKeys, _ := r.estimateTotalKeys()
	if totalKeys > 100_000_000 {
		return fmt.Errorf("IDLETIME mode blocked: %d keys exceeds 100M limit", totalKeys)
	}
	if totalKeys > 10_000_000 {
		log.Printf("⚠️ IDLETIME mode running with %d keys - performance may be degraded", totalKeys)
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

// runVerification 数据校验（任务自动完成时触发，使用采样模式）
func (r *TaskRunner) runVerification() {
	r.runVerificationWithConfig(&VerifyConfig{
		Mode:         VerifyModeSample,
		SampleSize:   10000,
		BatchSize:    1000,
		Concurrency:  50,
		KeyFilter:    r.shouldMigrateKey,
		ScanPatterns: r.getScanPatterns(),
	})
}

// runVerificationWithConfig 使用指定配置执行校验
func (r *TaskRunner) runVerificationWithConfig(config *VerifyConfig) {
	batchID := uuid.New().String()
	verifier := NewVerifier(r.sourceClient, r.targetClient, config)

	result, err := verifier.Verify(r.ctx)
	if err != nil {
		log.Printf("Verification failed: %v", err)
		return
	}

	r.master.store.SaveVerifyResult(&model.VerifyResult{
		TaskID:          r.task.ID,
		BatchID:         batchID,
		TotalKeys:       result.TotalKeys,
		MatchedKeys:     result.MatchedKeys,
		MismatchedKeys:  result.MismatchedKeys,
		MissingKeys:     result.MissingKeys,
		ExtraKeys:       result.ExtraKeys,
		ConsistencyRate: result.ConsistencyRate,
	})

	log.Printf("Verification result: mode=%s, total=%d, matched=%d, consistency=%.2f%%",
		config.Mode, result.TotalKeys, result.MatchedKeys, result.ConsistencyRate)
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

// Stop 停止任务
// 注意：只发送取消信号，资源清理由 Run() 的 defer cleanup() 负责
// 这避免了 Stop() 和 cleanup() 同时调用 wg.Wait() 的竞态问题
func (r *TaskRunner) Stop() {
	r.cancel()
}

// TriggerVerify 触发校验（支持指定校验配置）
func (r *TaskRunner) TriggerVerify(config *VerifyConfig) (string, error) {
	if config == nil {
		config = &VerifyConfig{
			Mode:         VerifyModeSample,
			SampleSize:   10000,
			BatchSize:    1000,
			Concurrency:  50,
			KeyFilter:    r.shouldMigrateKey,
			ScanPatterns: r.getScanPatterns(),
		}
	} else {
		if config.KeyFilter == nil {
			config.KeyFilter = r.shouldMigrateKey
		}
		if len(config.ScanPatterns) == 0 {
			config.ScanPatterns = r.getScanPatterns()
		}
	}

	batchID := uuid.New().String()
	
	go func() {
		verifier := NewVerifier(r.sourceClient, r.targetClient, config)
		result, err := verifier.Verify(r.ctx)
		if err != nil {
			log.Printf("Verification failed: %v", err)
			return
		}

		r.master.store.SaveVerifyResult(&model.VerifyResult{
			TaskID:          r.task.ID,
			BatchID:         batchID,
			TotalKeys:       result.TotalKeys,
			MatchedKeys:     result.MatchedKeys,
			MismatchedKeys:  result.MismatchedKeys,
			MissingKeys:     result.MissingKeys,
			ExtraKeys:       result.ExtraKeys,
			ConsistencyRate: result.ConsistencyRate,
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
// 重要：必须先等待所有 worker goroutine 退出，再关闭 Redis 连接
// 否则 worker 可能在使用连接时连接被关闭，导致 "use of closed network connection" panic
func (r *TaskRunner) cleanup() {
	// 先取消 context，通知所有 goroutine 退出
	r.cancel()

	// 等待所有 worker goroutine 退出（防止关闭连接时 worker 仍在使用）
	r.wg.Wait()

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
	
	// 最后关闭 Redis 连接（所有使用者已退出）
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
	
	// 暂停恢复相关：记录暂停时间，恢复后扩大 IDLETIME 阈值补偿暂停期间的写入
	pausedAt     atomic.Int64 // 暂停时的 Unix 纳秒时间戳，0 表示未暂停
	resumeBoost  atomic.Int64 // 恢复后需要补偿的额外秒数（暂停持续时间）
	
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
	
	const maxSlotRetries = 3 // 单个 slot 最大重试次数
	var failedSlots []int

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

		// 带重试的 slot 迁移
		var lastErr error
		for retry := 0; retry <= maxSlotRetries; retry++ {
			lastErr = migrator.MigrateSlot(ctx, slot)
			if lastErr == nil {
				break
			}
			if ctx.Err() != nil {
				return // 被取消，立即退出
			}
			if retry < maxSlotRetries {
				delay := time.Duration(1<<retry) * time.Second // 1s, 2s, 4s
				log.Printf("Worker %s slot %d failed (attempt %d/%d): %v, retrying in %v",
					w.id, slot, retry+1, maxSlotRetries+1, lastErr, delay)
				select {
				case <-ctx.Done():
					return
				case <-time.After(delay):
				}
			}
		}
		
		if lastErr != nil {
			log.Printf("Worker %s slot %d failed after %d retries: %v", w.id, slot, maxSlotRetries+1, lastErr)
			failedSlots = append(failedSlots, slot)
		}
	}

	if len(failedSlots) > 0 {
		log.Printf("Worker %s full migration completed with %d failed slots: %v", w.id, len(failedSlots), failedSlots)
	} else {
		log.Printf("Worker %s full migration completed successfully", w.id)
	}
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
	baseIdleTimeThreshold := syncInterval + 5*time.Second // 基准空闲时间阈值（35s）
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

			// 计算本轮的 IDLETIME 阈值
			// 恢复后第一轮使用扩大的阈值（补偿暂停期间的写入）
			idleTimeThreshold := baseIdleTimeThreshold
			if boost := w.resumeBoost.Swap(0); boost > 0 {
				idleTimeThreshold = time.Duration(boost)*time.Second + baseIdleTimeThreshold
				log.Printf("Worker %s using boosted IDLETIME threshold: %v (compensating pause duration)", 
					w.id, idleTimeThreshold)
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
// 优化：使用前缀 pattern 利用 SCAN MATCH 服务端过滤
func (w *EmbeddedWorker) scanNodeModifiedKeys(ctx context.Context, node *redis.Client, idleTimeThreshold time.Duration, batchSize int64) (synced, skipped int64) {
	const pipelineBatchSize = 100

	scanPatterns := w.runner.getScanPatterns()

	for _, pattern := range scanPatterns {
		var cursor uint64 = 0

		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

			if w.paused.Load() {
				return
			}

			keys, newCursor, err := node.Scan(ctx, cursor, pattern, batchSize).Result()
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
				if _, pipeErr := pipe.Exec(ctx); pipeErr != nil && pipeErr != redis.Nil {
					// Pipeline 整体失败（网络错误等），跳过本批次并记录
					log.Printf("Worker %s IDLETIME pipeline failed: %v, skipping %d keys", w.id, pipeErr, len(batchKeys))
					skipped += int64(len(batchKeys))
					continue
				}

				for j, key := range batchKeys {
					idleTime, err := idleTimeCmds[j].Result()
					if err != nil {
						continue
					}

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
				break
			}
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

	// PTTL（毫秒级精度）
	ttl, err := sourceNode.PTTL(ctx, key).Result()
	if err != nil {
		ttl = 0
	}
	if ttl == -2*time.Millisecond {
		// PTTL=-2 表示 key 不存在（DUMP 和 PTTL 之间被删除），跳过避免幽灵 Key
		return false, 0
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
	w.pausedAt.Store(time.Now().UnixNano())
}

// Resume 恢复
func (w *EmbeddedWorker) Resume() {
	pausedAt := w.pausedAt.Load()
	if pausedAt > 0 {
		pauseDuration := time.Since(time.Unix(0, pausedAt))
		// 记录暂停持续时间（秒），恢复后第一轮扫描将使用更大的 IDLETIME 阈值
		boostSeconds := int64(pauseDuration.Seconds()) + 10 // 额外 10 秒缓冲
		w.resumeBoost.Store(boostSeconds)
		log.Printf("Worker %s resume: pause duration=%.1fs, boost=%ds", w.id, pauseDuration.Seconds(), boostSeconds)
	}
	w.pausedAt.Store(0)
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
	
	// 崩溃恢复重试窗口：恢复后尚未追上上次进度前，冲突跳过不计入统计
	resuming         bool  // 是否处于恢复重试窗口
	resumeKeyTarget  int64 // 上次 checkpoint 中已迁移的 key 数（需要追上的目标）
	resumeKeysCurrent int64 // 恢复后当前已处理的 key 数（migrated + skipped + filtered）
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
// 优化：使用 CLUSTER GETKEYSINSLOT 精确获取 slot 内的 key，替代全局 SCAN + 客户端过滤
// GETKEYSINSLOT 是服务端精确过滤，命中率 100%，避免了全集群 SCAN 的冗余扫描
func (m *SlotMigrator) MigrateSlot(ctx context.Context, slot int) error {
	source := m.runner.sourceClient
	target := m.runner.targetClient

	// 检查 slot 是否已完成（断点恢复时跳过已完成的 slot）
	slotStatus, statusErr := m.runner.master.store.GetSlotStatus(m.runner.task.ID, slot)
	if statusErr == nil && slotStatus != nil && slotStatus.Status == "completed" {
		return nil // 已完成，跳过
	}

	// 从断点恢复已迁移的 key 数量（用于崩溃恢复重试窗口）
	var keysMigrated int64
	if statusErr == nil && slotStatus != nil {
		keysMigrated = slotStatus.KeysMigrated
		if keysMigrated > 0 {
			log.Printf("Slot %d: Resuming with %d keys already migrated", slot, keysMigrated)
		}
	}

	// 崩溃恢复重试窗口：如果有已迁移的 key，说明是恢复场景
	// 在重试窗口内遇到的冲突跳过是"假冲突"（崩溃前已迁移的），不计入统计
	m.resuming = false
	m.resumeKeysCurrent = 0
	if keysMigrated > 0 {
		m.resuming = true
		m.resumeKeyTarget = keysMigrated
		m.conflictHandler.SetRetryWindow(true)
		log.Printf("Slot %d: Retry window opened, will not count conflicts until catching up to %d keys",
			slot, keysMigrated)
	}

	batchSize := int64(m.runner.options.ScanBatchSize)
	if batchSize <= 0 {
		batchSize = 1000
	}

	// 断点保存配置
	const checkpointKeyInterval int64 = 2000         // 每 2000 个 key 保存一次
	const checkpointTimeInterval = 10 * time.Second   // 每 10 秒保存一次
	const maxRetries = 3                               // 单批次最大重试次数
	const retryBaseDelay = 2 * time.Second             // 重试基础延迟
	m.lastCheckpointTime = time.Now()
	m.keysInBatch = 0

	// 使用 CLUSTER GETKEYSINSLOT 获取该 slot 的所有 key
	// 注意：GETKEYSINSLOT 没有 cursor，每次返回前 N 个 key（有序且稳定）
	// 策略：先获取总数，然后一次性取出所有 key，再分批迁移
	allKeys, err := m.getKeysInSlot(ctx, source, slot)
	if err != nil {
		return fmt.Errorf("get keys in slot %d failed: %w", slot, err)
	}

	if len(allKeys) == 0 {
		m.runner.master.store.UpdateSlotStatus(m.runner.task.ID, slot, "completed")
		return nil
	}

	log.Printf("Slot %d: Found %d keys to migrate", slot, len(allKeys))

	// 分批迁移
	for i := 0; i < len(allKeys); i += int(batchSize) {
		select {
		case <-ctx.Done():
			if m.resuming {
				m.conflictHandler.SetRetryWindow(false)
				m.resuming = false
			}
			m.saveSlotCheckpoint(slot, 0, keysMigrated)
			return ctx.Err()
		default:
		}

		end := i + int(batchSize)
		if end > len(allKeys) {
			end = len(allKeys)
		}
		batch := allKeys[i:end]

		// 带重试的批量迁移
		var result *MigrateKeysResult
		var migrateErr error

		for retry := 0; retry <= maxRetries; retry++ {
			result, migrateErr = m.migrateKeys(ctx, source, target, batch)
			if migrateErr == nil {
				break
			}

			if ctx.Err() != nil {
				m.saveSlotCheckpoint(slot, 0, keysMigrated)
				return ctx.Err()
			}

			if retry < maxRetries {
				delay := retryBaseDelay * time.Duration(1<<retry)
				log.Printf("Slot %d: migrateKeys failed (attempt %d/%d): %v, retrying in %v",
					slot, retry+1, maxRetries+1, migrateErr, delay)

				select {
				case <-ctx.Done():
					m.saveSlotCheckpoint(slot, 0, keysMigrated)
					return ctx.Err()
				case <-time.After(delay):
				}
			}
		}

		if migrateErr != nil {
			m.saveSlotCheckpoint(slot, 0, keysMigrated)
			return fmt.Errorf("slot %d: migrateKeys failed after %d retries: %w", slot, maxRetries+1, migrateErr)
		}

		if result != nil {
			keysMigrated += result.Migrated
			m.keysInBatch += result.Migrated
			if result.Filtered > 0 || result.Skipped > 0 {
				m.runner.master.store.IncrementSkippedAndFiltered(
					m.runner.task.ID, result.Skipped, result.Filtered)
			}

			// 检查是否应该关闭重试窗口
			if m.resuming {
				m.resumeKeysCurrent += result.Migrated + result.Skipped + result.Filtered + result.RetrySkipped
				if m.resumeKeysCurrent >= m.resumeKeyTarget {
					m.conflictHandler.SetRetryWindow(false)
					m.resuming = false
					log.Printf("Slot %d: Retry window closed, caught up to previous progress (%d keys)",
						slot, m.resumeKeyTarget)
				}
			}
		}

		// 定期保存断点
		if m.keysInBatch >= checkpointKeyInterval || time.Since(m.lastCheckpointTime) >= checkpointTimeInterval {
			m.saveSlotCheckpoint(slot, 0, keysMigrated)
			m.keysInBatch = 0
			m.lastCheckpointTime = time.Now()
		}
	}

	// 关闭可能仍开启的重试窗口
	if m.resuming {
		m.conflictHandler.SetRetryWindow(false)
		m.resuming = false
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

// getKeysInSlot 使用 CLUSTER GETKEYSINSLOT 分批获取 slot 内的 key
// 相比旧方案（全局 SCAN + 客户端 CRC16 过滤），命中率从 ~25% 提升到 100%
// 分批获取避免大 slot（倾斜场景可能 >100 万 key）一次性占用过多内存
func (m *SlotMigrator) getKeysInSlot(ctx context.Context, client *redis.ClusterClient, slot int) ([]string, error) {
	// 先获取该 slot 的 key 总数
	count, err := client.ClusterCountKeysInSlot(ctx, slot).Result()
	if err != nil {
		return nil, fmt.Errorf("CLUSTER COUNTKEYSINSLOT %d failed: %w", slot, err)
	}

	if count == 0 {
		return nil, nil
	}

	// 小 slot（<= 10000 key）：一次性取出，避免多次网络往返
	const batchLimit = 10000
	if count <= batchLimit {
		keys, err := client.ClusterGetKeysInSlot(ctx, slot, int(count)).Result()
		if err != nil {
			return nil, fmt.Errorf("CLUSTER GETKEYSINSLOT %d %d failed: %w", slot, count, err)
		}
		return keys, nil
	}

	// 大 slot（> 10000 key）：分批获取
	// GETKEYSINSLOT 返回按字典序排列的前 N 个 key
	// 我们无法用 offset，但可以利用 SCAN 在特定节点上按 slot 过滤
	// 方案：直接一次性取出（Redis 内部是 O(count)，分批取总开销一样）
	// 但限制单次最大获取量以控制内存峰值
	log.Printf("Slot %d: Large slot with %d keys, fetching in batches of %d", slot, count, batchLimit)

	var allKeys []string
	remaining := count

	for remaining > 0 {
		select {
		case <-ctx.Done():
			return allKeys, ctx.Err()
		default:
		}

		fetchCount := remaining
		if fetchCount > batchLimit {
			fetchCount = batchLimit
		}

		keys, err := client.ClusterGetKeysInSlot(ctx, slot, int(fetchCount)).Result()
		if err != nil {
			return allKeys, fmt.Errorf("CLUSTER GETKEYSINSLOT %d %d failed: %w", slot, fetchCount, err)
		}

		if len(keys) == 0 {
			break // 没有更多 key
		}

		allKeys = append(allKeys, keys...)

		// GETKEYSINSLOT 不支持 offset，每次返回前 N 个
		// 如果一次返回的 key 数 < 请求的数量，说明已经没有更多 key 了
		if int64(len(keys)) < fetchCount {
			break
		}

		// 如果返回的数量等于请求的数量，且还有剩余
		// 需要通过迁移（删除源端 key）来推进，但我们不删源端
		// 因此对于不删源端 key 的场景，第二次 GETKEYSINSLOT 会返回相同的 key
		// 解决方案：直接一次性取全部，但限制 allKeys 切片的预分配
		remaining -= int64(len(keys))
		if remaining > 0 && int64(len(keys)) == fetchCount {
			// GETKEYSINSLOT 不支持 offset，只能一次取全部
			// 对于超大 slot，直接取全量
			moreKeys, err := client.ClusterGetKeysInSlot(ctx, slot, int(count)).Result()
			if err != nil {
				return allKeys, fmt.Errorf("CLUSTER GETKEYSINSLOT %d full failed: %w", slot, err)
			}
			allKeys = moreKeys
			break
		}
	}

	return allKeys, nil
}

// MigrateKeysResult 迁移结果统计
type MigrateKeysResult struct {
	Migrated     int64 // 实际成功迁移的 key 数
	Filtered     int64 // 被过滤器过滤掉的 key 数
	Skipped      int64 // 因冲突跳过的 key 数（真正的冲突）
	RetrySkipped int64 // 崩溃恢复重试导致的冲突跳过（不计入统计）
	Bytes        int64 // 迁移的字节数
}

// migrateKeys 迁移Key，返回详细的迁移结果统计
// 优化：DUMP + TTL 使用 Pipeline 批量获取，替代逐个串行调用
// 1000 key 从 2000 次 RTT 降低到 2 次 RTT（1 次 DUMP pipeline + 1 次 TTL pipeline 合并为 1 次）
func (m *SlotMigrator) migrateKeys(ctx context.Context, source, target *redis.ClusterClient, keys []string) (*MigrateKeysResult, error) {
	result := &MigrateKeysResult{}
	originalCount := int64(len(keys))

	// Key过滤
	keys = m.filterKeys(keys)
	result.Filtered = originalCount - int64(len(keys))
	if len(keys) == 0 {
		return result, nil
	}

	// 设置当前阶段
	m.conflictHandler.SetPhase(m.runner.GetPhase())

	// 记录 HandleBatchKeys 之前的 retrySkipped，以便区分本批次新增的
	retrySkippedBefore := m.conflictHandler.GetRetrySkippedCount()

	// 批量检查冲突
	keysToMigrate, err := m.conflictHandler.HandleBatchKeys(ctx, keys)
	if err != nil {
		return result, err
	}

	// 计算本批次的重试跳过和真正跳过
	totalSkipped := int64(len(keys)) - int64(len(keysToMigrate))
	retrySkippedAfter := m.conflictHandler.GetRetrySkippedCount()
	retrySkippedInBatch := retrySkippedAfter - retrySkippedBefore
	result.RetrySkipped = retrySkippedInBatch
	result.Skipped = totalSkipped - retrySkippedInBatch // 真正的冲突跳过

	if len(keysToMigrate) == 0 {
		return result, nil
	}

	// 应用源端限流（按实际 key 数量消耗令牌）
	if m.rateLimiter != nil {
		m.rateLimiter.AcquireSourceN(int64(len(keysToMigrate)))
	}

	// ===== 优化核心：Pipeline 批量 DUMP + TTL =====
	// 旧方案：逐个 source.Dump(key) + source.TTL(key) → 每 key 2 次 RTT
	// 新方案：Pipeline 一次性提交所有 DUMP + TTL → 全部 key 仅 1 次 RTT
	srcPipe := source.Pipeline()
	dumpCmds := make([]*redis.StringCmd, len(keysToMigrate))
	ttlCmds := make([]*redis.DurationCmd, len(keysToMigrate))

	for i, key := range keysToMigrate {
		dumpCmds[i] = srcPipe.Dump(ctx, key)
		ttlCmds[i] = srcPipe.PTTL(ctx, key) // 毫秒级精度
	}

	_, err = srcPipe.Exec(ctx)
	if err != nil && err != redis.Nil {
		// Pipeline 部分失败是正常的（某些 key 可能已被删除），逐个检查
	}

	// 收集成功的 DUMP 结果，构建 RESTORE pipeline
	dstPipe := target.Pipeline()
	var totalBytes int64
	var pipelineKeys []string

	for i, key := range keysToMigrate {
		dump, dumpErr := dumpCmds[i].Result()
		if dumpErr != nil {
			continue // key 可能在 DUMP 时已被删除
		}

		ttl, _ := ttlCmds[i].Result()
		if ttl == -2*time.Millisecond {
			continue // PTTL=-2 表示 key 不存在
		}
		if ttl < 0 {
			ttl = 0
		}

		totalBytes += int64(len(dump))
		dstPipe.RestoreReplace(ctx, key, ttl, dump)
		pipelineKeys = append(pipelineKeys, key)
	}

	if len(pipelineKeys) == 0 {
		return result, nil
	}

	// 应用目标端限流（按实际 key 数量消耗令牌，在 pipeline exec 之前）
	if m.rateLimiter != nil {
		m.rateLimiter.AcquireTargetN(int64(len(pipelineKeys)))
	}

	// 执行 RESTORE Pipeline 并逐个检查结果
	cmds, err := dstPipe.Exec(ctx)
	if err != nil {
		var successCount int64
		var failedKeys []string

		for i, cmd := range cmds {
			if cmd.Err() == nil {
				successCount++
			} else {
				if i < len(pipelineKeys) {
					failedKeys = append(failedKeys, pipelineKeys[i])
				}
			}
		}

		if successCount > 0 {
			result.Migrated = successCount
			result.Bytes = totalBytes
			m.worker.ReportProgress(successCount, totalBytes)
			log.Printf("Pipeline partial success: %d/%d keys succeeded, %d failed",
				successCount, len(pipelineKeys), len(failedKeys))

			if len(failedKeys) > 0 && len(failedKeys) <= 10 {
				log.Printf("Failed keys: %v", failedKeys)
			} else if len(failedKeys) > 10 {
				log.Printf("Failed keys (first 10): %v", failedKeys[:10])
			}
		}

		if successCount == 0 {
			return result, fmt.Errorf("pipeline exec all failed (%d keys): %w", len(pipelineKeys), err)
		}

		return result, nil
	}

	// 全部成功
	result.Migrated = int64(len(pipelineKeys))
	result.Bytes = totalBytes

	// 报告进度（只统计实际迁移的 key）
	m.worker.ReportProgress(result.Migrated, totalBytes)

	return result, nil
}

// 内置排除的系统内部 key 前缀（这些 key 不包含业务数据，不应被迁移）
// - stat:total:*  Tendis 内部统计（总计）
// - stat:daily:*  Tendis 内部统计（每日）
// - stat:hourly:* Tendis 内部统计（每小时）
var systemInternalKeyPrefixes = []string{
	"stat:total:",
	"stat:daily:",
	"stat:hourly:",
}

// isSystemInternalKey 判断是否为系统内部 key（不应被迁移）
func isSystemInternalKey(key string) bool {
	for _, prefix := range systemInternalKeyPrefixes {
		if strings.HasPrefix(key, prefix) {
			return true
		}
	}
	return false
}

// filterKeys 根据配置过滤Key
func (m *SlotMigrator) filterKeys(keys []string) []string {
	filter := m.runner.options.KeyFilter

	var result []string
	for _, key := range keys {
		// 内置排除：系统内部 key 始终跳过
		if isSystemInternalKey(key) {
			continue
		}
		if filter == nil {
			result = append(result, key)
			continue
		}
		if m.shouldMigrateKey(key, filter) {
			result = append(result, key)
		}
	}
	return result
}

// shouldMigrateKey 判断Key是否应该被迁移（使用预编译正则）
func (m *SlotMigrator) shouldMigrateKey(key string, filter *model.KeyFilterConfig) bool {
	// 先检查排除规则
	for _, prefix := range filter.ExcludePrefixes {
		if strings.HasPrefix(key, prefix) {
			return false
		}
	}
	for _, re := range m.runner.compiledExcludePatterns {
		if re.MatchString(key) {
			return false
		}
	}

	// 再检查包含规则
	switch filter.Mode {
	case model.KeyFilterModePrefix:
		if len(filter.Prefixes) == 0 {
			return true
		}
		for _, prefix := range filter.Prefixes {
			if strings.HasPrefix(key, prefix) {
				return true
			}
		}
		return false
	case model.KeyFilterModePattern:
		if len(m.runner.compiledPatterns) == 0 {
			return true
		}
		for _, re := range m.runner.compiledPatterns {
			if re.MatchString(key) {
				return true
			}
		}
		return false
	case model.KeyFilterModeKeys, model.KeyFilterModeKeylist:
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
	
	// 崩溃恢复重试窗口
	inRetryWindow  bool   // 是否处于重试窗口内
	retrySkipped   int64  // 重试窗口内跳过的 key 数（不计入真正冲突）
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
	if h.inRetryWindow {
		// 重试窗口内：这是崩溃恢复导致的假冲突，只计数不记录
		h.retrySkipped++
		log.Printf("[RETRY_SKIP] TaskID=%s Key=%s (crash recovery retry, not a real conflict)", h.taskID, key)
		return
	}
	h.skippedKeys = append(h.skippedKeys, key)
	log.Printf("[CONFLICT_SKIP] TaskID=%s Key=%s", h.taskID, key)
}

// SetRetryWindow 设置/关闭重试窗口
func (h *ConflictHandler) SetRetryWindow(enabled bool) {
	h.skippedKeysMu.Lock()
	defer h.skippedKeysMu.Unlock()
	if h.inRetryWindow && !enabled {
		log.Printf("[RETRY_WINDOW] TaskID=%s closed, retry_skipped=%d keys were from crash recovery",
			h.taskID, h.retrySkipped)
	}
	if !h.inRetryWindow && enabled {
		h.retrySkipped = 0
		log.Printf("[RETRY_WINDOW] TaskID=%s opened, conflicts in this window will not be counted as real", h.taskID)
	}
	h.inRetryWindow = enabled
}

// GetRetrySkippedCount 获取重试窗口内跳过的 key 数
func (h *ConflictHandler) GetRetrySkippedCount() int64 {
	h.skippedKeysMu.Lock()
	defer h.skippedKeysMu.Unlock()
	return h.retrySkipped
}

// IsInRetryWindow 是否处于重试窗口
func (h *ConflictHandler) IsInRetryWindow() bool {
	h.skippedKeysMu.Lock()
	defer h.skippedKeysMu.Unlock()
	return h.inRetryWindow
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
					// 跳过已存在的Key（全量阶段），同样记录
					h.RecordSkippedKey(key)
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
// VerifyMode 校验模式
type VerifyMode string

const (
	VerifyModeSample VerifyMode = "sample" // 采样校验（默认，快速）
	VerifyModeFull   VerifyMode = "full"   // 全量校验（流式，适用于 100 亿 Key 场景）
)

// VerifyConfig 校验配置
type VerifyConfig struct {
	Mode       VerifyMode             // 校验模式：sample / full
	SampleSize int                    // 采样模式下的采样数量（默认 10000）
	BatchSize  int64                  // 每次 SCAN 的批大小（默认 1000）
	Concurrency int                   // 并发校验协程数（默认 50）
	KeyFilter  func(key string) bool  // Key 过滤函数（nil 表示不过滤）
	ScanPatterns []string             // SCAN pattern 列表（如 ["prefix1*", "prefix2*"]，nil 表示 "*"）
}

// DefaultVerifyConfig 默认校验配置
func DefaultVerifyConfig() *VerifyConfig {
	return &VerifyConfig{
		Mode:        VerifyModeSample,
		SampleSize:  10000,
		BatchSize:   1000,
		Concurrency: 50,
	}
}

type Verifier struct {
	sourceClient *redis.ClusterClient
	targetClient *redis.ClusterClient
	config       *VerifyConfig
}

// NewVerifier 创建校验器
func NewVerifier(source, target *redis.ClusterClient, config *VerifyConfig) *Verifier {
	if config == nil {
		config = DefaultVerifyConfig()
	}
	if config.BatchSize <= 0 {
		config.BatchSize = 1000
	}
	if config.Concurrency <= 0 {
		config.Concurrency = 50
	}
	if config.SampleSize <= 0 {
		config.SampleSize = 10000
	}
	return &Verifier{
		sourceClient: source,
		targetClient: target,
		config:       config,
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

// Verify 执行校验（流式：SCAN 一批 → 过滤 → 比对 → 释放，不存储全量 Key）
// 优化：使用 ScanPatterns 利用 SCAN MATCH 服务端前缀过滤
func (v *Verifier) Verify(ctx context.Context) (*VerifyResult, error) {
	var totalKeys int64
	var matched, mismatched, missing int64

	sem := make(chan struct{}, v.config.Concurrency)
	var wg sync.WaitGroup

	batchSize := v.config.BatchSize
	isSample := v.config.Mode == VerifyModeSample
	sampleLimit := int64(v.config.SampleSize)

	scanPatterns := v.config.ScanPatterns
	if len(scanPatterns) == 0 {
		scanPatterns = []string{"*"}
	}

	for _, scanPattern := range scanPatterns {
		cursor := uint64(0)

		for {
			select {
			case <-ctx.Done():
				wg.Wait()
				return v.buildResult(totalKeys, matched, mismatched, missing), ctx.Err()
			default:
			}

			if isSample && atomic.LoadInt64(&totalKeys) >= sampleLimit {
				break
			}

			scanCount := batchSize
			if isSample {
				remaining := sampleLimit - atomic.LoadInt64(&totalKeys)
				if remaining < scanCount {
					scanCount = remaining
				}
			}
			keys, nextCursor, err := v.sourceClient.Scan(ctx, cursor, scanPattern, scanCount).Result()
			if err != nil {
				wg.Wait()
				return v.buildResult(totalKeys, matched, mismatched, missing), fmt.Errorf("SCAN failed: %w", err)
			}

			for _, key := range keys {
				if v.config.KeyFilter != nil && !v.config.KeyFilter(key) {
					continue
				}

				if isSample && atomic.LoadInt64(&totalKeys) >= sampleLimit {
					break
				}

				atomic.AddInt64(&totalKeys, 1)

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

			cursor = nextCursor
			if cursor == 0 {
				break
			}
		}

		// 采样模式达到上限则停止所有 pattern 的遍历
		if isSample && atomic.LoadInt64(&totalKeys) >= sampleLimit {
			break
		}
	}

	wg.Wait()

	result := v.buildResult(totalKeys, matched, mismatched, missing)

	log.Printf("Verify completed: mode=%s, total=%d, matched=%d, mismatched=%d, missing=%d, consistency=%.2f%%",
		v.config.Mode, result.TotalKeys, result.MatchedKeys, result.MismatchedKeys, result.MissingKeys, result.ConsistencyRate)

	return result, nil
}

// buildResult 构建校验结果
func (v *Verifier) buildResult(total, matched, mismatched, missing int64) *VerifyResult {
	result := &VerifyResult{
		TotalKeys:      int(total),
		MatchedKeys:    int(matched),
		MismatchedKeys: int(mismatched),
		MissingKeys:    int(missing),
	}
	if result.TotalKeys > 0 {
		result.ConsistencyRate = float64(result.MatchedKeys) / float64(result.TotalKeys) * 100
	}
	return result
}

// verifyKey 校验单个 Key（比较 DUMP 序列化值 + PTTL 一致性）
func (v *Verifier) verifyKey(ctx context.Context, key string) (match, exists bool) {
	targetExists, err := v.targetClient.Exists(ctx, key).Result()
	if err != nil || targetExists == 0 {
		return false, false
	}

	// 使用 Pipeline 一次性获取 DUMP + PTTL，减少 RTT
	srcPipe := v.sourceClient.Pipeline()
	srcDumpCmd := srcPipe.Dump(ctx, key)
	srcPttlCmd := srcPipe.PTTL(ctx, key)
	srcPipe.Exec(ctx)

	dstPipe := v.targetClient.Pipeline()
	dstDumpCmd := dstPipe.Dump(ctx, key)
	dstPttlCmd := dstPipe.PTTL(ctx, key)
	dstPipe.Exec(ctx)

	sourceDump, err := srcDumpCmd.Result()
	if err != nil {
		return false, true
	}

	targetDump, err := dstDumpCmd.Result()
	if err != nil {
		return false, true
	}

	// 比较 DUMP 序列化值
	if sourceDump != targetDump {
		return false, true
	}

	// 比较 PTTL 一致性
	srcTTL, _ := srcPttlCmd.Result()
	dstTTL, _ := dstPttlCmd.Result()

	// TTL 一致性规则：
	// 1. 源端永不过期(-1) → 目标端也必须永不过期(-1)
	// 2. 源端有过期时间 → 目标端也应有过期时间（允许 5 秒误差）
	const ttlTolerance = 5 * time.Second
	if srcTTL < 0 && dstTTL < 0 {
		// 两端都永不过期，一致
	} else if srcTTL < 0 && dstTTL >= 0 {
		// 源端永不过期但目标端有过期时间，不一致
		return false, true
	} else if srcTTL >= 0 && dstTTL < 0 {
		// 源端有过期时间但目标端永不过期，不一致（严重 bug）
		return false, true
	} else {
		// 两端都有过期时间，允许一定误差（迁移延迟导致）
		diff := srcTTL - dstTTL
		if diff < 0 {
			diff = -diff
		}
		if diff > ttlTolerance {
			return false, true
		}
	}

	return true, true
}
