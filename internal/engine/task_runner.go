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
	"tendis-migrate/internal/model"
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
		if err := r.runIncrementalSync(); err != nil {
			log.Printf("Incremental sync failed: %v", err)
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

	return nil
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
func (r *TaskRunner) runFullMigration() error {
	// 分配Slots
	workerCount := r.options.WorkerCount
	if workerCount <= 0 {
		workerCount = 4
	}

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
func (r *TaskRunner) runIncrementalSync() error {
	// 监听Keyspace通知
	// 使用收敛算法判断是否完成
	
	convergence := NewConvergenceDetector()
	
	r.workersMu.RLock()
	for _, worker := range r.workers {
		r.wg.Add(1)
		go func(w *EmbeddedWorker) {
			defer r.wg.Done()
			w.RunIncrementalSync(r.ctx, convergence)
		}(worker)
	}
	r.workersMu.RUnlock()

	// 等待收敛
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-r.ctx.Done():
			return r.ctx.Err()
		case <-ticker.C:
			stats, _ := r.master.store.GetOrCreateStats(r.task.ID)
			record := ConvergenceRecord{
				ChangeCount: stats.MigratedKeys,
				TotalKeys:   stats.TotalKeys,
				Timestamp:   time.Now(),
			}

			if converged, reason := convergence.IsConverged(record); converged {
				log.Printf("Incremental sync converged: %s", reason)
				return nil
			}

			// 检查是否暂停
			if r.paused.Load() {
				<-r.ctx.Done()
				return nil
			}
		}
	}
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
	if r.sourceClient != nil {
		r.sourceClient.Close()
	}
	if r.targetClient != nil {
		r.targetClient.Close()
	}
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

// RunIncrementalSync 运行增量同步
func (w *EmbeddedWorker) RunIncrementalSync(ctx context.Context, convergence *ConvergenceDetector) {
	// 增量同步逻辑
	// 监听Keyspace通知并处理变更
	
	log.Printf("Worker %s starting incremental sync", w.id)

	// 简化：等待收敛信号
	<-ctx.Done()
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
}

// NewSlotMigrator 创建Slot迁移器
func NewSlotMigrator(runner *TaskRunner, worker *EmbeddedWorker) *SlotMigrator {
	return &SlotMigrator{
		runner:          runner,
		worker:          worker,
		conflictHandler: NewConflictHandler(runner.options.ConflictPolicy, runner.targetClient, runner.task.ID),
	}
}

// MigrateSlot 迁移单个Slot
func (m *SlotMigrator) MigrateSlot(ctx context.Context, slot int) error {
	source := m.runner.sourceClient
	target := m.runner.targetClient

	var cursor uint64
	batchSize := int64(m.runner.options.ScanBatchSize)
	if batchSize <= 0 {
		batchSize = 1000
	}

	for {
		select {
		case <-ctx.Done():
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
			}
		}

		cursor = nextCursor
		if cursor == 0 {
			break
		}
	}

	return nil
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

	// Pipeline迁移
	pipe := target.Pipeline()
	
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

		pipe.RestoreReplace(ctx, key, ttl, dump)
	}

	_, err = pipe.Exec(ctx)
	if err != nil {
		return err
	}

	// 报告进度
	m.worker.ReportProgress(int64(len(keysToMigrate)), 0)

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
	case model.KeyFilterModeKeys:
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
