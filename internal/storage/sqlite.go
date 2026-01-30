package storage

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"tendis-migrate/internal/model"
)

// SQLiteStore SQLite存储
type SQLiteStore struct {
	db          *sql.DB
	batchWriter *BatchWriter
	mu          sync.RWMutex
}

// SQLiteConfig SQLite配置
type SQLiteConfig struct {
	Path            string
	MaxOpenConns    int
	MaxIdleConns    int
	ConnMaxLifetime time.Duration
	BatchSize       int
	FlushInterval   time.Duration
}

// DefaultSQLiteConfig 默认配置
var DefaultSQLiteConfig = SQLiteConfig{
	Path:            "./data/migrate.db",
	MaxOpenConns:    20,
	MaxIdleConns:    5,
	ConnMaxLifetime: 30 * time.Minute,
	BatchSize:       1000,
	FlushInterval:   1 * time.Second,
}

// NewSQLiteStore 创建SQLite存储
func NewSQLiteStore(cfg SQLiteConfig) (*SQLiteStore, error) {
	dsn := fmt.Sprintf("%s?_journal_mode=WAL&_synchronous=NORMAL&_busy_timeout=5000", cfg.Path)

	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}

	db.SetMaxOpenConns(cfg.MaxOpenConns)
	db.SetMaxIdleConns(cfg.MaxIdleConns)
	db.SetConnMaxLifetime(cfg.ConnMaxLifetime)

	store := &SQLiteStore{db: db}

	if err := store.initSchema(); err != nil {
		return nil, fmt.Errorf("init schema: %w", err)
	}

	store.batchWriter = NewBatchWriter(db, cfg.BatchSize, cfg.FlushInterval)
	go store.batchWriter.Run()

	return store, nil
}

// initSchema 初始化表结构
func (s *SQLiteStore) initSchema() error {
	schema := `
	-- 启用WAL模式
	PRAGMA journal_mode = WAL;
	PRAGMA synchronous = NORMAL;
	PRAGMA busy_timeout = 5000;

	-- 任务表
	CREATE TABLE IF NOT EXISTS tasks (
		id TEXT PRIMARY KEY,
		name TEXT NOT NULL,
		source_cluster TEXT NOT NULL,
		target_cluster TEXT NOT NULL,
		status TEXT NOT NULL DEFAULT 'pending',
		config TEXT,
		created_at INTEGER NOT NULL,
		updated_at INTEGER NOT NULL,
		started_at INTEGER,
		completed_at INTEGER
	);

	-- Slot分配表
	CREATE TABLE IF NOT EXISTS slot_assignments (
		task_id TEXT NOT NULL,
		worker_id TEXT NOT NULL,
		slot_start INTEGER NOT NULL,
		slot_end INTEGER NOT NULL,
		status TEXT NOT NULL DEFAULT 'pending',
		assigned_at INTEGER NOT NULL,
		PRIMARY KEY (task_id, slot_start),
		FOREIGN KEY (task_id) REFERENCES tasks(id)
	);
	CREATE INDEX IF NOT EXISTS idx_slot_worker ON slot_assignments(worker_id);

	-- 断点表
	CREATE TABLE IF NOT EXISTS checkpoints (
		task_id TEXT NOT NULL,
		worker_id TEXT NOT NULL,
		slot_id INTEGER NOT NULL,
		cursor TEXT NOT NULL DEFAULT '0',
		keys_migrated INTEGER DEFAULT 0,
		bytes_migrated INTEGER DEFAULT 0,
		last_key TEXT,
		updated_at INTEGER NOT NULL,
		PRIMARY KEY (task_id, worker_id, slot_id)
	);
	CREATE INDEX IF NOT EXISTS idx_checkpoint_task ON checkpoints(task_id);

	-- 大Key进度表
	CREATE TABLE IF NOT EXISTS large_key_progress (
		task_id TEXT NOT NULL,
		key_name TEXT NOT NULL,
		key_type TEXT NOT NULL,
		total_bytes INTEGER NOT NULL,
		transferred_bytes INTEGER DEFAULT 0,
		checksum TEXT,
		status TEXT NOT NULL DEFAULT 'pending',
		updated_at INTEGER NOT NULL,
		PRIMARY KEY (task_id, key_name)
	);

	-- 校验结果表
	CREATE TABLE IF NOT EXISTS verification_results (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		task_id TEXT NOT NULL,
		batch_id TEXT NOT NULL,
		total_keys INTEGER NOT NULL,
		matched_keys INTEGER NOT NULL,
		mismatched_keys INTEGER DEFAULT 0,
		missing_keys INTEGER DEFAULT 0,
		extra_keys INTEGER DEFAULT 0,
		details TEXT,
		created_at INTEGER NOT NULL
	);
	CREATE INDEX IF NOT EXISTS idx_verify_task ON verification_results(task_id);

	-- 迁移统计表
	CREATE TABLE IF NOT EXISTS migration_stats (
		task_id TEXT PRIMARY KEY,
		total_keys INTEGER DEFAULT 0,
		migrated_keys INTEGER DEFAULT 0,
		total_bytes INTEGER DEFAULT 0,
		migrated_bytes INTEGER DEFAULT 0,
		large_keys_count INTEGER DEFAULT 0,
		skipped_keys_count INTEGER DEFAULT 0,
		error_count INTEGER DEFAULT 0,
		start_time INTEGER,
		end_time INTEGER,
		FOREIGN KEY (task_id) REFERENCES tasks(id)
	);

	-- 监控指标表
	CREATE TABLE IF NOT EXISTS metrics (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		task_id TEXT NOT NULL,
		metric_name TEXT NOT NULL,
		metric_value REAL NOT NULL,
		timestamp INTEGER NOT NULL
	);
	CREATE INDEX IF NOT EXISTS idx_metrics_task_time ON metrics(task_id, timestamp);
	`

	_, err := s.db.Exec(schema)
	return err
}

// Close 关闭连接
func (s *SQLiteStore) Close() error {
	if s.batchWriter != nil {
		s.batchWriter.Stop()
	}
	return s.db.Close()
}

// ============ Task Operations ============

// CreateTask 创建任务
func (s *SQLiteStore) CreateTask(task *model.Task) error {
	query := `INSERT INTO tasks (id, name, source_cluster, target_cluster, status, config, created_at, updated_at)
			  VALUES (?, ?, ?, ?, ?, ?, ?, ?)`

	now := time.Now().Unix()
	_, err := s.db.Exec(query, task.ID, task.Name, task.SourceCluster, task.TargetCluster,
		task.Status, task.Config, now, now)
	return err
}

// GetTask 获取任务
func (s *SQLiteStore) GetTask(id string) (*model.Task, error) {
	query := `SELECT id, name, source_cluster, target_cluster, status, config, created_at, updated_at, started_at, completed_at
			  FROM tasks WHERE id = ?`

	task := &model.Task{}
	err := s.db.QueryRow(query, id).Scan(
		&task.ID, &task.Name, &task.SourceCluster, &task.TargetCluster,
		&task.Status, &task.Config, &task.CreatedAt, &task.UpdatedAt,
		&task.StartedAt, &task.CompletedAt,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return task, nil
}

// ListTasks 获取任务列表
func (s *SQLiteStore) ListTasks(status string, page, size int) ([]*model.Task, int, error) {
	var tasks []*model.Task
	var total int

	countQuery := "SELECT COUNT(*) FROM tasks"
	listQuery := `SELECT id, name, source_cluster, target_cluster, status, config, created_at, updated_at, started_at, completed_at
				  FROM tasks ORDER BY created_at DESC LIMIT ? OFFSET ?`

	if status != "" {
		countQuery += " WHERE status = ?"
		listQuery = `SELECT id, name, source_cluster, target_cluster, status, config, created_at, updated_at, started_at, completed_at
					 FROM tasks WHERE status = ? ORDER BY created_at DESC LIMIT ? OFFSET ?`
	}

	// Count
	if status != "" {
		s.db.QueryRow(countQuery, status).Scan(&total)
	} else {
		s.db.QueryRow(countQuery).Scan(&total)
	}

	// List
	offset := (page - 1) * size
	var rows *sql.Rows
	var err error
	if status != "" {
		rows, err = s.db.Query(listQuery, status, size, offset)
	} else {
		rows, err = s.db.Query(listQuery, size, offset)
	}
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	for rows.Next() {
		task := &model.Task{}
		err := rows.Scan(
			&task.ID, &task.Name, &task.SourceCluster, &task.TargetCluster,
			&task.Status, &task.Config, &task.CreatedAt, &task.UpdatedAt,
			&task.StartedAt, &task.CompletedAt,
		)
		if err != nil {
			return nil, 0, err
		}
		tasks = append(tasks, task)
	}

	return tasks, total, nil
}

// UpdateTaskStatus 更新任务状态
func (s *SQLiteStore) UpdateTaskStatus(id string, status model.TaskStatus) error {
	query := `UPDATE tasks SET status = ?, updated_at = ? WHERE id = ?`
	_, err := s.db.Exec(query, status, time.Now().Unix(), id)
	return err
}

// UpdateTaskStarted 更新任务启动时间
func (s *SQLiteStore) UpdateTaskStarted(id string) error {
	now := time.Now().Unix()
	query := `UPDATE tasks SET status = 'running', started_at = ?, updated_at = ? WHERE id = ?`
	_, err := s.db.Exec(query, now, now, id)
	return err
}

// UpdateTaskCompleted 更新任务完成时间
func (s *SQLiteStore) UpdateTaskCompleted(id string, status model.TaskStatus) error {
	now := time.Now().Unix()
	query := `UPDATE tasks SET status = ?, completed_at = ?, updated_at = ? WHERE id = ?`
	_, err := s.db.Exec(query, status, now, now, id)
	return err
}

// DeleteTask 删除任务
func (s *SQLiteStore) DeleteTask(id string) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// 删除关联数据
	tables := []string{"slot_assignments", "checkpoints", "large_key_progress", "verification_results", "migration_stats", "metrics"}
	for _, table := range tables {
		_, err := tx.Exec(fmt.Sprintf("DELETE FROM %s WHERE task_id = ?", table), id)
		if err != nil {
			return err
		}
	}

	// 删除任务
	_, err = tx.Exec("DELETE FROM tasks WHERE id = ?", id)
	if err != nil {
		return err
	}

	return tx.Commit()
}

// ============ Stats Operations ============

// GetOrCreateStats 获取或创建统计
func (s *SQLiteStore) GetOrCreateStats(taskID string) (*model.MigrationStats, error) {
	stats := &model.MigrationStats{TaskID: taskID}

	query := `SELECT total_keys, migrated_keys, total_bytes, migrated_bytes, large_keys_count, skipped_keys_count, error_count, start_time, end_time
			  FROM migration_stats WHERE task_id = ?`

	err := s.db.QueryRow(query, taskID).Scan(
		&stats.TotalKeys, &stats.MigratedKeys, &stats.TotalBytes, &stats.MigratedBytes,
		&stats.LargeKeysCount, &stats.SkippedKeysCount, &stats.ErrorCount,
		&stats.StartTime, &stats.EndTime,
	)

	if err == sql.ErrNoRows {
		// 创建新记录
		insertQuery := `INSERT INTO migration_stats (task_id, total_keys, migrated_keys, total_bytes, migrated_bytes)
						VALUES (?, 0, 0, 0, 0)`
		_, err := s.db.Exec(insertQuery, taskID)
		if err != nil {
			return nil, err
		}
		return stats, nil
	}

	if err != nil {
		return nil, err
	}

	return stats, nil
}

// UpdateStats 更新统计
func (s *SQLiteStore) UpdateStats(stats *model.MigrationStats) error {
	query := `UPDATE migration_stats SET 
			  total_keys = ?, migrated_keys = ?, total_bytes = ?, migrated_bytes = ?,
			  large_keys_count = ?, skipped_keys_count = ?, error_count = ?, start_time = ?, end_time = ?
			  WHERE task_id = ?`

	_, err := s.db.Exec(query,
		stats.TotalKeys, stats.MigratedKeys, stats.TotalBytes, stats.MigratedBytes,
		stats.LargeKeysCount, stats.SkippedKeysCount, stats.ErrorCount,
		stats.StartTime, stats.EndTime, stats.TaskID,
	)
	return err
}

// IncrementStats 增量更新统计
func (s *SQLiteStore) IncrementStats(taskID string, keys, bytes int64) error {
	query := `UPDATE migration_stats SET migrated_keys = migrated_keys + ?, migrated_bytes = migrated_bytes + ? WHERE task_id = ?`
	_, err := s.db.Exec(query, keys, bytes, taskID)
	return err
}

// ============ Checkpoint Operations ============

// SaveCheckpoint 保存断点
func (s *SQLiteStore) SaveCheckpoint(cp *model.Checkpoint) error {
	query := `INSERT OR REPLACE INTO checkpoints (task_id, worker_id, slot_id, cursor, keys_migrated, bytes_migrated, last_key, updated_at)
			  VALUES (?, ?, ?, ?, ?, ?, ?, ?)`

	_, err := s.db.Exec(query, cp.TaskID, cp.WorkerID, cp.SlotID, cp.Cursor,
		cp.KeysMigrated, cp.BytesMigrated, cp.LastKey, time.Now().Unix())
	return err
}

// GetCheckpoints 获取任务的所有断点
func (s *SQLiteStore) GetCheckpoints(taskID string) ([]*model.Checkpoint, error) {
	query := `SELECT task_id, worker_id, slot_id, cursor, keys_migrated, bytes_migrated, last_key, updated_at
			  FROM checkpoints WHERE task_id = ?`

	rows, err := s.db.Query(query, taskID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var checkpoints []*model.Checkpoint
	for rows.Next() {
		cp := &model.Checkpoint{}
		err := rows.Scan(&cp.TaskID, &cp.WorkerID, &cp.SlotID, &cp.Cursor,
			&cp.KeysMigrated, &cp.BytesMigrated, &cp.LastKey, &cp.UpdatedAt)
		if err != nil {
			return nil, err
		}
		checkpoints = append(checkpoints, cp)
	}

	return checkpoints, nil
}

// ============ Metrics Operations ============

// SaveMetric 保存指标
func (s *SQLiteStore) SaveMetric(taskID, name string, value float64) error {
	s.batchWriter.Write(
		"INSERT INTO metrics (task_id, metric_name, metric_value, timestamp) VALUES (?, ?, ?, ?)",
		taskID, name, value, time.Now().Unix(),
	)
	return nil
}

// GetMetrics 获取指标
func (s *SQLiteStore) GetMetrics(taskID string, startTime, endTime int64) (map[string][]MetricPoint, error) {
	query := `SELECT metric_name, metric_value, timestamp FROM metrics
			  WHERE task_id = ? AND timestamp >= ? AND timestamp <= ?
			  ORDER BY timestamp`

	rows, err := s.db.Query(query, taskID, startTime, endTime)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make(map[string][]MetricPoint)
	for rows.Next() {
		var name string
		var point MetricPoint
		if err := rows.Scan(&name, &point.Value, &point.Timestamp); err != nil {
			return nil, err
		}
		result[name] = append(result[name], point)
	}

	return result, nil
}

// MetricPoint 指标点
type MetricPoint struct {
	Value     float64 `json:"value"`
	Timestamp int64   `json:"timestamp"`
}

// ============ Verification Operations ============

// SaveVerifyResult 保存校验结果
func (s *SQLiteStore) SaveVerifyResult(result *model.VerifyResult) error {
	query := `INSERT INTO verification_results (task_id, batch_id, total_keys, matched_keys, mismatched_keys, missing_keys, extra_keys, details, created_at)
			  VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`

	_, err := s.db.Exec(query, result.TaskID, result.BatchID, result.TotalKeys,
		result.MatchedKeys, result.MismatchedKeys, result.MissingKeys, result.ExtraKeys,
		result.Details, time.Now().Unix())
	return err
}

// GetVerifyResults 获取校验结果
func (s *SQLiteStore) GetVerifyResults(taskID string) ([]*model.VerifyResult, error) {
	query := `SELECT id, task_id, batch_id, total_keys, matched_keys, mismatched_keys, missing_keys, extra_keys, details, created_at
			  FROM verification_results WHERE task_id = ? ORDER BY created_at DESC`

	rows, err := s.db.Query(query, taskID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []*model.VerifyResult
	for rows.Next() {
		r := &model.VerifyResult{}
		err := rows.Scan(&r.ID, &r.TaskID, &r.BatchID, &r.TotalKeys, &r.MatchedKeys,
			&r.MismatchedKeys, &r.MissingKeys, &r.ExtraKeys, &r.Details, &r.CreatedAt)
		if err != nil {
			return nil, err
		}
		if r.TotalKeys > 0 {
			r.ConsistencyRate = float64(r.MatchedKeys) / float64(r.TotalKeys) * 100
		}
		results = append(results, r)
	}

	return results, nil
}

// ============ Slot Assignment Operations ============

// SaveSlotAssignment 保存Slot分配
func (s *SQLiteStore) SaveSlotAssignment(assignment *model.SlotAssignment) error {
	query := `INSERT OR REPLACE INTO slot_assignments (task_id, worker_id, slot_start, slot_end, status, assigned_at)
			  VALUES (?, ?, ?, ?, ?, ?)`

	_, err := s.db.Exec(query, assignment.TaskID, assignment.WorkerID,
		assignment.SlotStart, assignment.SlotEnd, assignment.Status, time.Now().Unix())
	return err
}

// GetSlotAssignments 获取任务的Slot分配
func (s *SQLiteStore) GetSlotAssignments(taskID string) ([]*model.SlotAssignment, error) {
	query := `SELECT task_id, worker_id, slot_start, slot_end, status, assigned_at
			  FROM slot_assignments WHERE task_id = ?`

	rows, err := s.db.Query(query, taskID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var assignments []*model.SlotAssignment
	for rows.Next() {
		a := &model.SlotAssignment{}
		if err := rows.Scan(&a.TaskID, &a.WorkerID, &a.SlotStart, &a.SlotEnd, &a.Status, &a.AssignedAt); err != nil {
			return nil, err
		}
		assignments = append(assignments, a)
	}

	return assignments, nil
}

// ============ BatchWriter ============

// BatchWriter 批量写入器
type BatchWriter struct {
	db            *sql.DB
	buffer        []*WriteOp
	mu            sync.Mutex
	batchSize     int
	flushInterval time.Duration
	stopCh        chan struct{}
	stoppedCh     chan struct{}
}

// WriteOp 写操作
type WriteOp struct {
	Query string
	Args  []interface{}
}

// NewBatchWriter 创建批量写入器
func NewBatchWriter(db *sql.DB, batchSize int, flushInterval time.Duration) *BatchWriter {
	return &BatchWriter{
		db:            db,
		buffer:        make([]*WriteOp, 0, batchSize),
		batchSize:     batchSize,
		flushInterval: flushInterval,
		stopCh:        make(chan struct{}),
		stoppedCh:     make(chan struct{}),
	}
}

// Write 写入
func (w *BatchWriter) Write(query string, args ...interface{}) {
	w.mu.Lock()
	w.buffer = append(w.buffer, &WriteOp{Query: query, Args: args})
	shouldFlush := len(w.buffer) >= w.batchSize
	w.mu.Unlock()

	if shouldFlush {
		w.Flush()
	}
}

// Flush 刷新
func (w *BatchWriter) Flush() error {
	w.mu.Lock()
	ops := w.buffer
	w.buffer = make([]*WriteOp, 0, w.batchSize)
	w.mu.Unlock()

	if len(ops) == 0 {
		return nil
	}

	tx, err := w.db.Begin()
	if err != nil {
		return err
	}

	for _, op := range ops {
		if _, err := tx.Exec(op.Query, op.Args...); err != nil {
			tx.Rollback()
			return err
		}
	}

	return tx.Commit()
}

// Run 运行定时刷新
func (w *BatchWriter) Run() {
	ticker := time.NewTicker(w.flushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			w.Flush()
		case <-w.stopCh:
			w.Flush()
			close(w.stoppedCh)
			return
		}
	}
}

// Stop 停止
func (w *BatchWriter) Stop() {
	close(w.stopCh)
	<-w.stoppedCh
}

// GetTaskProgress 获取任务进度
func (s *SQLiteStore) GetTaskProgress(taskID string) (*model.Progress, error) {
	stats, err := s.GetOrCreateStats(taskID)
	if err != nil {
		return nil, err
	}

	task, err := s.GetTask(taskID)
	if err != nil {
		return nil, err
	}

	progress := &model.Progress{
		Phase:         model.PhaseFullMigration,
		TotalKeys:     stats.TotalKeys,
		MigratedKeys:  stats.MigratedKeys,
		TotalBytes:    stats.TotalBytes,
		MigratedBytes: stats.MigratedBytes,
	}

	if stats.TotalKeys > 0 {
		progress.Percentage = float64(stats.MigratedKeys) / float64(stats.TotalKeys) * 100
	}

	// 计算速度和ETA
	if task != nil && task.StartedAt != nil {
		elapsed := time.Now().Unix() - *task.StartedAt
		if elapsed > 0 {
			progress.CurrentSpeed = stats.MigratedKeys / elapsed
			if progress.CurrentSpeed > 0 {
				remaining := stats.TotalKeys - stats.MigratedKeys
				eta := remaining / progress.CurrentSpeed
				progress.EstimatedETA = formatDuration(time.Duration(eta) * time.Second)
			}
		}
	}

	return progress, nil
}

func formatDuration(d time.Duration) string {
	h := d / time.Hour
	m := (d % time.Hour) / time.Minute
	s := (d % time.Minute) / time.Second

	if h > 0 {
		return fmt.Sprintf("%dh%dm%ds", h, m, s)
	}
	if m > 0 {
		return fmt.Sprintf("%dm%ds", m, s)
	}
	return fmt.Sprintf("%ds", s)
}

// Helper function for JSON marshaling
func ToJSON(v interface{}) string {
	data, _ := json.Marshal(v)
	return string(data)
}
