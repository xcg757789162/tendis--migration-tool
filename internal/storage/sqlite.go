package storage

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"tendis-migrate/internal/model"
)

// SQLiteConfig SQLite 配置
type SQLiteConfig struct {
	Path string `json:"path"`
}

// DefaultSQLiteConfig 默认配置
var DefaultSQLiteConfig = SQLiteConfig{
	Path: "./data/migrate.db",
}

// SQLiteStore 兼容 engine 包使用的 SQLiteStore 类型别名
type SQLiteStore = SQLiteDB

// NewSQLiteStore 创建新的 SQLiteStore（兼容 engine 包）
func NewSQLiteStore(cfg SQLiteConfig) (*SQLiteStore, error) {
	return NewSQLiteDB(cfg.Path)
}

// ToJSON 将对象转换为 JSON 字符串
func ToJSON(v interface{}) string {
	data, err := json.Marshal(v)
	if err != nil {
		return "{}"
	}
	return string(data)
}

// SQLiteDB SQLite 数据库封装
type SQLiteDB struct {
	db           *sql.DB
	dbPath       string
	backupMgr    *BackupManager
}

// BackupManager 自动备份管理器
type BackupManager struct {
	db             *SQLiteDB
	backupDir      string
	interval       time.Duration
	maxBackups     int
	stopChan       chan struct{}
	running        bool
	mu             sync.Mutex
	lastBackupTime time.Time
	lastBackupPath string
	lastBackupErr  error
}

// NewSQLiteDB 创建新的 SQLite 数据库
func NewSQLiteDB(dbPath string) (*SQLiteDB, error) {
	// 打开数据库（启用 WAL 模式）
	db, err := sql.Open("sqlite3", fmt.Sprintf("file:%s?cache=shared&mode=rwc&_journal_mode=WAL", dbPath))
	if err != nil {
		return nil, fmt.Errorf("open sqlite failed: %w", err)
	}

	// WAL 性能优化
	if _, err := db.Exec("PRAGMA synchronous = NORMAL"); err != nil {
		return nil, err
	}
	if _, err := db.Exec("PRAGMA cache_size = -64000"); err != nil { // 64MB 缓存
		return nil, err
	}
	if _, err := db.Exec("PRAGMA temp_store = MEMORY"); err != nil {
		return nil, err
	}

	// 连接池配置
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(time.Hour)

	s := &SQLiteDB{db: db, dbPath: dbPath}

	// 初始化表结构
	if err := s.initTables(); err != nil {
		return nil, fmt.Errorf("init tables failed: %w", err)
	}

	return s, nil
}

// initTables 初始化表结构
func (s *SQLiteDB) initTables() error {
	schema := `
	-- 1. 任务表
	CREATE TABLE IF NOT EXISTS tasks (
		id TEXT PRIMARY KEY,
		name TEXT NOT NULL,
		status TEXT NOT NULL,
		phase TEXT NOT NULL,
		created_at TEXT NOT NULL,
		updated_at TEXT NOT NULL,
		source_cluster TEXT NOT NULL,
		target_cluster TEXT NOT NULL,
		source_password TEXT,
		target_password TEXT,
		migration_mode TEXT NOT NULL,
		num_workers INTEGER NOT NULL,
		keys_total INTEGER DEFAULT 0,
		keys_migrated INTEGER DEFAULT 0,
		keys_failed INTEGER DEFAULT 0,
		keys_skipped INTEGER DEFAULT 0,
		keys_filtered INTEGER DEFAULT 0,
		bytes_migrated INTEGER DEFAULT 0,
		bytes_total INTEGER DEFAULT 0,
		full_start_at TEXT,
		incr_start_at TEXT,
		completed_at TEXT,
		options TEXT
	);

	-- 2. Slot 状态表（核心：断点恢复）
	CREATE TABLE IF NOT EXISTS slot_status (
		task_id TEXT NOT NULL,
		slot INTEGER NOT NULL,
		worker_id INTEGER NOT NULL,
		status TEXT NOT NULL,
		keys_total INTEGER DEFAULT 0,
		keys_migrated INTEGER DEFAULT 0,
		bytes_migrated INTEGER DEFAULT 0,
		last_cursor TEXT DEFAULT '0',
		updated_at TEXT NOT NULL,
		PRIMARY KEY (task_id, slot),
		FOREIGN KEY (task_id) REFERENCES tasks(id) ON DELETE CASCADE
	);
	CREATE INDEX IF NOT EXISTS idx_slot_status_task_status ON slot_status(task_id, status);
	CREATE INDEX IF NOT EXISTS idx_slot_status_worker ON slot_status(task_id, worker_id);

	-- 3. Worker 状态表
	CREATE TABLE IF NOT EXISTS worker_status (
		task_id TEXT NOT NULL,
		worker_id INTEGER NOT NULL,
		pid INTEGER,
		status TEXT NOT NULL,
		assigned_slots TEXT,
		keys_migrated INTEGER DEFAULT 0,
		bytes_migrated INTEGER DEFAULT 0,
		current_slot INTEGER DEFAULT -1,
		last_heartbeat TEXT,
		created_at TEXT NOT NULL,
		updated_at TEXT NOT NULL,
		PRIMARY KEY (task_id, worker_id),
		FOREIGN KEY (task_id) REFERENCES tasks(id) ON DELETE CASCADE
	);

	-- 4. LevelDB 队列元数据
	CREATE TABLE IF NOT EXISTS queue_metadata (
		task_id TEXT NOT NULL,
		node_id TEXT NOT NULL,
		queue_path TEXT NOT NULL,
		enqueued_count INTEGER DEFAULT 0,
		dequeued_count INTEGER DEFAULT 0,
		pending_count INTEGER DEFAULT 0,
		last_enqueue_time TEXT,
		last_dequeue_time TEXT,
		PRIMARY KEY (task_id, node_id),
		FOREIGN KEY (task_id) REFERENCES tasks(id) ON DELETE CASCADE
	);

	-- 5. 进度快照表（用于 Web UI 图表）
	CREATE TABLE IF NOT EXISTS progress_snapshots (
		task_id TEXT NOT NULL,
		timestamp TEXT NOT NULL,
		phase TEXT NOT NULL,
		keys_migrated INTEGER NOT NULL,
		bytes_migrated INTEGER NOT NULL,
		speed INTEGER NOT NULL,
		PRIMARY KEY (task_id, timestamp),
		FOREIGN KEY (task_id) REFERENCES tasks(id) ON DELETE CASCADE
	);
	CREATE INDEX IF NOT EXISTS idx_progress_snapshots_task_time ON progress_snapshots(task_id, timestamp);

	-- 6. 增量同步断点表（用于增量同步恢复）
	CREATE TABLE IF NOT EXISTS incremental_checkpoints (
		task_id TEXT NOT NULL,
		node_id TEXT NOT NULL,
		last_event_id TEXT DEFAULT '',
		last_offset INTEGER DEFAULT 0,
		updated_at TEXT NOT NULL,
		PRIMARY KEY (task_id, node_id),
		FOREIGN KEY (task_id) REFERENCES tasks(id) ON DELETE CASCADE
	);

	-- 7. 数据库备份记录表
	CREATE TABLE IF NOT EXISTS backup_records (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		backup_path TEXT NOT NULL,
		backup_time TEXT NOT NULL,
		size_bytes INTEGER DEFAULT 0,
		status TEXT DEFAULT 'completed'
	);
	`

	_, err := s.db.Exec(schema)
	return err
}

// Close 关闭数据库
func (s *SQLiteDB) Close() error {
	// 停止自动备份
	if s.backupMgr != nil {
		s.backupMgr.Stop()
	}
	return s.db.Close()
}

// Begin 开始事务
func (s *SQLiteDB) Begin() (*sql.Tx, error) {
	return s.db.Begin()
}

// ========== 自动备份管理 ==========

// StartAutoBackup 启动自动备份
func (s *SQLiteDB) StartAutoBackup(backupDir string, interval time.Duration, maxBackups int) error {
	if s.backupMgr != nil && s.backupMgr.running {
		return fmt.Errorf("auto backup already running")
	}

	// 创建备份目录
	if err := os.MkdirAll(backupDir, 0755); err != nil {
		return fmt.Errorf("create backup dir failed: %w", err)
	}

	s.backupMgr = &BackupManager{
		db:         s,
		backupDir:  backupDir,
		interval:   interval,
		maxBackups: maxBackups,
		stopChan:   make(chan struct{}),
	}

	s.backupMgr.Start()
	return nil
}

// StopAutoBackup 停止自动备份
func (s *SQLiteDB) StopAutoBackup() {
	if s.backupMgr != nil {
		s.backupMgr.Stop()
	}
}

// GetBackupStatus 获取备份状态
func (s *SQLiteDB) GetBackupStatus() map[string]interface{} {
	if s.backupMgr == nil {
		return map[string]interface{}{
			"enabled": false,
		}
	}

	s.backupMgr.mu.Lock()
	defer s.backupMgr.mu.Unlock()

	status := map[string]interface{}{
		"enabled":       s.backupMgr.running,
		"backup_dir":    s.backupMgr.backupDir,
		"interval":      s.backupMgr.interval.String(),
		"max_backups":   s.backupMgr.maxBackups,
	}

	if !s.backupMgr.lastBackupTime.IsZero() {
		status["last_backup_time"] = s.backupMgr.lastBackupTime.Format(time.RFC3339)
		status["last_backup_path"] = s.backupMgr.lastBackupPath
	}

	if s.backupMgr.lastBackupErr != nil {
		status["last_error"] = s.backupMgr.lastBackupErr.Error()
	}

	return status
}

// ManualBackup 手动触发备份
func (s *SQLiteDB) ManualBackup() (string, error) {
	backupDir := "./data/backups"
	if s.backupMgr != nil {
		backupDir = s.backupMgr.backupDir
	}

	// 确保目录存在
	if err := os.MkdirAll(backupDir, 0755); err != nil {
		return "", fmt.Errorf("create backup dir failed: %w", err)
	}

	timestamp := time.Now().Format("20060102-150405")
	backupPath := filepath.Join(backupDir, fmt.Sprintf("tasks-%s.db", timestamp))

	if err := s.BackupDatabase(backupPath); err != nil {
		return "", err
	}

	// 记录备份
	s.recordBackup(backupPath)

	return backupPath, nil
}

// recordBackup 记录备份到数据库
func (s *SQLiteDB) recordBackup(backupPath string) {
	// 获取文件大小
	var size int64
	if info, err := os.Stat(backupPath); err == nil {
		size = info.Size()
	}

	s.db.Exec(`
		INSERT INTO backup_records (backup_path, backup_time, size_bytes, status)
		VALUES (?, ?, ?, 'completed')
	`, backupPath, time.Now().Format(time.RFC3339), size)
}

// ListBackups 列出所有备份
func (s *SQLiteDB) ListBackups() ([]map[string]interface{}, error) {
	rows, err := s.db.Query(`
		SELECT id, backup_path, backup_time, size_bytes, status
		FROM backup_records
		ORDER BY backup_time DESC
		LIMIT 50
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	backups := []map[string]interface{}{}
	for rows.Next() {
		var id int
		var path, backupTime, status string
		var size int64

		if err := rows.Scan(&id, &path, &backupTime, &size, &status); err != nil {
			continue
		}

		// 检查文件是否存在
		exists := false
		if _, err := os.Stat(path); err == nil {
			exists = true
		}

		backups = append(backups, map[string]interface{}{
			"id":          id,
			"path":        path,
			"backup_time": backupTime,
			"size_bytes":  size,
			"status":      status,
			"exists":      exists,
		})
	}

	return backups, nil
}

// CleanOldBackups 清理旧备份
func (s *SQLiteDB) CleanOldBackups(keepCount int) (int, error) {
	backups, err := s.ListBackups()
	if err != nil {
		return 0, err
	}

	if len(backups) <= keepCount {
		return 0, nil
	}

	// 删除多余的备份
	deleted := 0
	for i := keepCount; i < len(backups); i++ {
		path := backups[i]["path"].(string)
		id := backups[i]["id"].(int)

		// 删除文件
		if err := os.Remove(path); err == nil {
			deleted++
		}

		// 更新数据库记录
		s.db.Exec("UPDATE backup_records SET status = 'deleted' WHERE id = ?", id)
	}

	return deleted, nil
}

// Start 启动自动备份
func (bm *BackupManager) Start() {
	bm.mu.Lock()
	if bm.running {
		bm.mu.Unlock()
		return
	}
	bm.running = true
	bm.mu.Unlock()

	go bm.backupLoop()
}

// Stop 停止自动备份
func (bm *BackupManager) Stop() {
	bm.mu.Lock()
	if !bm.running {
		bm.mu.Unlock()
		return
	}
	bm.running = false
	close(bm.stopChan)
	bm.mu.Unlock()
}

// backupLoop 备份循环
func (bm *BackupManager) backupLoop() {
	ticker := time.NewTicker(bm.interval)
	defer ticker.Stop()

	// 启动时立即执行一次备份
	bm.doBackup()

	for {
		select {
		case <-bm.stopChan:
			return
		case <-ticker.C:
			bm.doBackup()
		}
	}
}

// doBackup 执行备份
func (bm *BackupManager) doBackup() {
	timestamp := time.Now().Format("20060102-150405")
	backupPath := filepath.Join(bm.backupDir, fmt.Sprintf("tasks-auto-%s.db", timestamp))

	err := bm.db.BackupDatabase(backupPath)

	bm.mu.Lock()
	bm.lastBackupTime = time.Now()
	bm.lastBackupErr = err
	if err == nil {
		bm.lastBackupPath = backupPath
		bm.db.recordBackup(backupPath)
	}
	bm.mu.Unlock()

	// 清理旧备份
	if err == nil && bm.maxBackups > 0 {
		bm.db.CleanOldBackups(bm.maxBackups)
	}
}

// ========== 任务操作 ==========

// Task 任务结构
type Task struct {
	ID             string `json:"id"`
	Name           string `json:"name"`
	Status         string `json:"status"`
	Phase          string `json:"phase"`
	CreatedAt      string `json:"created_at"`
	UpdatedAt      string `json:"updated_at"`
	SourceCluster  string `json:"source_cluster"`
	TargetCluster  string `json:"target_cluster"`
	SourcePassword string `json:"-"`
	TargetPassword string `json:"-"`
	MigrationMode  string `json:"migration_mode"`
	NumWorkers     int    `json:"num_workers"`
	KeysTotal      int64  `json:"keys_total"`
	KeysMigrated   int64  `json:"keys_migrated"`
	KeysFailed     int64  `json:"keys_failed"`
	KeysSkipped    int64  `json:"keys_skipped"`
	KeysFiltered   int64  `json:"keys_filtered"`
	BytesMigrated  int64  `json:"bytes_migrated"`
	BytesTotal     int64  `json:"bytes_total"`
	FullStartAt    string `json:"full_start_at,omitempty"`
	IncrStartAt    string `json:"incr_start_at,omitempty"`
	CompletedAt    string `json:"completed_at,omitempty"`
	Options        string `json:"options,omitempty"`
}

// CreateTask 创建任务
func (s *SQLiteDB) CreateTask(task *Task) error {
	query := `
		INSERT INTO tasks (
			id, name, status, phase, created_at, updated_at,
			source_cluster, target_cluster, source_password, target_password,
			migration_mode, num_workers, options
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`

	_, err := s.db.Exec(query,
		task.ID, task.Name, task.Status, task.Phase, task.CreatedAt, task.UpdatedAt,
		task.SourceCluster, task.TargetCluster, task.SourcePassword, task.TargetPassword,
		task.MigrationMode, task.NumWorkers, task.Options,
	)

	return err
}

// GetTask 获取任务
func (s *SQLiteDB) GetTask(taskID string) (*Task, error) {
	query := `
		SELECT id, name, status, phase, created_at, updated_at,
		       source_cluster, target_cluster, source_password, target_password,
		       migration_mode, num_workers, keys_total, keys_migrated, keys_failed,
		       keys_skipped, keys_filtered, bytes_migrated, bytes_total,
		       full_start_at, incr_start_at, completed_at, options
		FROM tasks WHERE id = ?
	`

	task := &Task{}
	err := s.db.QueryRow(query, taskID).Scan(
		&task.ID, &task.Name, &task.Status, &task.Phase, &task.CreatedAt, &task.UpdatedAt,
		&task.SourceCluster, &task.TargetCluster, &task.SourcePassword, &task.TargetPassword,
		&task.MigrationMode, &task.NumWorkers, &task.KeysTotal, &task.KeysMigrated, &task.KeysFailed,
		&task.KeysSkipped, &task.KeysFiltered, &task.BytesMigrated, &task.BytesTotal,
		&task.FullStartAt, &task.IncrStartAt, &task.CompletedAt, &task.Options,
	)

	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("task not found: %s", taskID)
	}

	return task, err
}

// UpdateTask 更新任务
func (s *SQLiteDB) UpdateTask(taskID string, updates map[string]interface{}) error {
	// 动态构建 UPDATE 语句
	query := "UPDATE tasks SET "
	args := []interface{}{}

	first := true
	for key, value := range updates {
		if !first {
			query += ", "
		}
		query += fmt.Sprintf("%s = ?", key)
		args = append(args, value)
		first = false
	}

	query += ", updated_at = ? WHERE id = ?"
	args = append(args, time.Now().Format(time.RFC3339), taskID)

	_, err := s.db.Exec(query, args...)
	return err
}

// ListTasks 列出所有任务
func (s *SQLiteDB) ListTasks() ([]*Task, error) {
	query := `
		SELECT id, name, status, phase, created_at, updated_at,
		       source_cluster, target_cluster, migration_mode, num_workers,
		       keys_total, keys_migrated, keys_failed, bytes_migrated,
		       full_start_at, incr_start_at, completed_at
		FROM tasks ORDER BY created_at DESC
	`

	rows, err := s.db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	tasks := []*Task{}
	for rows.Next() {
		task := &Task{}
		err := rows.Scan(
			&task.ID, &task.Name, &task.Status, &task.Phase, &task.CreatedAt, &task.UpdatedAt,
			&task.SourceCluster, &task.TargetCluster, &task.MigrationMode, &task.NumWorkers,
			&task.KeysTotal, &task.KeysMigrated, &task.KeysFailed, &task.BytesMigrated,
			&task.FullStartAt, &task.IncrStartAt, &task.CompletedAt,
		)
		if err != nil {
			return nil, err
		}
		tasks = append(tasks, task)
	}

	return tasks, rows.Err()
}

// DeleteTask 删除任务（级联删除相关数据）
func (s *SQLiteDB) DeleteTask(taskID string) error {
	_, err := s.db.Exec("DELETE FROM tasks WHERE id = ?", taskID)
	return err
}

// ========== 兼容 engine/master.go 的方法 ==========

// CreateTaskModel 创建任务（使用 model.Task）
func (s *SQLiteDB) CreateTaskModel(task *model.Task) error {
	query := `
		INSERT INTO tasks (
			id, name, status, phase, created_at, updated_at,
			source_cluster, target_cluster, migration_mode, num_workers, options
		) VALUES (?, ?, ?, 'pending', ?, ?, ?, ?, 'full', 4, ?)
	`
	
	now := time.Now().Format(time.RFC3339)
	_, err := s.db.Exec(query,
		task.ID, task.Name, task.Status, now, now,
		task.SourceCluster, task.TargetCluster, task.Config,
	)
	return err
}

// GetTaskModel 获取任务（返回 model.Task）
func (s *SQLiteDB) GetTaskModel(taskID string) (*model.Task, error) {
	query := `
		SELECT id, name, status, source_cluster, target_cluster, options, 
		       created_at, updated_at, full_start_at, completed_at
		FROM tasks WHERE id = ?
	`
	
	task := &model.Task{}
	var createdAt, updatedAt, fullStartAt, completedAt sql.NullString
	
	err := s.db.QueryRow(query, taskID).Scan(
		&task.ID, &task.Name, &task.Status, &task.SourceCluster, &task.TargetCluster, &task.Config,
		&createdAt, &updatedAt, &fullStartAt, &completedAt,
	)
	
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	
	// 解析时间
	if createdAt.Valid {
		if t, err := time.Parse(time.RFC3339, createdAt.String); err == nil {
			task.CreatedAt = t.Unix()
		}
	}
	if updatedAt.Valid {
		if t, err := time.Parse(time.RFC3339, updatedAt.String); err == nil {
			task.UpdatedAt = t.Unix()
		}
	}
	if fullStartAt.Valid && fullStartAt.String != "" {
		if t, err := time.Parse(time.RFC3339, fullStartAt.String); err == nil {
			ts := t.Unix()
			task.StartedAt = &ts
		}
	}
	if completedAt.Valid && completedAt.String != "" {
		if t, err := time.Parse(time.RFC3339, completedAt.String); err == nil {
			ts := t.Unix()
			task.CompletedAt = &ts
		}
	}
	
	return task, nil
}

// ListTasksWithFilter 获取任务列表（带过滤和分页）
func (s *SQLiteDB) ListTasksWithFilter(status string, page, size int) ([]*model.Task, int, error) {
	var query string
	var args []interface{}
	
	if status != "" {
		query = `SELECT id, name, status, source_cluster, target_cluster, options, 
		                created_at, updated_at, full_start_at, completed_at 
		         FROM tasks WHERE status = ? ORDER BY created_at DESC LIMIT ? OFFSET ?`
		args = []interface{}{status, size, (page - 1) * size}
	} else {
		query = `SELECT id, name, status, source_cluster, target_cluster, options, 
		                created_at, updated_at, full_start_at, completed_at 
		         FROM tasks ORDER BY created_at DESC LIMIT ? OFFSET ?`
		args = []interface{}{size, (page - 1) * size}
	}
	
	rows, err := s.db.Query(query, args...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	
	var tasks []*model.Task
	for rows.Next() {
		task := &model.Task{}
		var createdAt, updatedAt, fullStartAt, completedAt sql.NullString
		
		err := rows.Scan(
			&task.ID, &task.Name, &task.Status, &task.SourceCluster, &task.TargetCluster, &task.Config,
			&createdAt, &updatedAt, &fullStartAt, &completedAt,
		)
		if err != nil {
			continue
		}
		
		if createdAt.Valid {
			if t, err := time.Parse(time.RFC3339, createdAt.String); err == nil {
				task.CreatedAt = t.Unix()
			}
		}
		if updatedAt.Valid {
			if t, err := time.Parse(time.RFC3339, updatedAt.String); err == nil {
				task.UpdatedAt = t.Unix()
			}
		}
		if fullStartAt.Valid && fullStartAt.String != "" {
			if t, err := time.Parse(time.RFC3339, fullStartAt.String); err == nil {
				ts := t.Unix()
				task.StartedAt = &ts
			}
		}
		if completedAt.Valid && completedAt.String != "" {
			if t, err := time.Parse(time.RFC3339, completedAt.String); err == nil {
				ts := t.Unix()
				task.CompletedAt = &ts
			}
		}
		
		tasks = append(tasks, task)
	}
	
	// 获取总数
	var total int
	countQuery := "SELECT COUNT(*) FROM tasks"
	if status != "" {
		countQuery += " WHERE status = ?"
		s.db.QueryRow(countQuery, status).Scan(&total)
	} else {
		s.db.QueryRow(countQuery).Scan(&total)
	}
	
	return tasks, total, nil
}

// UpdateTaskStatus 更新任务状态
func (s *SQLiteDB) UpdateTaskStatus(taskID string, status model.TaskStatus) error {
	query := `UPDATE tasks SET status = ?, updated_at = ? WHERE id = ?`
	_, err := s.db.Exec(query, status, time.Now().Format(time.RFC3339), taskID)
	return err
}

// UpdateTaskStarted 更新任务启动时间
func (s *SQLiteDB) UpdateTaskStarted(taskID string) error {
	query := `UPDATE tasks SET status = 'running', full_start_at = ?, updated_at = ? WHERE id = ?`
	now := time.Now().Format(time.RFC3339)
	_, err := s.db.Exec(query, now, now, taskID)
	return err
}

// UpdateTaskCompleted 更新任务完成
func (s *SQLiteDB) UpdateTaskCompleted(taskID string, status model.TaskStatus) error {
	query := `UPDATE tasks SET status = ?, completed_at = ?, updated_at = ? WHERE id = ?`
	now := time.Now().Format(time.RFC3339)
	_, err := s.db.Exec(query, status, now, now, taskID)
	return err
}

// GetOrCreateStats 获取或创建统计
func (s *SQLiteDB) GetOrCreateStats(taskID string) (*model.MigrationStats, error) {
	stats := &model.MigrationStats{TaskID: taskID}
	
	// 从 tasks 表获取统计信息
	query := `SELECT keys_total, keys_migrated, bytes_total, bytes_migrated FROM tasks WHERE id = ?`
	err := s.db.QueryRow(query, taskID).Scan(&stats.TotalKeys, &stats.MigratedKeys, &stats.TotalBytes, &stats.MigratedBytes)
	if err == sql.ErrNoRows {
		return stats, nil
	}
	
	return stats, err
}

// UpdateStats 更新统计
func (s *SQLiteDB) UpdateStats(stats *model.MigrationStats) error {
	query := `UPDATE tasks SET keys_total = ?, keys_migrated = ?, bytes_total = ?, bytes_migrated = ?, updated_at = ? WHERE id = ?`
	_, err := s.db.Exec(query, stats.TotalKeys, stats.MigratedKeys, stats.TotalBytes, stats.MigratedBytes, time.Now().Format(time.RFC3339), stats.TaskID)
	return err
}

// IncrementStats 增量更新统计
func (s *SQLiteDB) IncrementStats(taskID string, keys, bytes int64) error {
	query := `UPDATE tasks SET keys_migrated = keys_migrated + ?, bytes_migrated = bytes_migrated + ?, updated_at = ? WHERE id = ?`
	_, err := s.db.Exec(query, keys, bytes, time.Now().Format(time.RFC3339), taskID)
	return err
}

// GetTaskProgressModel 获取任务进度（返回 model.Progress）
func (s *SQLiteDB) GetTaskProgressModel(taskID string) (*model.Progress, error) {
	query := `SELECT keys_total, keys_migrated, bytes_total, bytes_migrated, phase FROM tasks WHERE id = ?`
	
	progress := &model.Progress{}
	var phase string
	err := s.db.QueryRow(query, taskID).Scan(&progress.TotalKeys, &progress.MigratedKeys, &progress.TotalBytes, &progress.MigratedBytes, &phase)
	if err != nil {
		return nil, err
	}
	
	// 设置阶段
	switch phase {
	case "full":
		progress.Phase = model.PhaseFullMigration
	case "incremental":
		progress.Phase = model.PhaseIncrementalSync
	case "verify":
		progress.Phase = model.PhaseVerification
	}
	
	// 计算百分比
	if progress.TotalKeys > 0 {
		progress.Percentage = float64(progress.MigratedKeys) / float64(progress.TotalKeys) * 100
	}
	
	return progress, nil
}

// SaveSlotAssignment 保存 Slot 分配
// 重要修复：使用 INSERT OR IGNORE 避免覆盖已有的断点数据（last_cursor, keys_migrated 等）
// 如果 slot 已存在（从断点恢复场景），只更新 worker_id，保留断点信息
func (s *SQLiteDB) SaveSlotAssignment(assignment *model.SlotAssignment) error {
	now := time.Now().Format(time.RFC3339)
	for slot := assignment.SlotStart; slot <= assignment.SlotEnd; slot++ {
		// 先尝试插入新记录（如果不存在）
		result, err := s.db.Exec(
			`INSERT OR IGNORE INTO slot_status (task_id, slot, worker_id, status, last_cursor, keys_migrated, bytes_migrated, updated_at) 
			 VALUES (?, ?, ?, ?, '0', 0, 0, ?)`,
			assignment.TaskID, slot, assignment.WorkerID, assignment.Status, now,
		)
		if err != nil {
			return err
		}
		
		// 如果没有插入（记录已存在），只更新 worker_id，保留断点数据
		rowsAffected, _ := result.RowsAffected()
		if rowsAffected == 0 {
			_, err = s.db.Exec(
				`UPDATE slot_status SET worker_id = ?, updated_at = ? WHERE task_id = ? AND slot = ?`,
				assignment.WorkerID, now, assignment.TaskID, slot,
			)
			if err != nil {
				return err
			}
		}
	}
	
	return nil
}

// SaveCheckpoint 保存断点
func (s *SQLiteDB) SaveCheckpoint(cp *model.Checkpoint) error {
	query := `INSERT OR REPLACE INTO slot_status (task_id, slot, worker_id, status, last_cursor, keys_migrated, bytes_migrated, updated_at) 
	          VALUES (?, ?, ?, 'migrating', ?, ?, ?, ?)`
	_, err := s.db.Exec(query, cp.TaskID, cp.SlotID, cp.WorkerID, cp.Cursor, cp.KeysMigrated, cp.BytesMigrated, time.Now().Format(time.RFC3339))
	return err
}

// SaveVerifyResult 保存校验结果
func (s *SQLiteDB) SaveVerifyResult(result *model.VerifyResult) error {
	// 计算一致性率
	if result.TotalKeys > 0 {
		result.ConsistencyRate = float64(result.MatchedKeys) / float64(result.TotalKeys) * 100
	}
	
	// 创建校验结果表（如果不存在）
	s.db.Exec(`CREATE TABLE IF NOT EXISTS verify_results (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		task_id TEXT NOT NULL,
		batch_id TEXT NOT NULL,
		total_keys INTEGER,
		matched_keys INTEGER,
		mismatched_keys INTEGER,
		missing_keys INTEGER,
		extra_keys INTEGER,
		consistency_rate REAL,
		created_at TEXT
	)`)
	
	query := `INSERT INTO verify_results (task_id, batch_id, total_keys, matched_keys, mismatched_keys, missing_keys, extra_keys, consistency_rate, created_at) 
	          VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`
	_, err := s.db.Exec(query, result.TaskID, result.BatchID, result.TotalKeys, result.MatchedKeys, result.MismatchedKeys, result.MissingKeys, result.ExtraKeys, result.ConsistencyRate, time.Now().Format(time.RFC3339))
	return err
}

// GetVerifyResults 获取校验结果
func (s *SQLiteDB) GetVerifyResults(taskID string) ([]*model.VerifyResult, error) {
	// 确保表存在
	s.db.Exec(`CREATE TABLE IF NOT EXISTS verify_results (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		task_id TEXT NOT NULL,
		batch_id TEXT NOT NULL,
		total_keys INTEGER,
		matched_keys INTEGER,
		mismatched_keys INTEGER,
		missing_keys INTEGER,
		extra_keys INTEGER,
		consistency_rate REAL,
		created_at TEXT
	)`)
	
	query := `SELECT id, task_id, batch_id, total_keys, matched_keys, mismatched_keys, missing_keys, extra_keys, consistency_rate, created_at 
	          FROM verify_results WHERE task_id = ? ORDER BY created_at DESC`
	
	rows, err := s.db.Query(query, taskID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	
	var results []*model.VerifyResult
	for rows.Next() {
		result := &model.VerifyResult{}
		var createdAt string
		err := rows.Scan(&result.ID, &result.TaskID, &result.BatchID, &result.TotalKeys, &result.MatchedKeys, &result.MismatchedKeys, &result.MissingKeys, &result.ExtraKeys, &result.ConsistencyRate, &createdAt)
		if err != nil {
			continue
		}
		if t, err := time.Parse(time.RFC3339, createdAt); err == nil {
			result.CreatedAt = t.Unix()
		}
		results = append(results, result)
	}
	
	return results, nil
}

// GetMetrics 获取指标
func (s *SQLiteDB) GetMetrics(taskID string, startTime, endTime int64) ([]map[string]interface{}, error) {
	// 从 progress_snapshots 表获取
	query := `SELECT timestamp, phase, keys_migrated, bytes_migrated, speed 
	          FROM progress_snapshots 
	          WHERE task_id = ? AND timestamp >= ? AND timestamp <= ?
	          ORDER BY timestamp ASC`
	
	start := time.Unix(startTime, 0).Format(time.RFC3339)
	end := time.Unix(endTime, 0).Format(time.RFC3339)
	
	rows, err := s.db.Query(query, taskID, start, end)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	
	var metrics []map[string]interface{}
	for rows.Next() {
		var timestamp, phase string
		var keysMigrated, bytesMigrated, speed int64
		
		if err := rows.Scan(&timestamp, &phase, &keysMigrated, &bytesMigrated, &speed); err != nil {
			continue
		}
		
		metrics = append(metrics, map[string]interface{}{
			"timestamp":      timestamp,
			"phase":          phase,
			"keys_migrated":  keysMigrated,
			"bytes_migrated": bytesMigrated,
			"speed":          speed,
		})
	}
	
	return metrics, nil
}
