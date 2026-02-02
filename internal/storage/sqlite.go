package storage

import (
	"database/sql"
	"fmt"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

// SQLiteDB SQLite 数据库封装
type SQLiteDB struct {
	db *sql.DB
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

	s := &SQLiteDB{db: db}

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
	`

	_, err := s.db.Exec(schema)
	return err
}

// Close 关闭数据库
func (s *SQLiteDB) Close() error {
	return s.db.Close()
}

// Begin 开始事务
func (s *SQLiteDB) Begin() (*sql.Tx, error) {
	return s.db.Begin()
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
