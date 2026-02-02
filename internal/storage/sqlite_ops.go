package storage

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"time"
)

// ========== Slot 状态操作 ==========

// SlotStatus Slot 状态结构
type SlotStatus struct {
	TaskID        string `json:"task_id"`
	Slot          int    `json:"slot"`
	WorkerID      int    `json:"worker_id"`
	Status        string `json:"status"`
	KeysTotal     int64  `json:"keys_total"`
	KeysMigrated  int64  `json:"keys_migrated"`
	BytesMigrated int64  `json:"bytes_migrated"`
	LastCursor    string `json:"last_cursor"`
	UpdatedAt     string `json:"updated_at"`
}

// InitSlots 初始化任务的所有 Slot（16384 个）
func (s *SQLiteDB) InitSlots(taskID string, numWorkers int) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(`
		INSERT INTO slot_status (task_id, slot, worker_id, status, last_cursor, updated_at)
		VALUES (?, ?, ?, 'pending', '0', ?)
	`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	now := time.Now().Format(time.RFC3339)
	slotsPerWorker := 16384 / numWorkers

	for slot := 0; slot < 16384; slot++ {
		workerID := slot / slotsPerWorker
		if workerID >= numWorkers {
			workerID = numWorkers - 1
		}

		if _, err := stmt.Exec(taskID, slot, workerID, now); err != nil {
			return err
		}
	}

	return tx.Commit()
}

// GetSlotStatus 获取 Slot 状态
func (s *SQLiteDB) GetSlotStatus(taskID string, slot int) (*SlotStatus, error) {
	query := `
		SELECT task_id, slot, worker_id, status, keys_total, keys_migrated,
		       bytes_migrated, last_cursor, updated_at
		FROM slot_status
		WHERE task_id = ? AND slot = ?
	`

	status := &SlotStatus{}
	err := s.db.QueryRow(query, taskID, slot).Scan(
		&status.TaskID, &status.Slot, &status.WorkerID, &status.Status,
		&status.KeysTotal, &status.KeysMigrated, &status.BytesMigrated,
		&status.LastCursor, &status.UpdatedAt,
	)

	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("slot not found: %d", slot)
	}

	return status, err
}

// UpdateSlotCheckpoint 更新 Slot 断点
func (s *SQLiteDB) UpdateSlotCheckpoint(taskID string, slot int, cursor string, keysMigrated int64) error {
	query := `
		UPDATE slot_status
		SET last_cursor = ?, keys_migrated = ?, updated_at = ?
		WHERE task_id = ? AND slot = ?
	`

	_, err := s.db.Exec(query, cursor, keysMigrated, time.Now().Format(time.RFC3339), taskID, slot)
	return err
}

// GetSlotCheckpoint 获取 Slot 断点信息
func (s *SQLiteDB) GetSlotCheckpoint(taskID string, slot int) (string, error) {
	var cursor string
	err := s.db.QueryRow(`
		SELECT last_cursor FROM slot_status 
		WHERE task_id = ? AND slot = ?
	`, taskID, slot).Scan(&cursor)
	
	if err != nil {
		return "", err
	}
	return cursor, nil
}

// GetAllPendingSlots 获取所有待处理的 Slot（用于断点恢复）
func (s *SQLiteDB) GetAllPendingSlots(taskID string) ([]int, error) {
	rows, err := s.db.Query(`
		SELECT slot FROM slot_status 
		WHERE task_id = ? AND status IN ('pending', 'in_progress')
		ORDER BY slot
	`, taskID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var slots []int
	for rows.Next() {
		var slot int
		if err := rows.Scan(&slot); err != nil {
			return nil, err
		}
		slots = append(slots, slot)
	}

	return slots, nil
}

// GetTaskProgress 获取任务进度摘要
func (s *SQLiteDB) GetTaskProgress(taskID string) (map[string]interface{}, error) {
	var completed, inProgress, failed, pending int
	err := s.db.QueryRow(`
		SELECT 
			COUNT(CASE WHEN status = 'completed' THEN 1 END) as completed,
			COUNT(CASE WHEN status = 'in_progress' THEN 1 END) as in_progress,
			COUNT(CASE WHEN status = 'failed' THEN 1 END) as failed,
			COUNT(CASE WHEN status = 'pending' THEN 1 END) as pending
		FROM slot_status 
		WHERE task_id = ?
	`, taskID).Scan(&completed, &inProgress, &failed, &pending)

	if err != nil {
		return nil, err
	}

	return map[string]interface{}{
		"completed":   completed,
		"in_progress": inProgress,
		"failed":      failed,
		"pending":     pending,
		"total":       16384,
	}, nil
}

// UpdateSlotStatus 更新 Slot 状态
func (s *SQLiteDB) UpdateSlotStatus(taskID string, slot int, status string) error {
	query := `
		UPDATE slot_status
		SET status = ?, updated_at = ?
		WHERE task_id = ? AND slot = ?
	`

	_, err := s.db.Exec(query, status, time.Now().Format(time.RFC3339), taskID, slot)
	return err
}

// GetWorkerSlots 获取 Worker 分配的所有 Slot
func (s *SQLiteDB) GetWorkerSlots(taskID string, workerID int) ([]*SlotStatus, error) {
	query := `
		SELECT task_id, slot, worker_id, status, keys_total, keys_migrated,
		       bytes_migrated, last_cursor, updated_at
		FROM slot_status
		WHERE task_id = ? AND worker_id = ?
		ORDER BY slot
	`

	rows, err := s.db.Query(query, taskID, workerID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	slots := []*SlotStatus{}
	for rows.Next() {
		status := &SlotStatus{}
		err := rows.Scan(
			&status.TaskID, &status.Slot, &status.WorkerID, &status.Status,
			&status.KeysTotal, &status.KeysMigrated, &status.BytesMigrated,
			&status.LastCursor, &status.UpdatedAt,
		)
		if err != nil {
			return nil, err
		}
		slots = append(slots, status)
	}

	return slots, rows.Err()
}

// CountSlotsByStatus 统计指定状态的 Slot 数量
func (s *SQLiteDB) CountSlotsByStatus(taskID string, status string) (int, error) {
	query := "SELECT COUNT(*) FROM slot_status WHERE task_id = ? AND status = ?"
	var count int
	err := s.db.QueryRow(query, taskID, status).Scan(&count)
	return count, err
}

// ========== Worker 状态操作 ==========

// WorkerStatus Worker 状态结构
type WorkerStatus struct {
	TaskID         string `json:"task_id"`
	WorkerID       int    `json:"worker_id"`
	PID            int    `json:"pid"`
	Status         string `json:"status"`
	AssignedSlots  []int  `json:"assigned_slots"`
	KeysMigrated   int64  `json:"keys_migrated"`
	BytesMigrated  int64  `json:"bytes_migrated"`
	CurrentSlot    int    `json:"current_slot"`
	LastHeartbeat  string `json:"last_heartbeat"`
	CreatedAt      string `json:"created_at"`
	UpdatedAt      string `json:"updated_at"`
}

// CreateWorker 创建 Worker 记录
func (s *SQLiteDB) CreateWorker(worker *WorkerStatus) error {
	slotsJSON, _ := json.Marshal(worker.AssignedSlots)

	query := `
		INSERT INTO worker_status (
			task_id, worker_id, pid, status, assigned_slots,
			keys_migrated, bytes_migrated, current_slot,
			last_heartbeat, created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`

	_, err := s.db.Exec(query,
		worker.TaskID, worker.WorkerID, worker.PID, worker.Status, string(slotsJSON),
		worker.KeysMigrated, worker.BytesMigrated, worker.CurrentSlot,
		worker.LastHeartbeat, worker.CreatedAt, worker.UpdatedAt,
	)

	return err
}

// GetWorker 获取 Worker 状态
func (s *SQLiteDB) GetWorker(taskID string, workerID int) (*WorkerStatus, error) {
	query := `
		SELECT task_id, worker_id, pid, status, assigned_slots,
		       keys_migrated, bytes_migrated, current_slot,
		       last_heartbeat, created_at, updated_at
		FROM worker_status
		WHERE task_id = ? AND worker_id = ?
	`

	worker := &WorkerStatus{}
	var slotsJSON string

	err := s.db.QueryRow(query, taskID, workerID).Scan(
		&worker.TaskID, &worker.WorkerID, &worker.PID, &worker.Status, &slotsJSON,
		&worker.KeysMigrated, &worker.BytesMigrated, &worker.CurrentSlot,
		&worker.LastHeartbeat, &worker.CreatedAt, &worker.UpdatedAt,
	)

	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("worker not found: %d", workerID)
	}

	if err == nil && slotsJSON != "" {
		json.Unmarshal([]byte(slotsJSON), &worker.AssignedSlots)
	}

	return worker, err
}

// UpdateWorkerHeartbeat 更新 Worker 心跳
func (s *SQLiteDB) UpdateWorkerHeartbeat(taskID string, workerID int, keysMigrated, bytesMigrated int64, currentSlot int) error {
	query := `
		UPDATE worker_status
		SET keys_migrated = ?, bytes_migrated = ?, current_slot = ?,
		    last_heartbeat = ?, updated_at = ?
		WHERE task_id = ? AND worker_id = ?
	`

	now := time.Now().Format(time.RFC3339)
	_, err := s.db.Exec(query, keysMigrated, bytesMigrated, currentSlot, now, now, taskID, workerID)
	return err
}

// UpdateWorkerStatus 更新 Worker 状态
func (s *SQLiteDB) UpdateWorkerStatus(taskID string, workerID int, status string) error {
	query := `
		UPDATE worker_status
		SET status = ?, updated_at = ?
		WHERE task_id = ? AND worker_id = ?
	`

	_, err := s.db.Exec(query, status, time.Now().Format(time.RFC3339), taskID, workerID)
	return err
}

// ListWorkers 列出任务的所有 Worker
func (s *SQLiteDB) ListWorkers(taskID string) ([]*WorkerStatus, error) {
	query := `
		SELECT task_id, worker_id, pid, status, assigned_slots,
		       keys_migrated, bytes_migrated, current_slot,
		       last_heartbeat, created_at, updated_at
		FROM worker_status
		WHERE task_id = ?
		ORDER BY worker_id
	`

	rows, err := s.db.Query(query, taskID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	workers := []*WorkerStatus{}
	for rows.Next() {
		worker := &WorkerStatus{}
		var slotsJSON string

		err := rows.Scan(
			&worker.TaskID, &worker.WorkerID, &worker.PID, &worker.Status, &slotsJSON,
			&worker.KeysMigrated, &worker.BytesMigrated, &worker.CurrentSlot,
			&worker.LastHeartbeat, &worker.CreatedAt, &worker.UpdatedAt,
		)
		if err != nil {
			return nil, err
		}

		if slotsJSON != "" {
			json.Unmarshal([]byte(slotsJSON), &worker.AssignedSlots)
		}

		workers = append(workers, worker)
	}

	return workers, rows.Err()
}

// ========== 进度快照操作 ==========

// ProgressSnapshot 进度快照结构
type ProgressSnapshot struct {
	TaskID        string `json:"task_id"`
	Timestamp     string `json:"timestamp"`
	Phase         string `json:"phase"`
	KeysMigrated  int64  `json:"keys_migrated"`
	BytesMigrated int64  `json:"bytes_migrated"`
	Speed         int64  `json:"speed"` // keys/s
}

// AddProgressSnapshot 添加进度快照
func (s *SQLiteDB) AddProgressSnapshot(snapshot *ProgressSnapshot) error {
	query := `
		INSERT INTO progress_snapshots (task_id, timestamp, phase, keys_migrated, bytes_migrated, speed)
		VALUES (?, ?, ?, ?, ?, ?)
	`

	_, err := s.db.Exec(query,
		snapshot.TaskID, snapshot.Timestamp, snapshot.Phase,
		snapshot.KeysMigrated, snapshot.BytesMigrated, snapshot.Speed,
	)

	return err
}

// GetProgressSnapshots 获取进度快照列表
func (s *SQLiteDB) GetProgressSnapshots(taskID string, limit int) ([]*ProgressSnapshot, error) {
	query := `
		SELECT task_id, timestamp, phase, keys_migrated, bytes_migrated, speed
		FROM progress_snapshots
		WHERE task_id = ?
		ORDER BY timestamp DESC
		LIMIT ?
	`

	rows, err := s.db.Query(query, taskID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	snapshots := []*ProgressSnapshot{}
	for rows.Next() {
		snapshot := &ProgressSnapshot{}
		err := rows.Scan(
			&snapshot.TaskID, &snapshot.Timestamp, &snapshot.Phase,
			&snapshot.KeysMigrated, &snapshot.BytesMigrated, &snapshot.Speed,
		)
		if err != nil {
			return nil, err
		}
		snapshots = append(snapshots, snapshot)
	}

	return snapshots, rows.Err()
}
