package master

import (
	"fmt"
	"sync"

	"tendis-migrate/internal/storage"
)

// SlotManager Slot 分配管理器
type SlotManager struct {
	db             *storage.SQLiteDB
	taskID         string
	numWorkers     int
	slotAssignment map[int][]int // workerID -> []slot
	mu             sync.RWMutex
}

// NewSlotManager 创建 Slot 管理器
func NewSlotManager(db *storage.SQLiteDB, taskID string, numWorkers int) *SlotManager {
	return &SlotManager{
		db:             db,
		taskID:         taskID,
		numWorkers:     numWorkers,
		slotAssignment: make(map[int][]int),
	}
}

// InitialAssignment 初始化 Slot 分配（静态分配）
// 将 16384 个 Slot 均匀分配给 N 个 Worker
func (sm *SlotManager) InitialAssignment() error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	totalSlots := 16384
	slotsPerWorker := totalSlots / sm.numWorkers
	remainder := totalSlots % sm.numWorkers

	slotID := 0
	for workerID := 0; workerID < sm.numWorkers; workerID++ {
		slots := []int{}
		
		// 基础分配
		count := slotsPerWorker
		
		// 剩余的 Slot 分配给前面的 Worker
		if workerID < remainder {
			count++
		}

		// 分配 Slot 范围
		for i := 0; i < count; i++ {
			slots = append(slots, slotID)
			slotID++
		}

		sm.slotAssignment[workerID] = slots
	}

	// 持久化到数据库
	return sm.db.InitSlots(sm.taskID, sm.numWorkers)
}

// GetWorkerSlots 获取 Worker 分配的 Slot 列表
func (sm *SlotManager) GetWorkerSlots(workerID int) []int {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return sm.slotAssignment[workerID]
}

// GetAllAssignments 获取所有分配信息
func (sm *SlotManager) GetAllAssignments() map[int][]int {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	result := make(map[int][]int)
	for k, v := range sm.slotAssignment {
		result[k] = append([]int(nil), v...)
	}
	return result
}

// ReassignWorkerSlots 重新分配 Worker 的 Slot（Worker 崩溃时）
// 将失败的 Worker 的 Slot 重新分配给其他 Worker
func (sm *SlotManager) ReassignWorkerSlots(failedWorkerID int, targetWorkerIDs []int) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	// 获取失败 Worker 的未完成 Slot
	pendingSlots, err := sm.getPendingSlots(failedWorkerID)
	if err != nil {
		return fmt.Errorf("get pending slots failed: %w", err)
	}

	if len(pendingSlots) == 0 {
		return nil // 所有 Slot 已完成
	}

	// 均匀分配给目标 Worker
	slotsPerWorker := len(pendingSlots) / len(targetWorkerIDs)
	remainder := len(pendingSlots) % len(targetWorkerIDs)

	slotIdx := 0
	for i := range targetWorkerIDs {
		count := slotsPerWorker
		if i < remainder {
			count++
		}

		for j := 0; j < count && slotIdx < len(pendingSlots); j++ {
			slot := pendingSlots[slotIdx]
			slotIdx++

			// 更新数据库中的 Slot 分配
			if err := sm.db.UpdateSlotStatus(sm.taskID, slot, "pending"); err != nil {
				return err
			}

			// 更新 Worker ID
			// 注意：这里需要数据库支持更新 worker_id
			// 暂时先不实现，后续可以通过 SQL 更新
		}
	}

	return nil
}

// getPendingSlots 获取 Worker 未完成的 Slot
func (sm *SlotManager) getPendingSlots(workerID int) ([]int, error) {
	slots, err := sm.db.GetWorkerSlots(sm.taskID, workerID)
	if err != nil {
		return nil, err
	}

	pendingSlots := []int{}
	for _, slot := range slots {
		if slot.Status != "completed" {
			pendingSlots = append(pendingSlots, slot.Slot)
		}
	}

	return pendingSlots, nil
}

// GetProgress 获取整体进度
func (sm *SlotManager) GetProgress() (*SlotProgress, error) {
	completed, err := sm.db.CountSlotsByStatus(sm.taskID, "completed")
	if err != nil {
		return nil, err
	}

	migrating, err := sm.db.CountSlotsByStatus(sm.taskID, "migrating")
	if err != nil {
		return nil, err
	}

	failed, err := sm.db.CountSlotsByStatus(sm.taskID, "failed")
	if err != nil {
		return nil, err
	}

	pending, err := sm.db.CountSlotsByStatus(sm.taskID, "pending")
	if err != nil {
		return nil, err
	}

	return &SlotProgress{
		Total:     16384,
		Completed: completed,
		Migrating: migrating,
		Failed:    failed,
		Pending:   pending,
		Percentage: float64(completed) / 16384.0 * 100,
	}, nil
}

// SlotProgress Slot 进度统计
type SlotProgress struct {
	Total      int     `json:"total"`
	Completed  int     `json:"completed"`
	Migrating  int     `json:"migrating"`
	Failed     int     `json:"failed"`
	Pending    int     `json:"pending"`
	Percentage float64 `json:"percentage"`
}

// GetSlotDistribution 获取 Slot 分配分布（用于可视化）
func (sm *SlotManager) GetSlotDistribution() map[int]*WorkerSlotInfo {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	result := make(map[int]*WorkerSlotInfo)
	for workerID, slots := range sm.slotAssignment {
		minSlot := slots[0]
		maxSlot := slots[len(slots)-1]

		result[workerID] = &WorkerSlotInfo{
			WorkerID:  workerID,
			SlotCount: len(slots),
			SlotRange: fmt.Sprintf("%d-%d", minSlot, maxSlot),
		}
	}

	return result
}

// WorkerSlotInfo Worker Slot 分配信息
type WorkerSlotInfo struct {
	WorkerID  int    `json:"worker_id"`
	SlotCount int    `json:"slot_count"`
	SlotRange string `json:"slot_range"`
}
