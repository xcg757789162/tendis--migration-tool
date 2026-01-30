<template>
  <div class="tasks-page">
    <div class="page-header">
      <div>
        <h1>迁移任务</h1>
        <p>管理所有数据迁移任务</p>
      </div>
      <el-button type="primary" @click="$router.push('/create')">
        <el-icon><Plus /></el-icon> 创建任务
      </el-button>
    </div>
    
    <!-- 筛选栏 -->
    <div class="filter-bar">
      <el-input
        v-model="searchText"
        placeholder="搜索任务名称..."
        prefix-icon="Search"
        clearable
        style="width: 280px"
      />
      
      <el-select v-model="statusFilter" placeholder="全部状态" clearable style="width: 140px">
        <el-option label="全部" value="" />
        <el-option label="待启动" value="pending" />
        <el-option label="运行中" value="running" />
        <el-option label="已暂停" value="paused" />
        <el-option label="已完成" value="completed" />
        <el-option label="失败" value="failed" />
      </el-select>
      
      <el-button @click="fetchTasks">
        <el-icon><Refresh /></el-icon> 刷新
      </el-button>
    </div>
    
    <!-- 任务列表 -->
    <div class="tasks-list">
      <el-table 
        :data="filteredTasks" 
        style="width: 100%"
        :row-class-name="getRowClass"
        @row-click="goToDetail"
      >
        <el-table-column label="任务名称" min-width="200">
          <template #default="{ row }">
            <div class="task-name-cell">
              <span class="name">{{ row.name }}</span>
              <el-tooltip :content="row.id" placement="top">
                <span class="id" @click.stop="copyTaskId(row.id)">{{ row.id }}</span>
              </el-tooltip>
            </div>
          </template>
        </el-table-column>
        
        <el-table-column label="状态" width="120">
          <template #default="{ row }">
            <span :class="['status-tag', row.status]">{{ getStatusText(row.status) }}</span>
          </template>
        </el-table-column>
        
        <el-table-column label="进度" min-width="200">
          <template #default="{ row }">
            <div class="progress-cell" v-if="row.progress">
              <div class="progress-bar">
                <div class="progress-inner" :style="{ width: (row.progress.percentage || 0) + '%' }"></div>
              </div>
              <span class="progress-text">{{ (row.progress.percentage || 0).toFixed(1) }}%</span>
            </div>
            <span v-else class="no-progress">-</span>
          </template>
        </el-table-column>
        
        <el-table-column label="迁移数据" width="160">
          <template #default="{ row }">
            <span v-if="row.progress">
              {{ formatNumber(row.progress.keys_migrated || row.progress.migrated_keys || 0) }} / {{ formatNumber(row.progress.keys_total || row.progress.total_keys || 0) }}
            </span>
            <span v-else>-</span>
          </template>
        </el-table-column>
        
        <el-table-column label="速度" width="120">
          <template #default="{ row }">
            <span v-if="row.progress && (row.progress.speed || row.progress.current_speed)">
              {{ formatNumber(row.progress.speed || row.progress.current_speed) }}/s
            </span>
            <span v-else>-</span>
          </template>
        </el-table-column>
        
        <el-table-column label="创建时间" width="180">
          <template #default="{ row }">
            {{ formatTime(row.created_at) }}
          </template>
        </el-table-column>
        
        <el-table-column label="操作" width="200" fixed="right">
          <template #default="{ row }">
            <div class="actions" @click.stop>
              <template v-if="row.status === 'pending'">
                <el-button size="small" type="primary" @click="startTask(row.id)">启动</el-button>
              </template>
              <template v-else-if="row.status === 'running'">
                <el-button size="small" @click="pauseTask(row.id)">暂停</el-button>
              </template>
              <template v-else-if="row.status === 'paused'">
                <el-button size="small" type="primary" @click="resumeTask(row.id)">恢复</el-button>
              </template>
              <el-button size="small" type="danger" @click="deleteTask(row)">删除</el-button>
            </div>
          </template>
        </el-table-column>
      </el-table>
      
      <!-- 分页 -->
      <div class="pagination">
        <el-pagination
          v-model:current-page="page"
          v-model:page-size="pageSize"
          :total="total"
          :page-sizes="[10, 20, 50]"
          layout="total, sizes, prev, pager, next"
          @size-change="fetchTasks"
          @current-change="fetchTasks"
        />
      </div>
    </div>
  </div>
</template>

<script setup>
import { ref, computed, onMounted, onUnmounted, watch } from 'vue'
import { useRouter, useRoute } from 'vue-router'
import { ElMessage, ElMessageBox } from 'element-plus'
import api from '@/api'
import dayjs from 'dayjs'

const router = useRouter()
const route = useRoute()

const tasks = ref([])
const total = ref(0)
const page = ref(1)
const pageSize = ref(20)
const searchText = ref('')
const statusFilter = ref('')
let refreshTimer = null

// 监听路由参数变化
watch(() => route.query.status, (newStatus) => {
  statusFilter.value = newStatus || ''
  fetchTasks()
}, { immediate: true })

const filteredTasks = computed(() => {
  let result = tasks.value
  if (searchText.value) {
    result = result.filter(t => t.name.toLowerCase().includes(searchText.value.toLowerCase()))
  }
  return result
})

const fetchTasks = async () => {
  try {
    const data = await api.getTasks({
      page: page.value,
      size: pageSize.value,
      status: statusFilter.value
    })
    tasks.value = data.items || []
    total.value = data.total || 0
  } catch (err) {
    console.error('Fetch tasks failed:', err)
  }
}

const startTask = async (id) => {
  try {
    await api.startTask(id)
    ElMessage.success('任务已启动')
    fetchTasks()
  } catch (err) {
    ElMessage.error('启动失败')
  }
}

const pauseTask = async (id) => {
  try {
    await api.pauseTask(id)
    ElMessage.success('任务已暂停')
    fetchTasks()
  } catch (err) {
    ElMessage.error('暂停失败')
  }
}

const resumeTask = async (id) => {
  try {
    await api.resumeTask(id)
    ElMessage.success('任务已恢复')
    fetchTasks()
  } catch (err) {
    ElMessage.error('恢复失败')
  }
}

const deleteTask = async (task) => {
  try {
    await ElMessageBox.confirm(
      `确定要删除任务 "${task.name}" 吗？此操作不可恢复。`,
      '删除确认',
      { confirmButtonText: '删除', cancelButtonText: '取消', type: 'warning' }
    )
    await api.deleteTask(task.id)
    ElMessage.success('任务已删除')
    fetchTasks()
  } catch (err) {
    if (err !== 'cancel') {
      ElMessage.error('删除失败')
    }
  }
}

const goToDetail = (row) => {
  router.push(`/tasks/${row.id}`)
}

const getStatusText = (status) => {
  const map = {
    pending: '待启动',
    running: '运行中',
    paused: '已暂停',
    completed: '已完成',
    failed: '失败'
  }
  return map[status] || status
}

const getRowClass = ({ row }) => {
  return `row-${row.status}`
}

const formatNumber = (num) => {
  if (num >= 1000000000) return (num / 1000000000).toFixed(1) + 'B'
  if (num >= 1000000) return (num / 1000000).toFixed(1) + 'M'
  if (num >= 1000) return (num / 1000).toFixed(1) + 'K'
  return num.toString()
}

const formatTime = (time) => {
  return dayjs(time).format('YYYY-MM-DD HH:mm:ss')
}

const copyToClipboard = (text) => {
  if (navigator.clipboard && window.isSecureContext) {
    return navigator.clipboard.writeText(text)
  }
  // Fallback for non-HTTPS
  const textarea = document.createElement('textarea')
  textarea.value = text
  textarea.style.position = 'fixed'
  textarea.style.left = '-9999px'
  document.body.appendChild(textarea)
  textarea.select()
  try {
    document.execCommand('copy')
    return Promise.resolve()
  } catch (e) {
    return Promise.reject(e)
  } finally {
    document.body.removeChild(textarea)
  }
}

const copyTaskId = async (id) => {
  try {
    await copyToClipboard(id)
    ElMessage.success('任务ID已复制')
  } catch (err) {
    ElMessage.error('复制失败')
  }
}

onMounted(() => {
  fetchTasks()
  refreshTimer = setInterval(fetchTasks, 10000)
})

onUnmounted(() => {
  if (refreshTimer) clearInterval(refreshTimer)
})
</script>

<style lang="scss" scoped>
.tasks-page {
  max-width: 1400px;
  margin: 0 auto;
}

.page-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 24px;
  
  h1 {
    font-size: 28px;
    font-weight: 700;
    color: var(--text-primary);
    margin-bottom: 4px;
  }
  
  p {
    color: var(--text-secondary);
  }
}

.filter-bar {
  display: flex;
  gap: 16px;
  margin-bottom: 24px;
  padding: 20px;
  background: var(--bg-card);
  border-radius: var(--radius-lg);
  box-shadow: var(--shadow-card);
  border: 1px solid var(--border-light);
}

.tasks-list {
  background: var(--bg-card);
  border-radius: var(--radius-lg);
  box-shadow: var(--shadow-card);
  border: 1px solid var(--border-light);
  overflow: hidden;
  
  :deep(.el-table) {
    .el-table__row {
      cursor: pointer;
      
      &:hover {
        background: var(--bg-hover) !important;
      }
    }
  }
}

.task-name-cell {
  .name {
    display: block;
    font-weight: 500;
    color: var(--text-primary);
  }
  
  .id {
    font-size: 12px;
    color: var(--text-tertiary);
    font-family: monospace;
  }
}

.progress-cell {
  display: flex;
  align-items: center;
  gap: 12px;
  
  .progress-bar {
    flex: 1;
    height: 6px;
    background: var(--border-light);
    border-radius: 3px;
    overflow: hidden;
    
    .progress-inner {
      height: 100%;
      background: var(--gradient-blue);
      border-radius: 3px;
      transition: width 0.5s ease;
    }
  }
  
  .progress-text {
    font-size: 13px;
    font-weight: 500;
    color: var(--primary-color);
    min-width: 50px;
  }
}

.no-progress {
  color: var(--text-tertiary);
}

.actions {
  display: flex;
  gap: 8px;
}

.pagination {
  padding: 20px;
  display: flex;
  justify-content: flex-end;
  border-top: 1px solid var(--border-light);
}
</style>
