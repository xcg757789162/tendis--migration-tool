<template>
  <div class="dashboard">
    <!-- 页面标题 -->
    <div class="page-header">
      <h1>控制台</h1>
      <p>实时监控迁移任务状态和系统运行情况</p>
    </div>
    
    <!-- 统计卡片 -->
    <div class="stats-grid">
      <div class="stat-card clickable" @click="goToTasks('all')">
        <div class="stat-icon" style="background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);">
          <el-icon :size="24"><Tickets /></el-icon>
        </div>
        <div class="stat-content">
          <div class="stat-number">{{ stats.totalTasks }}</div>
          <div class="stat-label">总任务数</div>
        </div>
      </div>
      
      <div class="stat-card clickable" @click="goToTasks('running')">
        <div class="stat-icon" style="background: linear-gradient(135deg, #10b981 0%, #34d399 100%);">
          <el-icon :size="24"><VideoPlay /></el-icon>
        </div>
        <div class="stat-content">
          <div class="stat-number">{{ stats.runningTasks }}</div>
          <div class="stat-label">运行中</div>
        </div>
      </div>
      
      <div class="stat-card clickable" @click="goToTasks('paused')">
        <div class="stat-icon" style="background: linear-gradient(135deg, #f59e0b 0%, #fbbf24 100%);">
          <el-icon :size="24"><VideoPause /></el-icon>
        </div>
        <div class="stat-content">
          <div class="stat-number">{{ stats.pausedTasks }}</div>
          <div class="stat-label">已暂停</div>
        </div>
      </div>
      
      <div class="stat-card clickable" @click="goToTasks('completed')">
        <div class="stat-icon" style="background: linear-gradient(135deg, #2563eb 0%, #06b6d4 100%);">
          <el-icon :size="24"><CircleCheck /></el-icon>
        </div>
        <div class="stat-content">
          <div class="stat-number">{{ stats.completedTasks }}</div>
          <div class="stat-label">已完成</div>
        </div>
      </div>
    </div>
    
    <!-- 进行中和暂停的任务 -->
    <div class="section">
      <div class="section-header">
        <h2>
          <el-icon><VideoPlay /></el-icon>
          进行中的任务
        </h2>
        <router-link to="/tasks" class="view-all">查看全部 <el-icon><ArrowRight /></el-icon></router-link>
      </div>
      
      <div class="running-tasks" v-if="activeTasks.length">
        <div class="task-card" v-for="task in activeTasks" :key="task.id">
          <div class="task-header">
            <div class="task-name">{{ task.name }}</div>
            <span :class="['status-tag', task.status]">{{ task.status === 'running' ? '运行中' : '已暂停' }}</span>
          </div>
          
          <div class="task-progress">
            <div class="progress-info">
              <span>迁移进度</span>
              <span class="progress-percent">{{ task.progress?.percentage?.toFixed(1) || 0 }}%</span>
            </div>
            <div class="progress-bar">
              <div class="progress-inner" :style="{ width: (task.progress?.percentage || 0) + '%' }"></div>
            </div>
          </div>
          
          <div class="task-stats">
            <div class="stat-item">
              <span class="label">已迁移</span>
              <span class="value">{{ formatNumber(task.progress?.keys_migrated || task.progress?.migrated_keys || 0) }}</span>
            </div>
            <div class="stat-item">
              <span class="label">总Key数</span>
              <span class="value">{{ formatNumber(task.progress?.keys_total || task.progress?.total_keys || 0) }}</span>
            </div>
            <div class="stat-item">
              <span class="label">速度</span>
              <span class="value">{{ formatNumber(task.progress?.speed || task.progress?.current_speed || 0) }} keys/s</span>
            </div>
            <div class="stat-item">
              <span class="label">预计剩余</span>
              <span class="value">{{ task.progress?.estimated_eta || '-' }}</span>
            </div>
          </div>
          
          <div class="task-actions">
            <el-button v-if="task.status === 'running'" size="small" @click="pauseTask(task.id)">
              <el-icon><VideoPause /></el-icon> 暂停
            </el-button>
            <el-button v-if="task.status === 'paused'" size="small" type="success" @click="resumeTask(task.id)">
              <el-icon><VideoPlay /></el-icon> 恢复
            </el-button>
            <el-button size="small" type="primary" @click="$router.push(`/tasks/${task.id}`)">
              <el-icon><View /></el-icon> 详情
            </el-button>
          </div>
        </div>
      </div>
      
      <el-empty v-else description="暂无进行中的任务">
        <el-button type="primary" @click="$router.push('/create')">创建任务</el-button>
      </el-empty>
    </div>
    
    <!-- 系统状态 -->
    <div class="section">
      <div class="section-header">
        <h2>
          <el-icon><Monitor /></el-icon>
          系统状态
        </h2>
        <div class="system-actions">
          <el-popconfirm
            title="确定要创建系统备份吗？"
            @confirm="createBackup"
          >
            <template #reference>
              <el-button size="small" :loading="backingUp">
                <el-icon><FolderAdd /></el-icon> 系统备份
              </el-button>
            </template>
          </el-popconfirm>
          <el-button size="small" @click="showBackupDialog">
            <el-icon><Folder /></el-icon> 管理备份
          </el-button>
        </div>
      </div>
      
      <div class="system-info">
        <div class="info-card">
          <div class="info-icon">
            <el-icon :size="20"><Cpu /></el-icon>
          </div>
          <div class="info-content">
            <div class="info-label">Worker 进程</div>
            <div class="info-value">
              <span v-if="systemStatus.running_tasks > 0">
                {{ systemStatus.active_workers || 0 }} / {{ systemStatus.target_workers || 0 }} 活跃
              </span>
              <span v-else class="idle">空闲</span>
            </div>
          </div>
        </div>
        
        <div class="info-card">
          <div class="info-icon">
            <el-icon :size="20"><Connection /></el-icon>
          </div>
          <div class="info-content">
            <div class="info-label">系统状态</div>
            <div class="info-value" :class="systemStatus.status">{{ systemStatus.status === 'running' ? '正常运行' : '检查中' }}</div>
          </div>
        </div>
        
        <div class="info-card">
          <div class="info-icon">
            <el-icon :size="20"><Odometer /></el-icon>
          </div>
          <div class="info-content">
            <div class="info-label">内存使用</div>
            <div class="info-value">{{ formatBytes(systemStatus.memory_usage || 0) }}</div>
          </div>
        </div>
        
        <div class="info-card">
          <div class="info-icon">
            <el-icon :size="20"><Timer /></el-icon>
          </div>
          <div class="info-content">
            <div class="info-label">运行时长</div>
            <div class="info-value">{{ systemStatus.uptime || '-' }}</div>
          </div>
        </div>
      </div>
    </div>
    
    <!-- 智能重试状态 -->
    <div class="section" v-if="smartRetryStatus && smartRetryStatus.enabled">
      <div class="section-header">
        <h2>
          <el-icon><RefreshRight /></el-icon>
          智能重试
        </h2>
      </div>
      
      <div class="smart-retry-info">
        <div class="retry-stat">
          <span class="stat-value">{{ smartRetryStatus.pending_keys || 0 }}</span>
          <span class="stat-label">待重试Key</span>
        </div>
        <div class="retry-stat">
          <span class="stat-value success">{{ smartRetryStatus.success_keys || 0 }}</span>
          <span class="stat-label">重试成功</span>
        </div>
        <div class="retry-stat">
          <span class="stat-value warning">{{ smartRetryStatus.failed_keys || 0 }}</span>
          <span class="stat-label">重试失败</span>
        </div>
        <div class="retry-stat">
          <span class="stat-value">{{ smartRetryStatus.next_retry_in || '-' }}</span>
          <span class="stat-label">下次重试</span>
        </div>
      </div>
    </div>

    <!-- 备份管理对话框 -->
    <el-dialog v-model="backupDialogVisible" title="备份管理" width="700px" :close-on-click-modal="false">
      <div style="margin-bottom: 12px; display: flex; justify-content: flex-end;">
        <el-upload
          :show-file-list="false"
          accept=".json"
          :before-upload="handleBackupUpload"
        >
          <el-button size="small" type="success" :loading="uploading">
            <el-icon><Upload /></el-icon> 导入备份
          </el-button>
        </el-upload>
      </div>
      <div v-if="backupList.length === 0" style="text-align: center; padding: 30px 0; color: #999;">
        暂无备份记录，请先创建系统备份或导入备份文件
      </div>
      <el-table v-else :data="backupList" stripe style="width: 100%" size="small">
        <el-table-column prop="file_name" label="备份文件" min-width="240">
          <template #default="{ row }">
            <span style="font-family: monospace; font-size: 12px;">{{ row.file_name }}</span>
          </template>
        </el-table-column>
        <el-table-column prop="tasks_count" label="任务数" width="80" align="center" />
        <el-table-column prop="size" label="大小" width="90" align="center">
          <template #default="{ row }">{{ formatBytes(row.size) }}</template>
        </el-table-column>
        <el-table-column prop="created_at" label="创建时间" width="170">
          <template #default="{ row }">{{ formatTime(row.created_at) }}</template>
        </el-table-column>
        <el-table-column label="操作" width="200" align="center" fixed="right">
          <template #default="{ row }">
            <el-popconfirm
              title="恢复备份会导入任务数据（已有任务不受影响），确定？"
              @confirm="restoreBackup(row.file_name)"
            >
              <template #reference>
                <el-button size="small" type="primary" :loading="row._restoring">恢复</el-button>
              </template>
            </el-popconfirm>
            <el-button size="small" @click="downloadBackup(row.file_name)">下载</el-button>
            <el-popconfirm
              title="确定要删除此备份？"
              @confirm="deleteBackup(row.file_name)"
            >
              <template #reference>
                <el-button size="small" type="danger">删除</el-button>
              </template>
            </el-popconfirm>
          </template>
        </el-table-column>
      </el-table>
    </el-dialog>
  </div>
</template>

<script setup>
import { ref, onMounted, onUnmounted } from 'vue'
import { useRouter } from 'vue-router'
import api from '@/api'
import { ElMessage } from 'element-plus'

const router = useRouter()

const stats = ref({
  totalTasks: 0,
  runningTasks: 0,
  pausedTasks: 0,
  completedTasks: 0
})

const activeTasks = ref([])
const systemStatus = ref({})
const smartRetryStatus = ref(null)
const backingUp = ref(false)
const backupDialogVisible = ref(false)
const backupList = ref([])
const uploading = ref(false)
let refreshTimer = null

const fetchData = async () => {
  try {
    // 获取任务列表
    const tasksData = await api.getTasks({ page: 1, size: 100 })
    const tasks = tasksData.items || []
    
    stats.value.totalTasks = tasksData.total || 0
    stats.value.runningTasks = tasks.filter(t => t.status === 'running').length
    stats.value.pausedTasks = tasks.filter(t => t.status === 'paused').length
    stats.value.completedTasks = tasks.filter(t => t.status === 'completed').length
    
    activeTasks.value = tasks
      .filter(t => t.status === 'running' || t.status === 'paused')
      .sort((a, b) => {
        // running 排在 paused 前面
        if (a.status !== b.status) return a.status === 'running' ? -1 : 1
        return (b.created_at || '').localeCompare(a.created_at || '')
      })
      .slice(0, 5)
    
    // 获取系统状态
    systemStatus.value = await api.getSystemStatus()
    
    // 获取智能重试状态
    try {
      smartRetryStatus.value = await api.getSmartRetryStatus()
    } catch {
      // 忽略错误，可能功能未启用
      smartRetryStatus.value = null
    }
  } catch (err) {
    console.error('Fetch data failed:', err)
  }
}

const pauseTask = async (id) => {
  try {
    await api.pauseTask(id)
    ElMessage.success('任务已暂停')
    fetchData()
  } catch (err) {
    ElMessage.error('暂停失败')
  }
}

const resumeTask = async (id) => {
  try {
    await api.resumeTask(id)
    ElMessage.success('任务已恢复')
    fetchData()
  } catch (err) {
    ElMessage.error('恢复失败')
  }
}

const goToTasks = (status) => {
  if (status === 'all') {
    router.push('/tasks')
  } else {
    router.push({ path: '/tasks', query: { status } })
  }
}

const formatNumber = (num) => {
  // 精确显示，不使用 K/M/B 缩写，添加千分位分隔符
  if (num === null || num === undefined) return '0'
  return num.toLocaleString('zh-CN')
}

const formatBytes = (bytes) => {
  if (!bytes || bytes === 0) return '0 B'
  const units = ['B', 'KB', 'MB', 'GB', 'TB']
  let i = 0
  while (bytes >= 1024 && i < units.length - 1) {
    bytes /= 1024
    i++
  }
  return bytes.toFixed(2) + ' ' + units[i]
}

const createBackup = async () => {
  backingUp.value = true
  try {
    const result = await api.createSystemBackup()
    ElMessage.success('系统备份创建成功: ' + (result?.backup_file || ''))
  } catch (err) {
    ElMessage.error('备份失败: ' + (err.message || '未知错误'))
  } finally {
    backingUp.value = false
  }
}

const showBackupDialog = async () => {
  backupDialogVisible.value = true
  try {
    const data = await api.getBackups()
    backupList.value = (data?.backups || []).map(b => ({ ...b, _restoring: false }))
  } catch (err) {
    ElMessage.error('获取备份列表失败')
  }
}

const restoreBackup = async (filename) => {
  const item = backupList.value.find(b => b.file_name === filename)
  if (item) item._restoring = true
  try {
    const result = await api.restoreBackup(filename)
    ElMessage.success(`恢复成功：导入 ${result?.restored_tasks || 0} 个任务，跳过 ${result?.skipped_tasks || 0} 个已存在任务`)
    fetchData()
  } catch (err) {
    ElMessage.error('恢复失败: ' + (err.message || '未知错误'))
  } finally {
    if (item) item._restoring = false
  }
}

const downloadBackup = async (filename) => {
  try {
    const blob = await api.downloadBackup(filename)
    const url = URL.createObjectURL(blob)
    const a = document.createElement('a')
    a.href = url
    a.download = filename
    a.click()
    URL.revokeObjectURL(url)
  } catch (err) {
    ElMessage.error('下载失败')
  }
}

const deleteBackup = async (filename) => {
  try {
    await api.deleteBackup(filename)
    ElMessage.success('备份已删除')
    backupList.value = backupList.value.filter(b => b.file_name !== filename)
  } catch (err) {
    ElMessage.error('删除失败')
  }
}

const handleBackupUpload = async (file) => {
  uploading.value = true
  try {
    const result = await api.uploadBackup(file)
    ElMessage.success(`导入成功：${result?.file_name || ''}，包含 ${result?.tasks_count || 0} 个任务`)
    // 刷新备份列表
    const data = await api.getBackups()
    backupList.value = (data?.backups || []).map(b => ({ ...b, _restoring: false }))
  } catch (err) {
    ElMessage.error('导入失败: ' + (err.message || '未知错误'))
  } finally {
    uploading.value = false
  }
  return false // 阻止 el-upload 默认上传
}

const formatTime = (str) => {
  if (!str) return '-'
  const d = new Date(str)
  return d.toLocaleString('zh-CN')
}

onMounted(() => {
  fetchData()
  refreshTimer = setInterval(fetchData, 5000)
})

onUnmounted(() => {
  if (refreshTimer) clearInterval(refreshTimer)
})
</script>

<style lang="scss" scoped>
.dashboard {
  max-width: 1400px;
  margin: 0 auto;
}

.page-header {
  margin-bottom: 32px;
  
  h1 {
    font-size: 28px;
    font-weight: 700;
    color: var(--text-primary);
    margin-bottom: 8px;
  }
  
  p {
    color: var(--text-secondary);
  }
}

.stats-grid {
  display: grid;
  grid-template-columns: repeat(4, 1fr);
  gap: 24px;
  margin-bottom: 40px;
  
  @media (max-width: 1200px) {
    grid-template-columns: repeat(2, 1fr);
  }
  
  @media (max-width: 600px) {
    grid-template-columns: 1fr;
  }
}

.stat-card {
  background: var(--bg-card);
  border-radius: var(--radius-lg);
  padding: 24px;
  display: flex;
  align-items: center;
  gap: 20px;
  box-shadow: var(--shadow-card);
  border: 1px solid var(--border-light);
  transition: all 0.3s ease;
  
  &.clickable {
    cursor: pointer;
  }
  
  &:hover {
    transform: translateY(-4px);
    box-shadow: var(--shadow-lg);
  }
  
  .stat-icon {
    width: 56px;
    height: 56px;
    border-radius: var(--radius-md);
    display: flex;
    align-items: center;
    justify-content: center;
    color: white;
  }
  
  .stat-content {
    .stat-number {
      font-size: 32px;
      font-weight: 700;
      color: var(--text-primary);
      line-height: 1;
    }
    
    .stat-label {
      color: var(--text-secondary);
      font-size: 14px;
      margin-top: 4px;
    }
  }
}

.section {
  background: var(--bg-card);
  border-radius: var(--radius-lg);
  padding: 24px;
  margin-bottom: 24px;
  box-shadow: var(--shadow-card);
  border: 1px solid var(--border-light);
}

.section-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 20px;
  
  h2 {
    font-size: 18px;
    font-weight: 600;
    display: flex;
    align-items: center;
    gap: 8px;
    color: var(--text-primary);
  }
  
  .view-all {
    color: var(--primary-color);
    text-decoration: none;
    font-size: 14px;
    display: flex;
    align-items: center;
    gap: 4px;
    
    &:hover {
      text-decoration: underline;
    }
  }
}

.running-tasks {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(360px, 1fr));
  gap: 20px;
}

.task-card {
  background: var(--bg-primary);
  border-radius: var(--radius-md);
  padding: 20px;
  border: 1px solid var(--border-light);
  
  .task-header {
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin-bottom: 16px;
    
    .task-name {
      font-weight: 600;
      color: var(--text-primary);
    }
    
    .status-tag {
      font-size: 12px;
      padding: 2px 10px;
      border-radius: 10px;
      font-weight: 500;
      
      &.running {
        background: #ecfdf5;
        color: #059669;
      }
      
      &.paused {
        background: #fff7ed;
        color: #ea580c;
      }
    }
  }
  
  .task-progress {
    margin-bottom: 16px;
    
    .progress-info {
      display: flex;
      justify-content: space-between;
      font-size: 13px;
      color: var(--text-secondary);
      margin-bottom: 8px;
      
      .progress-percent {
        font-weight: 600;
        color: var(--primary-color);
      }
    }
  }
  
  .task-stats {
    display: grid;
    grid-template-columns: repeat(2, 1fr);
    gap: 12px;
    margin-bottom: 16px;
    
    .stat-item {
      .label {
        font-size: 12px;
        color: var(--text-tertiary);
      }
      
      .value {
        font-size: 14px;
        font-weight: 500;
        color: var(--text-primary);
      }
    }
  }
  
  .task-actions {
    display: flex;
    gap: 8px;
  }
}

.system-info {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(240px, 1fr));
  gap: 20px;
}

.info-card {
  display: flex;
  align-items: center;
  gap: 16px;
  padding: 16px;
  background: var(--bg-primary);
  border-radius: var(--radius-md);
  border: 1px solid var(--border-light);
  
  .info-icon {
    width: 44px;
    height: 44px;
    background: var(--primary-lighter);
    border-radius: var(--radius-sm);
    display: flex;
    align-items: center;
    justify-content: center;
    color: var(--primary-color);
  }
  
  .info-content {
    .info-label {
      font-size: 13px;
      color: var(--text-secondary);
    }
    
    .info-value {
      font-weight: 600;
      color: var(--text-primary);
      
      &.running {
        color: var(--success-color);
      }
      
      .idle {
        color: var(--text-tertiary);
        font-weight: normal;
      }
    }
  }
}

.system-actions {
  display: flex;
  gap: 12px;
}

// 智能重试状态样式
.smart-retry-info {
  display: grid;
  grid-template-columns: repeat(4, 1fr);
  gap: 20px;
  
  @media (max-width: 768px) {
    grid-template-columns: repeat(2, 1fr);
  }
  
  .retry-stat {
    text-align: center;
    padding: 20px;
    background: var(--bg-primary);
    border-radius: var(--radius-md);
    border: 1px solid var(--border-light);
    
    .stat-value {
      display: block;
      font-size: 28px;
      font-weight: 700;
      color: var(--text-primary);
      margin-bottom: 4px;
      
      &.success {
        color: var(--el-color-success);
      }
      
      &.warning {
        color: var(--el-color-warning);
      }
    }
    
    .stat-label {
      font-size: 13px;
      color: var(--text-secondary);
    }
  }
}
</style>
