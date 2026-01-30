<template>
  <div class="logs-view">
    <!-- 页面标题 -->
    <div class="page-header">
      <div class="header-content">
        <h1 class="page-title">
          <el-icon><Document /></el-icon>
          系统日志
        </h1>
        <p class="page-desc">查看、搜索和导出系统运行日志，便于故障排查</p>
      </div>
      <div class="header-actions">
        <el-button @click="refreshLogs" :loading="loading">
          <el-icon><Refresh /></el-icon>刷新
        </el-button>
        <el-popconfirm
          title="确定要清除所有日志吗？此操作不可恢复！"
          confirm-button-text="确定清除"
          cancel-button-text="取消"
          @confirm="clearAllLogs"
        >
          <template #reference>
            <el-button type="danger" plain>
              <el-icon><Delete /></el-icon>清除全部
            </el-button>
          </template>
        </el-popconfirm>
        <el-dropdown @command="handleExport">
          <el-button type="primary">
            <el-icon><Download /></el-icon>导出日志
            <el-icon class="el-icon--right"><ArrowDown /></el-icon>
          </el-button>
          <template #dropdown>
            <el-dropdown-menu>
              <el-dropdown-item command="text">导出全部日志 (.txt)</el-dropdown-item>
              <el-dropdown-item command="json">导出全部日志 (.json)</el-dropdown-item>
              <el-dropdown-item divided v-if="filter.task_id">
                <span style="font-size: 12px; color: #409EFF; font-weight: 500;">
                  当前筛选任务: {{ getTaskName(filter.task_id) }}
                </span>
              </el-dropdown-item>
              <el-dropdown-item command="task-text" v-if="filter.task_id">
                <el-icon><Document /></el-icon> 导出当前任务日志 (.txt)
              </el-dropdown-item>
              <el-dropdown-item command="task-json" v-if="filter.task_id">
                <el-icon><Document /></el-icon> 导出当前任务日志 (.json)
              </el-dropdown-item>
              <el-dropdown-item divided disabled v-if="!filter.task_id">
                <span style="font-size: 12px; color: #999;">提示: 选择任务后可单独导出</span>
              </el-dropdown-item>
            </el-dropdown-menu>
          </template>
        </el-dropdown>
      </div>
    </div>

    <!-- 统计卡片 -->
    <div class="stats-row">
      <div class="stat-card">
        <div class="stat-icon total"><el-icon><Document /></el-icon></div>
        <div class="stat-info">
          <span class="stat-value">{{ stats.total }}</span>
          <span class="stat-label">总日志数</span>
        </div>
      </div>
      <div class="stat-card">
        <div class="stat-icon error"><el-icon><CircleClose /></el-icon></div>
        <div class="stat-info">
          <span class="stat-value">{{ stats.by_level?.ERROR || 0 }}</span>
          <span class="stat-label">错误</span>
        </div>
      </div>
      <div class="stat-card">
        <div class="stat-icon warn"><el-icon><Warning /></el-icon></div>
        <div class="stat-info">
          <span class="stat-value">{{ stats.by_level?.WARN || 0 }}</span>
          <span class="stat-label">警告</span>
        </div>
      </div>
      <div class="stat-card">
        <div class="stat-icon info"><el-icon><InfoFilled /></el-icon></div>
        <div class="stat-info">
          <span class="stat-value">{{ stats.by_level?.INFO || 0 }}</span>
          <span class="stat-label">信息</span>
        </div>
      </div>
      <div class="stat-card">
        <div class="stat-icon uptime"><el-icon><Timer /></el-icon></div>
        <div class="stat-info">
          <span class="stat-value uptime-text">{{ stats.uptime || '-' }}</span>
          <span class="stat-label">运行时间</span>
        </div>
      </div>
    </div>

    <!-- 过滤器 -->
    <div class="filter-section">
      <div class="filter-row">
        <el-input
          v-model="filter.keyword"
          placeholder="搜索日志内容..."
          clearable
          class="search-input"
          @keyup.enter="searchLogs"
        >
          <template #prefix>
            <el-icon><Search /></el-icon>
          </template>
        </el-input>
        
        <el-select v-model="filter.level" placeholder="日志级别" clearable class="filter-select">
          <el-option label="全部级别" value="" />
          <el-option label="DEBUG" value="DEBUG" />
          <el-option label="INFO" value="INFO" />
          <el-option label="WARN" value="WARN" />
          <el-option label="ERROR" value="ERROR" />
          <el-option label="FATAL" value="FATAL" />
        </el-select>

        <el-select 
          v-model="filter.task_id" 
          placeholder="选择任务" 
          clearable 
          filterable
          class="filter-select-task"
        >
          <el-option label="全部任务" value="" />
          <el-option 
            v-for="task in taskList" 
            :key="task.id" 
            :label="`${task.name} (${task.id.substring(0, 8)})`"
            :value="task.id"
          />
        </el-select>

        <el-button type="primary" @click="searchLogs">
          <el-icon><Search /></el-icon>搜索
        </el-button>
        <el-button @click="resetFilter">重置</el-button>
      </div>
    </div>

    <!-- 日志列表 -->
    <div class="logs-container">
      <div class="logs-header">
        <span class="logs-count">共 {{ total }} 条日志</span>
        <div class="logs-actions">
          <el-checkbox v-model="autoRefresh" @change="toggleAutoRefresh">
            自动刷新 (5s)
          </el-checkbox>
          <el-button text @click="copyAllLogs" :disabled="logs.length === 0">
            <el-icon><CopyDocument /></el-icon>复制全部
          </el-button>
        </div>
      </div>

      <div class="logs-list" v-loading="loading">
        <div v-if="logs.length === 0 && !loading" class="empty-state">
          <el-icon :size="48"><Document /></el-icon>
          <p>暂无日志记录</p>
        </div>
        
        <div
          v-for="log in logs"
          :key="log.id"
          :class="['log-item', `level-${log.level.toLowerCase()}`]"
          @click="showLogDetail(log)"
        >
          <div class="log-header">
            <span :class="['log-level', log.level.toLowerCase()]">{{ log.level }}</span>
            <span class="log-time">{{ log.timestamp }}</span>
            <span v-if="log.source" class="log-source">{{ log.source }}</span>
            <span v-if="log.request_id" class="log-tag request">
              <el-icon><Connection /></el-icon>
              {{ log.request_id.substring(0, 8) }}
            </span>
            <span v-if="log.task_id" class="log-tag task">
              <el-icon><Files /></el-icon>
              {{ log.task_id.substring(0, 8) }}
            </span>
          </div>
          <div class="log-message">{{ log.message }}</div>
          <div v-if="log.fields && Object.keys(log.fields).length > 0" class="log-fields">
            <span v-for="(value, key) in log.fields" :key="key" class="field-item">
              <span class="field-key">{{ key }}:</span>
              <span class="field-value">{{ formatValue(value) }}</span>
            </span>
          </div>
          <div class="log-actions">
            <el-button text size="small" @click.stop="copyLog(log)">
              <el-icon><CopyDocument /></el-icon>复制
            </el-button>
          </div>
        </div>
      </div>

      <!-- 分页 -->
      <div class="pagination-wrapper" v-if="total > pageSize">
        <el-pagination
          v-model:current-page="currentPage"
          v-model:page-size="pageSize"
          :page-sizes="[50, 100, 200, 500]"
          :total="total"
          layout="total, sizes, prev, pager, next, jumper"
          @size-change="handleSizeChange"
          @current-change="handlePageChange"
        />
      </div>
    </div>

    <!-- 日志详情弹窗 -->
    <el-dialog
      v-model="detailVisible"
      title="日志详情"
      width="800px"
      class="log-detail-dialog"
    >
      <div v-if="selectedLog" class="log-detail">
        <div class="detail-section">
          <h4>基本信息</h4>
          <div class="detail-grid">
            <div class="detail-item">
              <span class="label">ID</span>
              <span class="value">{{ selectedLog.id }}</span>
            </div>
            <div class="detail-item">
              <span class="label">级别</span>
              <span :class="['value', 'level-badge', selectedLog.level.toLowerCase()]">
                {{ selectedLog.level }}
              </span>
            </div>
            <div class="detail-item">
              <span class="label">时间</span>
              <span class="value">{{ selectedLog.timestamp }}</span>
            </div>
            <div class="detail-item">
              <span class="label">来源</span>
              <span class="value">{{ selectedLog.source || '-' }}</span>
            </div>
            <div class="detail-item" v-if="selectedLog.request_id">
              <span class="label">请求ID</span>
              <span class="value mono">{{ selectedLog.request_id }}</span>
            </div>
            <div class="detail-item" v-if="selectedLog.task_id">
              <span class="label">任务ID</span>
              <span class="value mono">{{ selectedLog.task_id }}</span>
            </div>
          </div>
        </div>

        <div class="detail-section">
          <h4>日志消息</h4>
          <div class="message-box">{{ selectedLog.message }}</div>
        </div>

        <div class="detail-section" v-if="selectedLog.fields && Object.keys(selectedLog.fields).length > 0">
          <h4>附加字段</h4>
          <pre class="fields-json">{{ JSON.stringify(selectedLog.fields, null, 2) }}</pre>
        </div>

        <div class="detail-section" v-if="selectedLog.stack">
          <h4>堆栈跟踪</h4>
          <pre class="stack-trace">{{ selectedLog.stack }}</pre>
        </div>
      </div>

      <template #footer>
        <el-button @click="copyLog(selectedLog)">
          <el-icon><CopyDocument /></el-icon>复制日志
        </el-button>
        <el-button type="primary" @click="detailVisible = false">关闭</el-button>
      </template>
    </el-dialog>
  </div>
</template>

<script setup>
import { ref, onMounted, onUnmounted } from 'vue'
import { ElMessage } from 'element-plus'
import api from '@/api'

const loading = ref(false)
const logs = ref([])
const total = ref(0)
const currentPage = ref(1)
const pageSize = ref(100)
const autoRefresh = ref(false)
const detailVisible = ref(false)
const selectedLog = ref(null)
const taskList = ref([])
let refreshTimer = null

const stats = ref({
  total: 0,
  by_level: {},
  uptime: '-',
  memory_mb: 0
})

const filter = ref({
  level: '',
  keyword: '',
  task_id: '',
  request_id: ''
})

const fetchLogs = async () => {
  loading.value = true
  try {
    const params = {
      limit: pageSize.value,
      offset: (currentPage.value - 1) * pageSize.value,
      level: filter.value.level,
      keyword: filter.value.keyword,
      task_id: filter.value.task_id,
      request_id: filter.value.request_id
    }
    const data = await api.getLogs(params)
    logs.value = data.items || []
    total.value = data.total || 0
  } catch (error) {
    console.error('Failed to fetch logs:', error)
  } finally {
    loading.value = false
  }
}

const fetchStats = async () => {
  try {
    const data = await api.getLogsStats()
    stats.value = data
  } catch (error) {
    console.error('Failed to fetch stats:', error)
  }
}

const fetchTasks = async () => {
  try {
    const data = await api.getTasks()
    taskList.value = data.items || []
  } catch (error) {
    console.error('Failed to fetch tasks:', error)
  }
}

const getTaskName = (taskId) => {
  const task = taskList.value.find(t => t.id === taskId)
  return task ? `${task.name} (${taskId.substring(0, 8)})` : taskId.substring(0, 8)
}

const refreshLogs = () => {
  fetchLogs()
  fetchStats()
}

const searchLogs = () => {
  currentPage.value = 1
  fetchLogs()
}

const resetFilter = () => {
  filter.value = {
    level: '',
    keyword: '',
    task_id: '',
    request_id: ''
  }
  currentPage.value = 1
  fetchLogs()
}

const handlePageChange = (page) => {
  currentPage.value = page
  fetchLogs()
}

const handleSizeChange = (size) => {
  pageSize.value = size
  currentPage.value = 1
  fetchLogs()
}

const toggleAutoRefresh = (enabled) => {
  if (enabled) {
    refreshTimer = setInterval(refreshLogs, 5000)
  } else {
    clearInterval(refreshTimer)
    refreshTimer = null
  }
}

const handleExport = async (command) => {
  try {
    let format = command
    let taskId = ''
    
    // 处理任务导出
    if (command.startsWith('task-')) {
      format = command.replace('task-', '')
      taskId = filter.value.task_id
    }
    
    const params = new URLSearchParams()
    params.set('format', format)
    if (filter.value.level) params.set('level', filter.value.level)
    if (filter.value.keyword) params.set('keyword', filter.value.keyword)
    if (taskId) params.set('task_id', taskId)
    
    const url = `/api/v1/logs/export?${params.toString()}`
    const link = document.createElement('a')
    link.href = url
    document.body.appendChild(link)
    link.click()
    document.body.removeChild(link)
    
    ElMessage.success(taskId ? '任务日志导出成功' : '日志导出成功')
  } catch (error) {
    ElMessage.error('导出失败: ' + error.message)
  }
}

const clearAllLogs = async () => {
  try {
    await api.clearLogs()
    ElMessage.success('日志已清除')
    refreshLogs()
  } catch (error) {
    ElMessage.error('清除失败: ' + error.message)
  }
}

const showLogDetail = (log) => {
  selectedLog.value = log
  detailVisible.value = true
}

const formatValue = (value) => {
  if (typeof value === 'object') {
    return JSON.stringify(value)
  }
  return String(value)
}

const copyLog = (log) => {
  const text = formatLogForCopy(log)
  navigator.clipboard.writeText(text).then(() => {
    ElMessage.success('已复制到剪贴板')
  }).catch(() => {
    // Fallback
    const textarea = document.createElement('textarea')
    textarea.value = text
    document.body.appendChild(textarea)
    textarea.select()
    document.execCommand('copy')
    document.body.removeChild(textarea)
    ElMessage.success('已复制到剪贴板')
  })
}

const copyAllLogs = () => {
  const text = logs.value.map(formatLogForCopy).join('\n\n')
  navigator.clipboard.writeText(text).then(() => {
    ElMessage.success(`已复制 ${logs.value.length} 条日志`)
  })
}

const formatLogForCopy = (log) => {
  let text = `[${log.timestamp}] [${log.level}]`
  if (log.source) text += ` [${log.source}]`
  if (log.request_id) text += ` [req:${log.request_id.substring(0, 8)}]`
  if (log.task_id) text += ` [task:${log.task_id.substring(0, 8)}]`
  text += ` ${log.message}`
  if (log.fields && Object.keys(log.fields).length > 0) {
    text += `\n  Fields: ${JSON.stringify(log.fields)}`
  }
  if (log.stack) {
    text += `\n  Stack:\n${log.stack}`
  }
  return text
}

onMounted(() => {
  fetchTasks()
  refreshLogs()
})

onUnmounted(() => {
  if (refreshTimer) {
    clearInterval(refreshTimer)
  }
})
</script>

<style lang="scss" scoped>
.logs-view {
  max-width: 1400px;
  margin: 0 auto;
}

.page-header {
  display: flex;
  justify-content: space-between;
  align-items: flex-start;
  margin-bottom: 24px;
  
  .header-content {
    .page-title {
      display: flex;
      align-items: center;
      gap: 12px;
      font-size: 28px;
      font-weight: 700;
      color: var(--text-primary);
      margin: 0 0 8px;
      
      .el-icon {
        color: var(--primary-color);
      }
    }
    
    .page-desc {
      color: var(--text-secondary);
      margin: 0;
    }
  }
  
  .header-actions {
    display: flex;
    gap: 12px;
  }
}

.stats-row {
  display: grid;
  grid-template-columns: repeat(5, 1fr);
  gap: 16px;
  margin-bottom: 24px;
  
  .stat-card {
    background: var(--bg-secondary);
    border-radius: var(--radius-lg);
    padding: 20px;
    display: flex;
    align-items: center;
    gap: 16px;
    
    .stat-icon {
      width: 48px;
      height: 48px;
      border-radius: var(--radius-md);
      display: flex;
      align-items: center;
      justify-content: center;
      font-size: 24px;
      
      &.total {
        background: rgba(64, 158, 255, 0.1);
        color: #409EFF;
      }
      &.error {
        background: rgba(245, 108, 108, 0.1);
        color: #F56C6C;
      }
      &.warn {
        background: rgba(230, 162, 60, 0.1);
        color: #E6A23C;
      }
      &.info {
        background: rgba(103, 194, 58, 0.1);
        color: #67C23A;
      }
      &.uptime {
        background: rgba(144, 147, 153, 0.1);
        color: #909399;
      }
    }
    
    .stat-info {
      display: flex;
      flex-direction: column;
      
      .stat-value {
        font-size: 24px;
        font-weight: 700;
        color: var(--text-primary);
        
        &.uptime-text {
          font-size: 14px;
        }
      }
      
      .stat-label {
        font-size: 13px;
        color: var(--text-tertiary);
      }
    }
  }
}

.filter-section {
  background: var(--bg-secondary);
  border-radius: var(--radius-lg);
  padding: 20px;
  margin-bottom: 24px;
  
  .filter-row {
    display: flex;
    gap: 12px;
    align-items: center;
    flex-wrap: wrap;
    
    .search-input {
      width: 300px;
    }
    
    .filter-select {
      width: 140px;
    }
    
    .filter-select-task {
      width: 280px;
    }
    
    .filter-input {
      width: 180px;
    }
  }
}

.logs-container {
  background: var(--bg-secondary);
  border-radius: var(--radius-lg);
  overflow: hidden;
  
  .logs-header {
    padding: 16px 20px;
    border-bottom: 1px solid var(--border-color);
    display: flex;
    justify-content: space-between;
    align-items: center;
    
    .logs-count {
      font-weight: 500;
      color: var(--text-secondary);
    }
    
    .logs-actions {
      display: flex;
      align-items: center;
      gap: 16px;
    }
  }
  
  .logs-list {
    max-height: 600px;
    overflow-y: auto;
    min-height: 200px;
    
    .empty-state {
      padding: 60px 20px;
      text-align: center;
      color: var(--text-tertiary);
      
      .el-icon {
        margin-bottom: 12px;
      }
    }
  }
  
  .log-item {
    padding: 12px 20px;
    border-bottom: 1px solid var(--border-light);
    cursor: pointer;
    transition: background 0.2s;
    position: relative;
    
    &:hover {
      background: var(--bg-hover);
      
      .log-actions {
        opacity: 1;
      }
    }
    
    &.level-error {
      border-left: 3px solid #F56C6C;
      background: rgba(245, 108, 108, 0.02);
    }
    
    &.level-warn {
      border-left: 3px solid #E6A23C;
      background: rgba(230, 162, 60, 0.02);
    }
    
    &.level-fatal {
      border-left: 3px solid #ff4d4f;
      background: rgba(255, 77, 79, 0.05);
    }
    
    .log-header {
      display: flex;
      align-items: center;
      gap: 12px;
      margin-bottom: 6px;
      flex-wrap: wrap;
      
      .log-level {
        font-size: 11px;
        font-weight: 600;
        padding: 2px 8px;
        border-radius: 4px;
        text-transform: uppercase;
        
        &.debug { background: #909399; color: white; }
        &.info { background: #67C23A; color: white; }
        &.warn { background: #E6A23C; color: white; }
        &.error { background: #F56C6C; color: white; }
        &.fatal { background: #ff4d4f; color: white; }
      }
      
      .log-time {
        font-size: 12px;
        color: var(--text-tertiary);
        font-family: 'Monaco', 'Menlo', monospace;
      }
      
      .log-source {
        font-size: 12px;
        color: var(--text-tertiary);
        background: var(--bg-primary);
        padding: 2px 8px;
        border-radius: 4px;
      }
      
      .log-tag {
        font-size: 11px;
        display: flex;
        align-items: center;
        gap: 4px;
        padding: 2px 8px;
        border-radius: 4px;
        
        &.request {
          background: rgba(64, 158, 255, 0.1);
          color: #409EFF;
        }
        
        &.task {
          background: rgba(103, 194, 58, 0.1);
          color: #67C23A;
        }
      }
    }
    
    .log-message {
      font-size: 14px;
      color: var(--text-primary);
      line-height: 1.5;
      word-break: break-word;
    }
    
    .log-fields {
      margin-top: 8px;
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
      
      .field-item {
        font-size: 12px;
        background: var(--bg-primary);
        padding: 4px 8px;
        border-radius: 4px;
        
        .field-key {
          color: var(--text-tertiary);
        }
        
        .field-value {
          color: var(--primary-color);
          margin-left: 4px;
          font-family: 'Monaco', 'Menlo', monospace;
        }
      }
    }
    
    .log-actions {
      position: absolute;
      right: 20px;
      top: 12px;
      opacity: 0;
      transition: opacity 0.2s;
    }
  }
  
  .pagination-wrapper {
    padding: 16px 20px;
    border-top: 1px solid var(--border-color);
    display: flex;
    justify-content: center;
  }
}

.log-detail-dialog {
  .log-detail {
    .detail-section {
      margin-bottom: 24px;
      
      h4 {
        font-size: 14px;
        color: var(--text-secondary);
        margin: 0 0 12px;
        font-weight: 600;
      }
      
      .detail-grid {
        display: grid;
        grid-template-columns: repeat(2, 1fr);
        gap: 12px;
        
        .detail-item {
          display: flex;
          flex-direction: column;
          gap: 4px;
          
          .label {
            font-size: 12px;
            color: var(--text-tertiary);
          }
          
          .value {
            font-size: 14px;
            color: var(--text-primary);
            
            &.mono {
              font-family: 'Monaco', 'Menlo', monospace;
              font-size: 12px;
            }
            
            &.level-badge {
              display: inline-block;
              padding: 2px 8px;
              border-radius: 4px;
              font-size: 12px;
              font-weight: 600;
              width: fit-content;
              
              &.debug { background: #909399; color: white; }
              &.info { background: #67C23A; color: white; }
              &.warn { background: #E6A23C; color: white; }
              &.error { background: #F56C6C; color: white; }
              &.fatal { background: #ff4d4f; color: white; }
            }
          }
        }
      }
      
      .message-box {
        background: var(--bg-primary);
        padding: 16px;
        border-radius: var(--radius-md);
        font-size: 14px;
        line-height: 1.6;
        word-break: break-word;
      }
      
      .fields-json, .stack-trace {
        background: #1e1e1e;
        color: #d4d4d4;
        padding: 16px;
        border-radius: var(--radius-md);
        font-family: 'Monaco', 'Menlo', monospace;
        font-size: 12px;
        line-height: 1.5;
        overflow-x: auto;
        white-space: pre-wrap;
        word-break: break-word;
        max-height: 300px;
        overflow-y: auto;
      }
    }
  }
}
</style>
