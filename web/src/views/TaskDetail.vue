<template>
  <div class="task-detail" v-if="task">
    <!-- 返回按钮和标题 -->
    <div class="page-header">
      <div class="header-left">
        <el-button text @click="$router.push('/tasks')">
          <el-icon><ArrowLeft /></el-icon> 返回列表
        </el-button>
        <div class="title-section">
          <h1>{{ task.name }}</h1>
          <div class="task-id-row">
            <span class="task-id-label">ID:</span>
            <el-tooltip content="点击复制" placement="top">
              <span class="task-id" @click="copyTaskId">{{ task.id }}</span>
            </el-tooltip>
            <el-icon class="copy-icon" @click="copyTaskId"><CopyDocument /></el-icon>
          </div>
        </div>
        <span :class="['status-tag', task.status]">{{ getStatusText(task.status) }}</span>
      </div>
      
      <div class="header-actions">
        <template v-if="task.status === 'pending'">
          <el-button type="primary" @click="startTask">
            <el-icon><VideoPlay /></el-icon> 启动任务
          </el-button>
        </template>
        <template v-else-if="task.status === 'running'">
          <el-button @click="pauseTask">
            <el-icon><VideoPause /></el-icon> 暂停
          </el-button>
          <el-button type="primary" @click="triggerVerify">
            <el-icon><Checked /></el-icon> 校验
          </el-button>
        </template>
        <template v-else-if="task.status === 'paused'">
          <el-button type="primary" @click="resumeTask">
            <el-icon><VideoPlay /></el-icon> 恢复
          </el-button>
        </template>
      </div>
    </div>
    
    <!-- 进度概览 -->
    <div class="progress-overview card" v-if="progress">
      <div class="overview-header">
        <h2>迁移进度</h2>
        <div class="header-right">
          <el-select v-model="refreshInterval" size="small" style="width: 120px" @change="changeRefreshInterval">
            <el-option label="手动刷新" :value="0" />
            <el-option label="每 10 秒" :value="10000" />
            <el-option label="每 30 秒" :value="30000" />
            <el-option label="每 1 分钟" :value="60000" />
          </el-select>
          <el-button size="small" @click="fetchTask" :loading="refreshing">
            <el-icon><Refresh /></el-icon> 刷新
          </el-button>
          <span class="phase-tag">{{ getPhaseText(progress.phase) }}</span>
        </div>
      </div>
      
      <div class="big-progress">
        <div class="progress-circle">
          <svg viewBox="0 0 100 100">
            <circle class="bg" cx="50" cy="50" r="45" />
            <circle 
              class="progress" 
              cx="50" cy="50" r="45" 
              :style="{ strokeDasharray: `${progress.percentage * 2.83} 283` }"
            />
          </svg>
          <div class="progress-text">
            <span class="percent">{{ progress.percentage?.toFixed(1) || 0 }}</span>
            <span class="unit">%</span>
          </div>
        </div>
        
        <div class="progress-stats">
          <div class="stat-row">
            <div class="stat-item">
              <span class="label">已迁移Key</span>
              <span class="value stat-number">{{ formatNumber(progress.migrated_keys || 0) }}</span>
            </div>
            <div class="stat-item">
              <span class="label">总Key数</span>
              <span class="value">{{ formatNumber(progress.total_keys || 0) }}</span>
            </div>
            <div class="stat-item">
              <span class="label">过滤Key</span>
              <span class="value filtered">{{ formatNumber(task.keys_filtered || 0) }}</span>
            </div>
          </div>
          <div class="stat-row">
            <div class="stat-item">
              <span class="label">已迁移数据</span>
              <span class="value">{{ formatBytes(progress.migrated_bytes || 0) }}</span>
            </div>
            <div class="stat-item">
              <span class="label">总数据量</span>
              <span class="value">{{ formatBytes(progress.total_bytes || 0) }}</span>
            </div>
          </div>
          <div class="stat-row">
            <div class="stat-item">
              <span class="label">当前速度</span>
              <span class="value highlight">{{ formatNumber(progress.current_speed || 0) }} keys/s</span>
            </div>
            <div class="stat-item">
              <span class="label">预计剩余时间</span>
              <span class="value">{{ progress.estimated_eta || '-' }}</span>
            </div>
            <div class="stat-item">
              <span class="label">已耗时间</span>
              <span class="value elapsed">{{ progress.elapsed_time || '-' }}</span>
            </div>
          </div>
        </div>
      </div>
    </div>
    
    <!-- 详细信息 -->
    <div class="info-grid">
      <!-- 源集群 -->
      <div class="info-card card">
        <h3><el-icon><Connection /></el-icon> 源集群</h3>
        <div class="cluster-info">
          <div class="info-row" v-for="addr in sourceCluster?.addrs || []" :key="addr">
            <el-icon><Link /></el-icon>
            <span>{{ addr }}</span>
          </div>
        </div>
      </div>
      
      <!-- 目标集群 -->
      <div class="info-card card">
        <h3><el-icon><Connection /></el-icon> 目标集群</h3>
        <div class="cluster-info">
          <div class="info-row" v-for="addr in targetCluster?.addrs || []" :key="addr">
            <el-icon><Link /></el-icon>
            <span>{{ addr }}</span>
          </div>
        </div>
      </div>
      
      <!-- 任务配置 -->
      <div class="info-card card config-card">
        <div class="config-header">
          <h3><el-icon><Setting /></el-icon> 运行参数</h3>
          <el-button 
            v-if="task.status === 'running'" 
            type="primary" 
            size="small" 
            text
            @click="openConfigDialog"
          >
            <el-icon><Edit /></el-icon> 调整参数
          </el-button>
        </div>
        <div class="config-info">
          <div class="config-row">
            <span class="label">Worker数量</span>
            <span class="value highlight">{{ config?.worker_count || 4 }}</span>
          </div>
          <div class="config-row">
            <span class="label">扫描批次大小</span>
            <span class="value">{{ config?.scan_batch_size || 1000 }}</span>
          </div>
          <div class="config-row">
            <span class="label">源端QPS限制</span>
            <span class="value">{{ config?.rate_limit?.source_qps || '不限制' }}</span>
          </div>
          <div class="config-row">
            <span class="label">目标端QPS限制</span>
            <span class="value">{{ config?.rate_limit?.target_qps || '不限制' }}</span>
          </div>
          <div class="config-row">
            <span class="label">源端连接数</span>
            <span class="value">{{ config?.rate_limit?.source_connections || 50 }}</span>
          </div>
          <div class="config-row">
            <span class="label">目标端连接数</span>
            <span class="value">{{ config?.rate_limit?.target_connections || 50 }}</span>
          </div>
          <div class="config-row">
            <span class="label">冲突策略</span>
            <span class="value">{{ getConflictPolicyText(config?.conflict_policy) }}</span>
          </div>
          <div class="config-row">
            <span class="label">大Key阈值</span>
            <span class="value">{{ formatBytes(config?.large_key_threshold || 10485760) }}</span>
          </div>
        </div>
      </div>
      
      <!-- 时间信息 -->
      <div class="info-card card">
        <h3><el-icon><Clock /></el-icon> 时间信息</h3>
        <div class="config-info">
          <div class="config-row">
            <span class="label">创建时间</span>
            <span class="value">{{ formatTime(task.created_at) }}</span>
          </div>
          <div class="config-row" v-if="task.started_at">
            <span class="label">启动时间</span>
            <span class="value">{{ task.started_at }}</span>
          </div>
          <div class="config-row" v-if="task.full_start_at">
            <span class="label">全量开始</span>
            <span class="value">{{ task.full_start_at }}</span>
          </div>
          <div class="config-row" v-if="task.incr_start_at">
            <span class="label">增量开始</span>
            <span class="value">{{ task.incr_start_at }}</span>
          </div>
          <div class="config-row" v-if="task.completed_at">
            <span class="label">完成时间</span>
            <span class="value">{{ formatTime(task.completed_at) }}</span>
          </div>
        </div>
      </div>
    </div>
    
    <!-- 校验结果 -->
    <div class="verify-section card" v-if="verifyResults.length">
      <h3><el-icon><Checked /></el-icon> 校验结果</h3>
      <el-table :data="verifyResults" style="width: 100%">
        <el-table-column label="批次ID" width="200">
          <template #default="{ row }">
            <span class="mono">{{ row.batch_id.slice(0, 8) }}...</span>
          </template>
        </el-table-column>
        <el-table-column label="采样Key数" prop="total_keys" width="120" />
        <el-table-column label="匹配数" prop="matched_keys" width="100" />
        <el-table-column label="不一致" width="100">
          <template #default="{ row }">
            <span :class="{ 'error-text': row.mismatched_keys > 0 }">
              {{ row.mismatched_keys }}
            </span>
          </template>
        </el-table-column>
        <el-table-column label="缺失Key" width="100">
          <template #default="{ row }">
            <span :class="{ 'error-text': row.missing_keys > 0 }">
              {{ row.missing_keys }}
            </span>
          </template>
        </el-table-column>
        <el-table-column label="一致性">
          <template #default="{ row }">
            <span :class="['consistency-rate', { high: row.consistency_rate >= 99.9 }]">
              {{ row.consistency_rate?.toFixed(2) || 0 }}%
            </span>
          </template>
        </el-table-column>
      </el-table>
    </div>
    
    <!-- 异常/跳过Key统计 -->
    <div class="error-keys-section card" v-if="task.status === 'completed' || task.status === 'failed' || errorKeys.total > 0">
      <div class="section-header">
        <h3><el-icon><Warning /></el-icon> 异常/跳过Key统计</h3>
        <el-button 
          type="primary" 
          size="small" 
          @click="downloadErrorKeys"
          :disabled="errorKeys.total === 0"
        >
          <el-icon><Download /></el-icon> 下载详情
        </el-button>
      </div>
      
      <div class="error-stats">
        <div class="stat-item">
          <span class="stat-value error">{{ errorKeys.failed || 0 }}</span>
          <span class="stat-label">迁移失败</span>
        </div>
        <div class="stat-item">
          <span class="stat-value warning">{{ errorKeys.skipped || 0 }}</span>
          <span class="stat-label">冲突跳过</span>
        </div>
        <div class="stat-item">
          <span class="stat-value info">{{ errorKeys.large_keys || 0 }}</span>
          <span class="stat-label">大Key处理</span>
        </div>
        <div class="stat-item">
          <span class="stat-value">{{ errorKeys.total || 0 }}</span>
          <span class="stat-label">异常总数</span>
        </div>
      </div>
      
      <!-- 异常Key列表 -->
      <div class="error-keys-list" v-if="errorKeysList.length > 0">
        <el-table :data="errorKeysList" style="width: 100%" max-height="300">
          <el-table-column label="Key名称" min-width="200">
            <template #default="{ row }">
              <span class="mono">{{ row.key }}</span>
            </template>
          </el-table-column>
          <el-table-column label="类型" width="100" prop="type" />
          <el-table-column label="原因" width="120">
            <template #default="{ row }">
              <el-tag :type="getErrorTagType(row.reason)" size="small">
                {{ getErrorReasonText(row.reason) }}
              </el-tag>
            </template>
          </el-table-column>
          <el-table-column label="详情" min-width="200" prop="detail" />
          <el-table-column label="时间" width="100">
            <template #default="{ row }">
              {{ formatLogTime(row.timestamp) }}
            </template>
          </el-table-column>
        </el-table>
        
        <div class="list-footer" v-if="errorKeys.total > errorKeysList.length">
          <span>显示前 {{ errorKeysList.length }} 条，共 {{ errorKeys.total }} 条</span>
          <el-button text type="primary" @click="downloadErrorKeys">查看全部并下载</el-button>
        </div>
      </div>
      
      <div class="no-errors" v-else-if="errorKeys.total === 0">
        <el-icon><SuccessFilled /></el-icon>
        <span>暂无异常Key</span>
      </div>
    </div>
    
    <!-- 任务日志 -->
    <div class="task-logs card">
      <div class="logs-header">
        <h3><el-icon><Document /></el-icon> 任务日志</h3>
        <div class="logs-actions">
          <el-select v-model="logLevel" placeholder="日志级别" size="small" style="width: 100px" clearable>
            <el-option label="DEBUG" value="DEBUG" />
            <el-option label="INFO" value="INFO" />
            <el-option label="WARN" value="WARN" />
            <el-option label="ERROR" value="ERROR" />
          </el-select>
          <el-button size="small" @click="fetchTaskLogs">
            <el-icon><Refresh /></el-icon> 刷新
          </el-button>
        </div>
      </div>
      <div class="logs-container" ref="logsContainer">
        <div v-if="taskLogs.length === 0" class="no-logs">
          暂无日志
        </div>
        <div 
          v-for="log in taskLogs" 
          :key="log.id" 
          :class="['log-entry', log.level?.toLowerCase()]"
        >
          <span class="log-time">{{ formatLogTime(log.timestamp) }}</span>
          <span :class="['log-level', log.level?.toLowerCase()]">{{ log.level }}</span>
          <span class="log-message">{{ log.message }}</span>
          <span class="log-fields" v-if="log.fields">{{ formatLogFields(log.fields) }}</span>
        </div>
      </div>
    </div>
  </div>
  
  <div class="loading" v-else>
    <el-icon class="loading-icon"><Loading /></el-icon>
    <span>加载中...</span>
  </div>
  
  <!-- 参数调整对话框 -->
  <el-dialog v-model="configDialogVisible" title="调整运行参数" width="500px">
    <el-alert type="warning" :closable="false" style="margin-bottom: 20px">
      <template #title>
        <strong>优雅调整说明</strong>
      </template>
      参数调整将在当前批次完成后生效，确保正在迁移的数据完整性。调整期间任务会短暂暂停。
    </el-alert>
    
    <el-form :model="configForm" label-width="120px">
      <el-form-item label="Worker数量">
        <el-input-number 
          v-model="configForm.worker_count" 
          :min="1" 
          :max="100"
          style="width: 100%"
        />
        <div class="form-tip">增加Worker可提高速度，但会增加源端负载</div>
      </el-form-item>
      
      <el-form-item label="扫描批次大小">
        <el-input-number 
          v-model="configForm.scan_batch_size" 
          :min="100" 
          :max="10000"
          :step="100"
          style="width: 100%"
        />
      </el-form-item>
      
      <el-form-item label="源端QPS限制">
        <el-input-number 
          v-model="configForm.source_qps" 
          :min="0" 
          :max="100000"
          :step="1000"
          style="width: 100%"
        />
        <div class="form-tip">0 表示不限制</div>
      </el-form-item>
      
      <el-form-item label="目标端QPS限制">
        <el-input-number 
          v-model="configForm.target_qps" 
          :min="0" 
          :max="100000"
          :step="1000"
          style="width: 100%"
        />
        <div class="form-tip">0 表示不限制</div>
      </el-form-item>
    </el-form>
    
    <template #footer>
      <el-button @click="configDialogVisible = false">取消</el-button>
      <el-button type="primary" @click="applyConfig" :loading="applyingConfig">
        应用参数
      </el-button>
    </template>
  </el-dialog>
</template>

<script setup>
import { ref, reactive, computed, onMounted, onUnmounted } from 'vue'
import { useRoute } from 'vue-router'
import { ElMessage } from 'element-plus'
import api from '@/api'
import dayjs from 'dayjs'

const route = useRoute()
const taskId = computed(() => route.params.id)

const task = ref(null)
const progress = ref(null)
const verifyResults = ref([])
const taskLogs = ref([])
const logLevel = ref('')
const logsContainer = ref(null)
const refreshInterval = ref(10000)
const refreshing = ref(false)
const errorKeys = ref({ total: 0, failed: 0, skipped: 0, large_keys: 0 })
const errorKeysList = ref([])
let refreshTimer = null

// 参数调整相关
const configDialogVisible = ref(false)
const applyingConfig = ref(false)
const configForm = reactive({
  worker_count: 8,
  scan_batch_size: 1000,
  source_qps: 0,
  target_qps: 0
})

const sourceCluster = computed(() => {
  if (!task.value?.source_cluster) return {}
  // 如果已经是对象
  if (typeof task.value.source_cluster === 'object') {
    return task.value.source_cluster
  }
  // 尝试JSON解析
  try {
    return JSON.parse(task.value.source_cluster)
  } catch {
    // 如果是逗号分隔的字符串
    if (typeof task.value.source_cluster === 'string') {
      return { addrs: task.value.source_cluster.split(',').map(s => s.trim()).filter(s => s) }
    }
    return {}
  }
})

const targetCluster = computed(() => {
  if (!task.value?.target_cluster) return {}
  // 如果已经是对象
  if (typeof task.value.target_cluster === 'object') {
    return task.value.target_cluster
  }
  // 尝试JSON解析
  try {
    return JSON.parse(task.value.target_cluster)
  } catch {
    // 如果是逗号分隔的字符串
    if (typeof task.value.target_cluster === 'string') {
      return { addrs: task.value.target_cluster.split(',').map(s => s.trim()).filter(s => s) }
    }
    return {}
  }
})

const config = computed(() => {
  try {
    return JSON.parse(task.value?.config || '{}')
  } catch {
    return {}
  }
})

const fetchTask = async () => {
  try {
    refreshing.value = true
    const data = await api.getTask(taskId.value)
    task.value = data
    progress.value = data.progress
  } catch (err) {
    console.error('Fetch task failed:', err)
  } finally {
    refreshing.value = false
  }
}

const changeRefreshInterval = () => {
  if (refreshTimer) {
    clearInterval(refreshTimer)
    refreshTimer = null
  }
  if (refreshInterval.value > 0) {
    refreshTimer = setInterval(() => {
      if (task.value?.status === 'running') {
        fetchTask()
        fetchTaskLogs()
      }
    }, refreshInterval.value)
  }
}

const fetchVerifyResults = async () => {
  try {
    const result = await api.getVerifyResults(taskId.value)
    verifyResults.value = result || []
  } catch (err) {
    // 忽略错误，可能接口不存在
    verifyResults.value = []
  }
}

const fetchErrorKeys = async () => {
  try {
    const result = await api.getErrorKeys(taskId.value)
    errorKeys.value = result?.stats || { total: 0, failed: 0, skipped: 0, large_keys: 0 }
    errorKeysList.value = result?.items || []
  } catch (err) {
    // 忽略错误
    errorKeys.value = { total: 0, failed: 0, skipped: 0, large_keys: 0 }
    errorKeysList.value = []
  }
}

const downloadErrorKeys = async () => {
  try {
    const blob = await api.downloadErrorKeys(taskId.value)
    const url = window.URL.createObjectURL(blob)
    const a = document.createElement('a')
    a.href = url
    a.download = `error-keys-${taskId.value.substring(0, 8)}-${dayjs().format('YYYYMMDDHHmmss')}.csv`
    document.body.appendChild(a)
    a.click()
    window.URL.revokeObjectURL(url)
    document.body.removeChild(a)
    ElMessage.success('下载成功')
  } catch (err) {
    ElMessage.error('下载失败: ' + (err.message || '未知错误'))
  }
}

const getErrorTagType = (reason) => {
  const map = {
    failed: 'danger',
    skipped: 'warning',
    large_key: 'info',
    timeout: 'danger',
    conflict: 'warning'
  }
  return map[reason] || 'info'
}

const getErrorReasonText = (reason) => {
  const map = {
    failed: '迁移失败',
    skipped: '冲突跳过',
    large_key: '大Key',
    timeout: '超时',
    conflict: '键冲突'
  }
  return map[reason] || reason
}

const fetchTaskLogs = async () => {
  try {
    const params = { limit: 100 }
    if (logLevel.value) {
      params.level = logLevel.value
    }
    const result = await api.getTaskLogs(taskId.value, params)
    taskLogs.value = result?.items || []
  } catch (err) {
    console.error('Fetch task logs failed:', err)
    taskLogs.value = []
  }
}

const formatLogTime = (timestamp) => {
  return dayjs(timestamp).format('HH:mm:ss.SSS')
}

const formatLogFields = (fields) => {
  if (!fields || typeof fields !== 'object') return ''
  const entries = Object.entries(fields)
  if (entries.length === 0) return ''
  return entries.map(([k, v]) => `${k}=${JSON.stringify(v)}`).join(' ')
}

const startTask = async () => {
  try {
    await api.startTask(taskId.value)
    ElMessage.success('任务已启动')
    fetchTask()
  } catch (err) {
    ElMessage.error('启动失败')
  }
}

const pauseTask = async () => {
  try {
    await api.pauseTask(taskId.value)
    ElMessage.success('任务已暂停')
    fetchTask()
  } catch (err) {
    ElMessage.error('暂停失败')
  }
}

const resumeTask = async () => {
  try {
    await api.resumeTask(taskId.value)
    ElMessage.success('任务已恢复')
    fetchTask()
  } catch (err) {
    ElMessage.error('恢复失败')
  }
}

const triggerVerify = async () => {
  try {
    await api.triggerVerify(taskId.value)
    ElMessage.success('校验任务已触发')
    setTimeout(fetchVerifyResults, 3000)
  } catch (err) {
    ElMessage.error('触发校验失败')
  }
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

const copyTaskId = async () => {
  try {
    await copyToClipboard(task.value.id)
    ElMessage.success('任务ID已复制')
  } catch (err) {
    ElMessage.error('复制失败')
  }
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

const getConflictPolicyText = (policy) => {
  const map = {
    skip: '跳过已存在',
    replace: '覆盖',
    error: '报错',
    skip_full_only: '仅全量跳过'
  }
  return map[policy] || policy || 'skip'
}

// 打开参数调整对话框
const openConfigDialog = () => {
  const cfg = config.value || {}
  configForm.worker_count = cfg.worker_count || 8
  configForm.scan_batch_size = cfg.scan_batch_size || 1000
  configForm.source_qps = cfg.rate_limit?.source_qps || 0
  configForm.target_qps = cfg.rate_limit?.target_qps || 0
  configDialogVisible.value = true
}

// 应用参数调整
const applyConfig = async () => {
  applyingConfig.value = true
  try {
    await api.updateTaskConfig(taskId.value, {
      worker_count: configForm.worker_count,
      scan_batch_size: configForm.scan_batch_size,
      rate_limit: {
        source_qps: configForm.source_qps,
        target_qps: configForm.target_qps
      }
    })
    ElMessage.success('参数调整已提交，将在当前批次完成后生效')
    configDialogVisible.value = false
    // 延迟刷新任务信息
    setTimeout(fetchTask, 2000)
  } catch (err) {
    ElMessage.error('参数调整失败: ' + (err.message || '未知错误'))
  } finally {
    applyingConfig.value = false
  }
}

const getPhaseText = (phase) => {
  const map = {
    full: '全量迁移',
    incremental: '增量同步',
    verify: '数据校验'
  }
  return map[phase] || phase
}

const formatNumber = (num) => {
  if (num >= 1000000000) return (num / 1000000000).toFixed(1) + 'B'
  if (num >= 1000000) return (num / 1000000).toFixed(1) + 'M'
  if (num >= 1000) return (num / 1000).toFixed(1) + 'K'
  return num.toString()
}

const formatBytes = (bytes) => {
  if (bytes >= 1099511627776) return (bytes / 1099511627776).toFixed(2) + ' TB'
  if (bytes >= 1073741824) return (bytes / 1073741824).toFixed(2) + ' GB'
  if (bytes >= 1048576) return (bytes / 1048576).toFixed(2) + ' MB'
  if (bytes >= 1024) return (bytes / 1024).toFixed(2) + ' KB'
  return bytes + ' B'
}

const formatTime = (time) => {
  return dayjs(time).format('YYYY-MM-DD HH:mm:ss')
}

const getRunningTime = () => {
  if (!task.value?.started_at) return '-'
  const start = dayjs(task.value.started_at)
  const end = task.value.completed_at ? dayjs(task.value.completed_at) : dayjs()
  const diff = end.diff(start, 'second')
  
  const hours = Math.floor(diff / 3600)
  const minutes = Math.floor((diff % 3600) / 60)
  const seconds = diff % 60
  
  if (hours > 0) return `${hours}h ${minutes}m ${seconds}s`
  if (minutes > 0) return `${minutes}m ${seconds}s`
  return `${seconds}s`
}

onMounted(() => {
  fetchTask()
  fetchVerifyResults()
  fetchTaskLogs()
  fetchErrorKeys()
  // 默认10秒刷新
  if (refreshInterval.value > 0) {
    refreshTimer = setInterval(() => {
      if (task.value?.status === 'running') {
        fetchTask()
        fetchTaskLogs()
        fetchErrorKeys()
      }
    }, refreshInterval.value)
  }
})

onUnmounted(() => {
  if (refreshTimer) clearInterval(refreshTimer)
})
</script>

<style lang="scss" scoped>
.task-detail {
  max-width: 1400px;
  margin: 0 auto;
}

.page-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 24px;
  
  .header-left {
    display: flex;
    align-items: center;
    gap: 16px;
    
    .title-section {
      h1 {
        font-size: 24px;
        font-weight: 700;
        color: var(--text-primary);
        margin-bottom: 4px;
      }
      
      .task-id-row {
        display: flex;
        align-items: center;
        gap: 6px;
        
        .task-id-label {
          font-size: 12px;
          color: var(--text-tertiary);
        }
        
        .task-id {
          font-family: 'Consolas', 'Monaco', monospace;
          font-size: 12px;
          color: var(--text-secondary);
          background: var(--bg-primary);
          padding: 2px 8px;
          border-radius: 4px;
          cursor: pointer;
          transition: all 0.2s;
          
          &:hover {
            background: var(--primary-lighter);
            color: var(--primary-color);
          }
        }
        
        .copy-icon {
          font-size: 14px;
          color: var(--text-tertiary);
          cursor: pointer;
          transition: color 0.2s;
          
          &:hover {
            color: var(--primary-color);
          }
        }
      }
    }
  }
  
  .header-actions {
    display: flex;
    gap: 12px;
  }
}

.card {
  background: var(--bg-card);
  border-radius: var(--radius-lg);
  padding: 24px;
  box-shadow: var(--shadow-card);
  border: 1px solid var(--border-light);
  margin-bottom: 24px;
}

.progress-overview {
  .overview-header {
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin-bottom: 24px;
    
    h2 {
      font-size: 18px;
      font-weight: 600;
    }
    
    .header-right {
      display: flex;
      align-items: center;
      gap: 12px;
    }
    
    .phase-tag {
      padding: 6px 16px;
      background: var(--primary-lighter);
      color: var(--primary-color);
      border-radius: 20px;
      font-size: 13px;
      font-weight: 500;
    }
  }
  
  .big-progress {
    display: flex;
    gap: 48px;
    align-items: center;
    
    @media (max-width: 768px) {
      flex-direction: column;
    }
  }
  
  .progress-circle {
    width: 180px;
    height: 180px;
    position: relative;
    flex-shrink: 0;
    
    svg {
      width: 100%;
      height: 100%;
      transform: rotate(-90deg);
      
      circle {
        fill: none;
        stroke-width: 8;
        
        &.bg {
          stroke: var(--border-light);
        }
        
        &.progress {
          stroke: url(#gradient);
          stroke-linecap: round;
          transition: stroke-dasharray 0.5s ease;
        }
      }
      
      defs {
        linearGradient {
          stop:first-child {
            stop-color: #2563eb;
          }
          stop:last-child {
            stop-color: #06b6d4;
          }
        }
      }
    }
    
    .progress-text {
      position: absolute;
      top: 50%;
      left: 50%;
      transform: translate(-50%, -50%);
      text-align: center;
      
      .percent {
        font-size: 42px;
        font-weight: 700;
        background: var(--gradient-blue);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        background-clip: text;
      }
      
      .unit {
        font-size: 16px;
        color: var(--text-secondary);
      }
    }
  }
  
  .progress-stats {
    flex: 1;
    
    .stat-row {
      display: grid;
      grid-template-columns: repeat(3, 1fr);
      gap: 24px;
      margin-bottom: 20px;
      
      &:last-child {
        margin-bottom: 0;
      }
    }
    
    .stat-item {
      .label {
        display: block;
        font-size: 13px;
        color: var(--text-secondary);
        margin-bottom: 4px;
      }
      
      .value {
        font-size: 20px;
        font-weight: 600;
        color: var(--text-primary);
        
        &.highlight {
          color: var(--primary-color);
        }
        
        &.filtered {
          color: var(--el-color-info);
        }
      }
    }
  }
}

.info-grid {
  display: grid;
  grid-template-columns: repeat(2, 1fr);
  gap: 24px;
  
  @media (max-width: 900px) {
    grid-template-columns: 1fr;
  }
}

.info-card {
  h3 {
    display: flex;
    align-items: center;
    gap: 8px;
    font-size: 16px;
    font-weight: 600;
    margin-bottom: 16px;
    color: var(--text-primary);
  }
  
  .cluster-info {
    .info-row {
      display: flex;
      align-items: center;
      gap: 8px;
      padding: 8px 12px;
      background: var(--bg-primary);
      border-radius: var(--radius-sm);
      margin-bottom: 8px;
      font-family: monospace;
      font-size: 13px;
      
      &:last-child {
        margin-bottom: 0;
      }
    }
  }
  
  .config-info {
    .config-row {
      display: flex;
      justify-content: space-between;
      padding: 10px 0;
      border-bottom: 1px solid var(--border-light);
      
      &:last-child {
        border-bottom: none;
      }
      
      .label {
        color: var(--text-secondary);
      }
      
      .value {
        font-weight: 500;
        color: var(--text-primary);
      }
    }
  }
}

.verify-section {
  h3 {
    display: flex;
    align-items: center;
    gap: 8px;
    font-size: 16px;
    font-weight: 600;
    margin-bottom: 16px;
  }
  
  .mono {
    font-family: monospace;
  }
  
  .error-text {
    color: var(--error-color);
    font-weight: 500;
  }
  
  .consistency-rate {
    font-weight: 600;
    
    &.high {
      color: var(--success-color);
    }
  }
}

.error-keys-section {
  .section-header {
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin-bottom: 20px;
    
    h3 {
      display: flex;
      align-items: center;
      gap: 8px;
      font-size: 16px;
      font-weight: 600;
      margin: 0;
    }
  }
  
  .error-stats {
    display: grid;
    grid-template-columns: repeat(4, 1fr);
    gap: 16px;
    margin-bottom: 20px;
    
    .stat-item {
      text-align: center;
      padding: 16px;
      background: var(--bg-primary);
      border-radius: var(--radius-md);
      
      .stat-value {
        display: block;
        font-size: 28px;
        font-weight: 700;
        margin-bottom: 4px;
        
        &.error { color: var(--el-color-danger); }
        &.warning { color: var(--el-color-warning); }
        &.info { color: var(--el-color-info); }
      }
      
      .stat-label {
        font-size: 13px;
        color: var(--text-secondary);
      }
    }
  }
  
  .error-keys-list {
    .mono {
      font-family: 'Consolas', 'Monaco', monospace;
      font-size: 12px;
    }
    
    .list-footer {
      display: flex;
      justify-content: space-between;
      align-items: center;
      padding: 12px 0;
      font-size: 13px;
      color: var(--text-secondary);
    }
  }
  
  .no-errors {
    display: flex;
    align-items: center;
    justify-content: center;
    gap: 8px;
    padding: 40px;
    color: var(--el-color-success);
    font-size: 14px;
    
    .el-icon {
      font-size: 20px;
    }
  }
}

.loading {
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  height: 400px;
  color: var(--text-secondary);
  
  .loading-icon {
    font-size: 32px;
    animation: spin 1s linear infinite;
    margin-bottom: 12px;
  }
}

@keyframes spin {
  from { transform: rotate(0deg); }
  to { transform: rotate(360deg); }
}

.task-logs {
  .logs-header {
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin-bottom: 16px;
    
    h3 {
      display: flex;
      align-items: center;
      gap: 8px;
      font-size: 16px;
      font-weight: 600;
      margin: 0;
    }
    
    .logs-actions {
      display: flex;
      gap: 8px;
    }
  }
  
  .logs-container {
    background: #1e1e1e;
    border-radius: var(--radius-md);
    padding: 16px;
    max-height: 400px;
    overflow-y: auto;
    font-family: 'Consolas', 'Monaco', monospace;
    font-size: 12px;
    line-height: 1.6;
  }
  
  .no-logs {
    color: #888;
    text-align: center;
    padding: 40px;
  }
  
  .log-entry {
    display: flex;
    gap: 12px;
    padding: 4px 0;
    color: #d4d4d4;
    
    &.debug { color: #888; }
    &.info { color: #4fc3f7; }
    &.warn { color: #ffb74d; }
    &.error { color: #ef5350; }
    &.fatal { color: #ff1744; }
    
    .log-time {
      color: #888;
      flex-shrink: 0;
    }
    
    .log-level {
      width: 50px;
      flex-shrink: 0;
      font-weight: 600;
      
      &.debug { color: #888; }
      &.info { color: #4fc3f7; }
      &.warn { color: #ffb74d; }
      &.error { color: #ef5350; }
      &.fatal { color: #ff1744; }
    }
    
    .log-message {
      flex: 1;
      word-break: break-all;
    }
    
    .log-fields {
      color: #888;
      font-size: 11px;
    }
  }
}
</style>
