<template>
  <div class="tasks-page">
    <div class="page-header">
      <div>
        <h1>迁移任务</h1>
        <p>管理所有数据迁移任务</p>
      </div>
      <div class="header-actions">
        <el-button @click="showImportDialog = true">
          <el-icon><Upload /></el-icon> 导入配置
        </el-button>
        <el-button type="primary" @click="$router.push('/create')">
          <el-icon><Plus /></el-icon> 创建任务
        </el-button>
      </div>
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
        <el-option label="已停止" value="stopped" />
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
              <span class="id" @click.stop="copyTaskId(row.id)">{{ row.id }}</span>
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
        
        <el-table-column label="操作" width="260" fixed="right">
          <template #default="{ row }">
            <div class="actions" @click.stop>
              <template v-if="row.status === 'pending'">
                <el-button size="small" type="primary" @click="startTask(row.id)">启动</el-button>
              </template>
              <template v-else-if="row.status === 'running'">
                <el-button size="small" @click="pauseTask(row.id)">暂停</el-button>
                <el-popconfirm 
                  title="确定要停止该任务吗？停止后任务将标记为已停止状态。"
                  confirm-button-text="确认停止"
                  cancel-button-text="取消"
                  @confirm="stopTask(row.id)"
                >
                  <template #reference>
                    <el-button size="small" type="warning">停止</el-button>
                  </template>
                </el-popconfirm>
              </template>
              <template v-else-if="row.status === 'paused'">
                <el-button size="small" type="primary" @click="resumeTask(row.id)">恢复</el-button>
                <el-popconfirm 
                  title="确定要停止该任务吗？停止后任务将标记为已停止状态。"
                  confirm-button-text="确认停止"
                  cancel-button-text="取消"
                  @confirm="stopTask(row.id)"
                >
                  <template #reference>
                    <el-button size="small" type="warning">停止</el-button>
                  </template>
                </el-popconfirm>
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
    
    <!-- 配置导入对话框 -->
    <el-dialog v-model="showImportDialog" title="导入任务配置" width="600px">
      <el-tabs v-model="importTab">
        <el-tab-pane label="上传文件" name="file">
          <el-upload
            ref="importUploadRef"
            :auto-upload="false"
            :limit="1"
            accept=".json"
            :on-change="handleImportFileChange"
            :file-list="importFileList"
            drag
            class="import-upload"
          >
            <el-icon class="el-icon--upload"><Upload /></el-icon>
            <div class="el-upload__text">
              拖拽配置文件到此处，或<em>点击上传</em>
            </div>
            <template #tip>
              <div class="el-upload__tip">
                仅支持 JSON 格式的任务配置文件（通过「导出配置」功能生成）
              </div>
            </template>
          </el-upload>
        </el-tab-pane>
        
        <el-tab-pane label="粘贴JSON" name="paste">
          <el-input
            v-model="importJsonText"
            type="textarea"
            :rows="10"
            placeholder="粘贴任务配置 JSON..."
          />
        </el-tab-pane>
      </el-tabs>
      
      <!-- 配置预览 -->
      <div class="import-preview" v-if="importPreview">
        <h4>配置预览</h4>
        <div class="preview-item">
          <span class="label">任务名称:</span>
          <span class="value">{{ importPreview.name }}</span>
        </div>
        <div class="preview-item">
          <span class="label">迁移模式:</span>
          <el-tag size="small" :type="importPreview.migration_mode === 'full_only' ? 'info' : 'success'">
            {{ importPreview.migration_mode === 'full_only' ? '全量迁移' : '全量+增量' }}
          </el-tag>
        </div>
        <div class="preview-item">
          <span class="label">源集群:</span>
          <span class="value mono">{{ formatCluster(importPreview.source_cluster) }}</span>
        </div>
        <div class="preview-item">
          <span class="label">目标集群:</span>
          <span class="value mono">{{ formatCluster(importPreview.target_cluster) }}</span>
        </div>
      </div>
      
      <template #footer>
        <el-button @click="showImportDialog = false">取消</el-button>
        <el-button type="primary" @click="doImportConfig" :loading="importing" :disabled="!importPreview">
          创建任务
        </el-button>
      </template>
    </el-dialog>
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

// 配置导入相关
const showImportDialog = ref(false)
const importTab = ref('file')
const importUploadRef = ref(null)
const importFileList = ref([])
const importJsonText = ref('')
const importPreview = ref(null)
const importing = ref(false)

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

// 【新增】停止任务
const stopTask = async (id) => {
  try {
    await api.stopTask(id)
    ElMessage.success('任务已停止')
    fetchTasks()
  } catch (err) {
    ElMessage.error('停止失败: ' + (err.message || '未知错误'))
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
    stopped: '已停止',
    completed: '已完成',
    failed: '失败',
    incremental: '增量同步',
    incremental_stopped: '增量已停止',
    retrying: '重试中'
  }
  return map[status] || status
}

const getRowClass = ({ row }) => {
  return `row-${row.status}`
}

const formatNumber = (num) => {
  // 精确显示，不使用 K/M/B 缩写，添加千分位分隔符
  if (num === null || num === undefined) return '0'
  return num.toLocaleString('zh-CN')
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

// 配置导入相关方法
const handleImportFileChange = (file) => {
  if (!file.raw) return
  
  const reader = new FileReader()
  reader.onload = (e) => {
    try {
      const config = JSON.parse(e.target.result)
      importPreview.value = config
      importJsonText.value = e.target.result
    } catch (err) {
      ElMessage.error('JSON 格式解析失败')
      importPreview.value = null
    }
  }
  reader.readAsText(file.raw)
}

// 监听 JSON 文本变化
watch(importJsonText, (newVal) => {
  if (importTab.value === 'paste' && newVal.trim()) {
    try {
      importPreview.value = JSON.parse(newVal)
    } catch {
      importPreview.value = null
    }
  }
})

// 执行导入
const doImportConfig = async () => {
  if (!importPreview.value) {
    ElMessage.error('请先上传或粘贴有效的配置')
    return
  }
  
  importing.value = true
  try {
    const result = await api.importTaskConfig(importPreview.value)
    ElMessage.success('任务创建成功')
    showImportDialog.value = false
    
    // 重置状态
    importPreview.value = null
    importJsonText.value = ''
    importFileList.value = []
    
    // 跳转到任务详情
    const taskId = result?.task_id || result?.id
    if (taskId) {
      router.push(`/tasks/${taskId}`)
    } else {
      fetchTasks()
    }
  } catch (err) {
    ElMessage.error('导入失败: ' + (err.message || '未知错误'))
  } finally {
    importing.value = false
  }
}

// 格式化集群地址
const formatCluster = (cluster) => {
  if (!cluster) return '-'
  if (typeof cluster === 'string') return cluster
  if (cluster.addrs) return cluster.addrs.join(', ')
  return JSON.stringify(cluster)
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

.header-actions {
  display: flex;
  gap: 12px;
}

// 导入对话框样式
.import-upload {
  width: 100%;
  
  :deep(.el-upload-dragger) {
    padding: 30px 20px;
  }
}

.import-preview {
  margin-top: 20px;
  padding: 16px;
  background: var(--bg-primary);
  border-radius: var(--radius-md);
  border: 1px solid var(--border-light);
  
  h4 {
    font-size: 14px;
    font-weight: 600;
    color: var(--text-primary);
    margin-bottom: 12px;
  }
  
  .preview-item {
    display: flex;
    align-items: center;
    gap: 12px;
    padding: 8px 0;
    font-size: 13px;
    
    &:not(:last-child) {
      border-bottom: 1px dashed var(--border-light);
    }
    
    .label {
      width: 80px;
      color: var(--text-secondary);
      flex-shrink: 0;
    }
    
    .value {
      color: var(--text-primary);
      
      &.mono {
        font-family: 'Consolas', 'Monaco', monospace;
        font-size: 12px;
        word-break: break-all;
      }
    }
  }
}
</style>
