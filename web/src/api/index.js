import axios from 'axios'
import { ElMessage } from 'element-plus'

const api = axios.create({
  baseURL: '/api/v1',
  timeout: 30000
})

api.interceptors.response.use(
  response => {
    const data = response.data
    if (data.code !== 0) {
      ElMessage.error(data.message || '请求失败')
      return Promise.reject(new Error(data.message))
    }
    return data.data
  },
  error => {
    ElMessage.error(error.message || '网络错误')
    return Promise.reject(error)
  }
)

export default {
  // 任务相关
  getTasks(params) {
    return api.get('/tasks', { params })
  },
  
  getTask(id) {
    return api.get(`/tasks/${id}`)
  },
  
  createTask(data) {
    return api.post('/tasks', data)
  },
  
  deleteTask(id) {
    return api.delete(`/tasks/${id}`, {
      headers: { 'X-Confirm-Password': 'confirm-delete' }
    })
  },
  
  startTask(id) {
    return api.post(`/tasks/${id}/start`)
  },
  
  pauseTask(id) {
    return api.post(`/tasks/${id}/pause`)
  },
  
  resumeTask(id) {
    return api.post(`/tasks/${id}/resume`)
  },
  
  // 停止增量同步（手动停止，任务进入完成前准备状态）
  stopIncrementalSync(id) {
    return api.post(`/tasks/${id}/stop-incremental`)
  },
  
  // 完成任务（停止增量同步并标记完成）
  completeTask(id, skipVerify = false) {
    return api.post(`/tasks/${id}/complete`, null, {
      params: { skip_verify: skipVerify }
    })
  },
  
  // 更新任务配置（运行时调整）
  updateTaskConfig(id, data) {
    return api.put(`/tasks/${id}/config`, data)
  },
  
  getProgress(id) {
    return api.get(`/tasks/${id}/progress`)
  },
  
  getMetrics(id) {
    return api.get(`/tasks/${id}/metrics`)
  },
  
  triggerVerify(id) {
    return api.post(`/tasks/${id}/verify`)
  },
  
  getVerifyResults(id) {
    return api.get(`/tasks/${id}/verify/results`)
  },
  
  getReport(id, format = 'json') {
    return api.get(`/tasks/${id}/report`, { params: { format } })
  },
  
  // 系统相关
  getSystemStatus() {
    return api.get('/system/status')
  },
  
  getWorkers() {
    return api.get('/system/workers')
  },
  
  // 日志相关
  getLogs(params) {
    return api.get('/logs', { params })
  },
  
  getLogsStats() {
    return api.get('/logs/stats')
  },
  
  getTaskLogs(taskId, params) {
    return api.get(`/tasks/${taskId}/logs`, { params })
  },
  
  clearLogs() {
    return api.post('/logs/clear')
  },

  // 日志清理（清理旧日志文件）
  cleanupLogs() {
    return api.post('/logs/cleanup')
  },

  // 测试连接
  testConnection(data) {
    return api.post('/test-connection', data)
  },

  // 分析集群
  analyzeCluster(data) {
    return api.post('/analyze-cluster', data)
  },

  // 获取推荐配置
  getRecommendedConfig(data) {
    return api.post('/recommend-config', data)
  },
  
  // 异常Key
  getErrorKeys(taskId) {
    return api.get(`/tasks/${taskId}/error-keys`)
  },
  
  downloadErrorKeys(taskId) {
    return axios.get(`/api/v1/tasks/${taskId}/error-keys/download`, {
      responseType: 'blob'
    }).then(res => res.data)
  },

  // 模板相关
  getTemplates() {
    return api.get('/templates')
  },

  getTemplate(id) {
    return api.get(`/templates/${id}`)
  },

  createTemplate(data) {
    return api.post('/templates', data)
  },

  updateTemplate(id, data) {
    return api.put(`/templates/${id}`, data)
  },

  deleteTemplate(id) {
    return api.delete(`/templates/${id}`)
  },

  createTaskFromTemplate(templateId, data) {
    return api.post(`/templates/${templateId}/create-task`, data)
  },

  // 任务配置导出
  exportTaskConfig(id, asFile = false) {
    const params = asFile ? { format: 'file' } : {}
    return api.get(`/tasks/${id}/export`, { params })
  },

  // 任务配置导入
  importTaskConfig(data) {
    return api.post('/tasks/import', data)
  },

  // 下载任务报告
  downloadTaskReport(taskId, format = 'csv') {
    return axios.get(`/api/v1/tasks/${taskId}/report`, {
      params: { format },
      responseType: format === 'csv' ? 'blob' : 'json'
    }).then(res => res.data)
  },

  // 影子模式统计
  getShadowStats(taskId) {
    return api.get(`/tasks/${taskId}/shadow-stats`)
  },

  // 任务健康状态
  getTaskHealth(taskId) {
    return api.get(`/tasks/${taskId}/health`)
  },

  // 自动恢复状态
  getAutoRecoveryStatus(taskId) {
    return api.get(`/tasks/${taskId}/auto-recovery`)
  },

  // 切换自动恢复
  toggleAutoRecovery(taskId, data) {
    return api.post(`/tasks/${taskId}/auto-recovery`, data)
  },

  // 重试失败的 Key
  retryFailedKeys(taskId) {
    return api.post(`/tasks/${taskId}/retry-failed`)
  },

  // 智能重试状态
  getSmartRetryStatus() {
    return api.get('/smart-retry/status')
  },

  // 系统备份
  createSystemBackup() {
    return api.post('/system/backup')
  },

  // 系统健康检查
  getHealth() {
    return api.get('/health')
  },

  // 系统详细健康检查
  getHealthDetailed() {
    return api.get('/health/detailed')
  },

  // Key 清单上传
  uploadKeyList(formData) {
    return axios.post('/api/v1/upload-keylist', formData, {
      headers: { 'Content-Type': 'multipart/form-data' }
    }).then(res => res.data?.data || res.data)
  },

  // 解析 Key 清单（用于预览）
  parseKeyList(content, format = 'txt') {
    return api.post('/parse-keylist', { content, format })
  },

  // 日志导出
  exportLogs(params) {
    return axios.get('/api/v1/logs/export', {
      params,
      responseType: 'blob'
    }).then(res => res.data)
  }
}
