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
  }
}
