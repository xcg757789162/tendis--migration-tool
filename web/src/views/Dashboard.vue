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
    
    <!-- 运行中的任务 -->
    <div class="section">
      <div class="section-header">
        <h2>
          <el-icon><VideoPlay /></el-icon>
          运行中的任务
        </h2>
        <router-link to="/tasks" class="view-all">查看全部 <el-icon><ArrowRight /></el-icon></router-link>
      </div>
      
      <div class="running-tasks" v-if="runningTasks.length">
        <div class="task-card" v-for="task in runningTasks" :key="task.id">
          <div class="task-header">
            <div class="task-name">{{ task.name }}</div>
            <span class="status-tag running">运行中</span>
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
            <el-button size="small" @click="pauseTask(task.id)">
              <el-icon><VideoPause /></el-icon> 暂停
            </el-button>
            <el-button size="small" type="primary" @click="$router.push(`/tasks/${task.id}`)">
              <el-icon><View /></el-icon> 详情
            </el-button>
          </div>
        </div>
      </div>
      
      <el-empty v-else description="暂无运行中的任务">
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
      </div>
      
      <div class="system-info">
        <div class="info-card">
          <div class="info-icon">
            <el-icon :size="20"><Cpu /></el-icon>
          </div>
          <div class="info-content">
            <div class="info-label">Worker 进程</div>
            <div class="info-value">{{ systemStatus.worker_count || 0 }} 个运行中</div>
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
      </div>
    </div>
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

const runningTasks = ref([])
const systemStatus = ref({})
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
    
    runningTasks.value = tasks.filter(t => t.status === 'running').slice(0, 3)
    
    // 获取系统状态
    systemStatus.value = await api.getSystemStatus()
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

const goToTasks = (status) => {
  if (status === 'all') {
    router.push('/tasks')
  } else {
    router.push({ path: '/tasks', query: { status } })
  }
}

const formatNumber = (num) => {
  if (num >= 1000000000) return (num / 1000000000).toFixed(1) + 'B'
  if (num >= 1000000) return (num / 1000000).toFixed(1) + 'M'
  if (num >= 1000) return (num / 1000).toFixed(1) + 'K'
  return num.toString()
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
    }
  }
}
</style>
