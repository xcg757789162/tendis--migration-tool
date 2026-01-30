<template>
  <div class="app-container">
    <!-- 顶部导航 -->
    <header class="app-header">
      <div class="header-left">
        <div class="logo">
          <div class="logo-icon">
            <el-icon :size="28"><DataLine /></el-icon>
          </div>
          <div class="logo-text">
            <span class="title">Tendis Migration</span>
            <span class="subtitle">数据迁移工具 v1.4</span>
          </div>
        </div>
      </div>
      
      <nav class="header-nav">
        <router-link 
          v-for="item in navItems" 
          :key="item.path" 
          :to="item.path"
          :class="['nav-item', { active: isActive(item.path) }]"
        >
          <el-icon><component :is="item.icon" /></el-icon>
          <span>{{ item.name }}</span>
        </router-link>
      </nav>
      
      <div class="header-right">
        <div class="status-indicator" :class="{ online: systemOnline }">
          <span class="dot"></span>
          <span>{{ systemOnline ? '系统在线' : '连接中...' }}</span>
        </div>
      </div>
    </header>
    
    <!-- 装饰线 -->
    <div class="tech-line"></div>
    
    <!-- 主内容 -->
    <main class="app-main">
      <router-view v-slot="{ Component }">
        <transition name="fade" mode="out-in">
          <component :is="Component" />
        </transition>
      </router-view>
    </main>
    
    <!-- 背景装饰 -->
    <div class="bg-decoration">
      <div class="circle c1"></div>
      <div class="circle c2"></div>
      <div class="circle c3"></div>
    </div>
  </div>
</template>

<script setup>
import { ref, onMounted, computed } from 'vue'
import { useRoute } from 'vue-router'
import api from '@/api'

const route = useRoute()
const systemOnline = ref(false)

const navItems = [
  { path: '/dashboard', name: '控制台', icon: 'DataBoard' },
  { path: '/tasks', name: '迁移任务', icon: 'List' },
  { path: '/create', name: '创建任务', icon: 'Plus' },
  { path: '/logs', name: '系统日志', icon: 'Document' }
]

const isActive = (path) => {
  return route.path === path || route.path.startsWith(path + '/')
}

const checkSystemStatus = async () => {
  try {
    await api.getSystemStatus()
    systemOnline.value = true
  } catch {
    systemOnline.value = false
  }
}

onMounted(() => {
  checkSystemStatus()
  setInterval(checkSystemStatus, 30000)
})
</script>

<style lang="scss" scoped>
.app-container {
  min-height: 100vh;
  position: relative;
  overflow: hidden;
}

.app-header {
  height: 72px;
  background: var(--bg-secondary);
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: 0 32px;
  position: sticky;
  top: 0;
  z-index: 100;
  box-shadow: var(--shadow-sm);
}

.header-left {
  .logo {
    display: flex;
    align-items: center;
    gap: 12px;
    
    .logo-icon {
      width: 44px;
      height: 44px;
      background: var(--gradient-blue);
      border-radius: var(--radius-md);
      display: flex;
      align-items: center;
      justify-content: center;
      color: white;
    }
    
    .logo-text {
      display: flex;
      flex-direction: column;
      
      .title {
        font-size: 18px;
        font-weight: 700;
        color: var(--text-primary);
        letter-spacing: -0.5px;
      }
      
      .subtitle {
        font-size: 12px;
        color: var(--text-tertiary);
      }
    }
  }
}

.header-nav {
  display: flex;
  gap: 8px;
  
  .nav-item {
    display: flex;
    align-items: center;
    gap: 8px;
    padding: 10px 20px;
    border-radius: var(--radius-md);
    color: var(--text-secondary);
    text-decoration: none;
    font-weight: 500;
    transition: all 0.3s ease;
    
    &:hover {
      background: var(--bg-hover);
      color: var(--primary-color);
    }
    
    &.active {
      background: var(--primary-lighter);
      color: var(--primary-color);
    }
  }
}

.header-right {
  .status-indicator {
    display: flex;
    align-items: center;
    gap: 8px;
    padding: 8px 16px;
    background: var(--bg-primary);
    border-radius: 20px;
    font-size: 13px;
    color: var(--text-secondary);
    
    .dot {
      width: 8px;
      height: 8px;
      border-radius: 50%;
      background: var(--text-tertiary);
    }
    
    &.online {
      .dot {
        background: var(--success-color);
        box-shadow: 0 0 8px var(--success-color);
        animation: pulse-dot 2s infinite;
      }
    }
  }
}

@keyframes pulse-dot {
  0%, 100% {
    opacity: 1;
  }
  50% {
    opacity: 0.5;
  }
}

.app-main {
  padding: 32px;
  position: relative;
  z-index: 1;
}

.bg-decoration {
  position: fixed;
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  pointer-events: none;
  z-index: 0;
  overflow: hidden;
  
  .circle {
    position: absolute;
    border-radius: 50%;
    background: var(--gradient-blue);
    opacity: 0.03;
    
    &.c1 {
      width: 600px;
      height: 600px;
      top: -200px;
      right: -200px;
    }
    
    &.c2 {
      width: 400px;
      height: 400px;
      bottom: -100px;
      left: -100px;
    }
    
    &.c3 {
      width: 300px;
      height: 300px;
      top: 50%;
      right: 10%;
    }
  }
}
</style>
