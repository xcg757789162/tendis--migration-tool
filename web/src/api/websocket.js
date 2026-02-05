/**
 * WebSocket 服务类
 * 支持任务实时更新：metrics, logs, status
 */

class WebSocketService {
  constructor() {
    this.ws = null
    this.reconnectAttempts = 0
    this.maxReconnectAttempts = 5
    this.reconnectInterval = 3000
    this.reconnectTimer = null
    this.pingTimer = null
    this.pingInterval = 30000
    
    // 消息处理器
    this.handlers = {
      metrics: [],
      log: [],
      status: [],
      subscribed: [],
      unsubscribed: [],
      pong: [],
      error: []
    }
    
    // 连接状态回调
    this.onConnected = null
    this.onDisconnected = null
    this.onError = null
    
    // 当前订阅的任务ID列表
    this.subscribedTasks = new Set()
    
    // 绑定方法上下文
    this.handleOpen = this.handleOpen.bind(this)
    this.handleMessage = this.handleMessage.bind(this)
    this.handleClose = this.handleClose.bind(this)
    this.handleError = this.handleError.bind(this)
  }
  
  /**
   * 连接 WebSocket
   */
  connect() {
    if (this.ws && (this.ws.readyState === WebSocket.CONNECTING || this.ws.readyState === WebSocket.OPEN)) {
      console.log('[WS] Already connected or connecting')
      return
    }
    
    // 构建 WebSocket URL
    const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:'
    const host = window.location.host
    const url = `${protocol}//${host}/ws`
    
    console.log('[WS] Connecting to:', url)
    
    try {
      this.ws = new WebSocket(url)
      this.ws.onopen = this.handleOpen
      this.ws.onmessage = this.handleMessage
      this.ws.onclose = this.handleClose
      this.ws.onerror = this.handleError
    } catch (error) {
      console.error('[WS] Connection error:', error)
      this.scheduleReconnect()
    }
  }
  
  /**
   * 断开连接
   */
  disconnect() {
    console.log('[WS] Disconnecting...')
    
    // 清除定时器
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer)
      this.reconnectTimer = null
    }
    if (this.pingTimer) {
      clearInterval(this.pingTimer)
      this.pingTimer = null
    }
    
    // 关闭连接
    if (this.ws) {
      this.ws.onclose = null // 防止触发重连
      this.ws.close()
      this.ws = null
    }
    
    // 清空订阅
    this.subscribedTasks.clear()
    this.reconnectAttempts = 0
  }
  
  /**
   * 订阅任务更新
   * @param {string} taskId 任务ID
   */
  subscribe(taskId) {
    if (!taskId) return
    
    if (this.subscribedTasks.has(taskId)) {
      console.log('[WS] Already subscribed to task:', taskId)
      return
    }
    
    const message = {
      type: 'subscribe',
      task_id: taskId
    }
    
    this.send(message)
    this.subscribedTasks.add(taskId)
    console.log('[WS] Subscribing to task:', taskId)
  }
  
  /**
   * 取消订阅任务更新
   * @param {string} taskId 任务ID
   */
  unsubscribe(taskId) {
    if (!taskId) return
    
    if (!this.subscribedTasks.has(taskId)) {
      return
    }
    
    const message = {
      type: 'unsubscribe',
      task_id: taskId
    }
    
    this.send(message)
    this.subscribedTasks.delete(taskId)
    console.log('[WS] Unsubscribing from task:', taskId)
  }
  
  /**
   * 发送消息
   * @param {object} message 消息对象
   */
  send(message) {
    if (!this.ws || this.ws.readyState !== WebSocket.OPEN) {
      console.warn('[WS] Cannot send message, not connected')
      return false
    }
    
    try {
      this.ws.send(JSON.stringify(message))
      return true
    } catch (error) {
      console.error('[WS] Send error:', error)
      return false
    }
  }
  
  /**
   * 注册消息处理器
   * @param {string} type 消息类型 (metrics, log, status)
   * @param {function} handler 处理函数
   * @returns {function} 取消注册函数
   */
  on(type, handler) {
    if (!this.handlers[type]) {
      this.handlers[type] = []
    }
    this.handlers[type].push(handler)
    
    // 返回取消注册函数
    return () => {
      const index = this.handlers[type].indexOf(handler)
      if (index > -1) {
        this.handlers[type].splice(index, 1)
      }
    }
  }
  
  /**
   * 移除所有处理器
   * @param {string} type 可选，指定消息类型
   */
  off(type) {
    if (type) {
      this.handlers[type] = []
    } else {
      Object.keys(this.handlers).forEach(key => {
        this.handlers[key] = []
      })
    }
  }
  
  // === 内部方法 ===
  
  handleOpen() {
    console.log('[WS] Connected')
    this.reconnectAttempts = 0
    
    // 启动心跳
    this.startPing()
    
    // 重新订阅之前的任务
    this.subscribedTasks.forEach(taskId => {
      this.send({ type: 'subscribe', task_id: taskId })
    })
    
    // 回调
    if (this.onConnected) {
      this.onConnected()
    }
  }
  
  handleMessage(event) {
    try {
      // 消息可能是多行的（批量发送）
      const messages = event.data.split('\n').filter(s => s.trim())
      
      messages.forEach(msgStr => {
        const message = JSON.parse(msgStr)
        const { type, task_id, payload } = message
        
        // 分发消息给处理器
        const handlers = this.handlers[type]
        if (handlers && handlers.length > 0) {
          handlers.forEach(handler => {
            try {
              handler({ taskId: task_id, payload, type })
            } catch (e) {
              console.error('[WS] Handler error:', e)
            }
          })
        }
      })
    } catch (error) {
      console.error('[WS] Message parse error:', error, event.data)
    }
  }
  
  handleClose(event) {
    console.log('[WS] Closed:', event.code, event.reason)
    
    // 停止心跳
    if (this.pingTimer) {
      clearInterval(this.pingTimer)
      this.pingTimer = null
    }
    
    // 回调
    if (this.onDisconnected) {
      this.onDisconnected()
    }
    
    // 尝试重连
    this.scheduleReconnect()
  }
  
  handleError(error) {
    console.error('[WS] Error:', error)
    
    if (this.onError) {
      this.onError(error)
    }
  }
  
  scheduleReconnect() {
    if (this.reconnectAttempts >= this.maxReconnectAttempts) {
      console.log('[WS] Max reconnect attempts reached')
      return
    }
    
    this.reconnectAttempts++
    const delay = this.reconnectInterval * this.reconnectAttempts
    
    console.log(`[WS] Reconnecting in ${delay}ms (attempt ${this.reconnectAttempts}/${this.maxReconnectAttempts})`)
    
    this.reconnectTimer = setTimeout(() => {
      this.connect()
    }, delay)
  }
  
  startPing() {
    if (this.pingTimer) {
      clearInterval(this.pingTimer)
    }
    
    this.pingTimer = setInterval(() => {
      this.send({ type: 'ping' })
    }, this.pingInterval)
  }
  
  /**
   * 获取连接状态
   */
  get isConnected() {
    return this.ws && this.ws.readyState === WebSocket.OPEN
  }
  
  /**
   * 获取连接状态文本
   */
  get connectionState() {
    if (!this.ws) return 'CLOSED'
    switch (this.ws.readyState) {
      case WebSocket.CONNECTING: return 'CONNECTING'
      case WebSocket.OPEN: return 'OPEN'
      case WebSocket.CLOSING: return 'CLOSING'
      case WebSocket.CLOSED: return 'CLOSED'
      default: return 'UNKNOWN'
    }
  }
}

// 导出单例
export const wsService = new WebSocketService()

export default wsService
