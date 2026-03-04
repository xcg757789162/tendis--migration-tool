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
          <el-popconfirm 
            title="确定要停止该任务吗？停止后任务将无法恢复。"
            confirm-button-text="确认停止"
            cancel-button-text="取消"
            @confirm="stopTask"
          >
            <template #reference>
              <el-button type="danger">
                <el-icon><CircleClose /></el-icon> 停止
              </el-button>
            </template>
          </el-popconfirm>
          <el-button type="primary" @click="triggerVerify">
            <el-icon><Checked /></el-icon> 校验
          </el-button>
          <!-- 增量同步阶段显示完成按钮 -->
          <el-popconfirm 
            v-if="progress?.phase === 'incremental'"
            title="确定要停止增量同步吗？任务将标记为已完成。"
            confirm-button-text="停止任务"
            cancel-button-text="取消"
            @confirm="completeTask"
          >
            <template #reference>
              <el-button type="warning">
                <el-icon><CircleCheck /></el-icon> 完成任务
              </el-button>
            </template>
          </el-popconfirm>
        </template>
        <template v-else-if="task.status === 'paused'">
          <el-button type="primary" @click="resumeTask">
            <el-icon><VideoPlay /></el-icon> 恢复
          </el-button>
          <el-popconfirm 
            title="确定要停止该任务吗？停止后任务将无法恢复。"
            confirm-button-text="确认停止"
            cancel-button-text="取消"
            @confirm="stopTask"
          >
            <template #reference>
              <el-button type="danger">
                <el-icon><CircleClose /></el-icon> 停止
              </el-button>
            </template>
          </el-popconfirm>
        </template>
        <template v-else-if="task.status === 'incremental_stopped'">
          <el-button type="primary" @click="triggerVerify">
            <el-icon><Checked /></el-icon> 执行校验
          </el-button>
          <el-button type="success" @click="markComplete">
            <el-icon><CircleCheck /></el-icon> 标记完成
          </el-button>
        </template>
        
        <!-- 更多操作下拉菜单 -->
        <el-dropdown @command="handleMoreActions" trigger="click">
          <el-button>
            <el-icon><MoreFilled /></el-icon> 更多
          </el-button>
          <template #dropdown>
            <el-dropdown-menu>
              <el-dropdown-item command="export-report-csv">
                <el-icon><Download /></el-icon> 导出报告 (CSV)
              </el-dropdown-item>
              <el-dropdown-item command="export-report-json">
                <el-icon><Download /></el-icon> 导出报告 (JSON)
              </el-dropdown-item>
              <el-dropdown-item command="export-config" divided>
                <el-icon><Document /></el-icon> 导出配置
              </el-dropdown-item>
              <el-dropdown-item command="view-health">
                <el-icon><Promotion /></el-icon> 健康状态
              </el-dropdown-item>
              <el-dropdown-item command="toggle-auto-recovery" divided>
                <el-icon><Refresh /></el-icon> 自动恢复设置
              </el-dropdown-item>
              <el-dropdown-item command="retry-failed" :disabled="errorKeys.failed === 0">
                <el-icon><RefreshRight /></el-icon> 重试失败Key
              </el-dropdown-item>
            </el-dropdown-menu>
          </template>
        </el-dropdown>
      </div>
    </div>
    
    <!-- 集群拓扑健康告警 -->
    <div class="topology-warnings" v-if="task.topology_warnings && task.topology_warnings.length > 0">
      <el-alert
        v-for="(warning, idx) in task.topology_warnings"
        :key="idx"
        :title="warning"
        type="warning"
        :closable="false"
        show-icon
        style="margin-bottom: 8px"
      >
        <template #default>
          <span>部分 Key 可能迁移失败，建议先修复集群拓扑（清理无效节点）后，再手动重试失败的 Key。</span>
        </template>
      </el-alert>
    </div>

    <!-- 进度概览 -->
    <div class="progress-overview card" v-if="progress">
      <div class="overview-header">
        <h2>迁移进度</h2>
        <div class="header-right">
          <el-select v-model="refreshInterval" size="small" style="width: 140px" @change="changeRefreshInterval">
            <el-option label="实时更新" :value="0">
              <span class="realtime-option">
                <span class="dot" :class="{ connected: wsConnected }"></span>
                实时更新
              </span>
            </el-option>
            <el-option label="每 10 秒" :value="10000" />
            <el-option label="每 30 秒" :value="30000" />
            <el-option label="每 1 分钟" :value="60000" />
          </el-select>
          <el-tooltip v-if="refreshInterval === 0" :content="wsConnected ? 'WebSocket 已连接' : 'WebSocket 未连接'" placement="top">
            <span class="ws-status" :class="{ connected: wsConnected }">
              <span class="status-dot"></span>
              {{ wsConnected ? '实时' : '离线' }}
            </span>
          </el-tooltip>
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
            <template v-if="isEstimating">
              <span class="percent estimating">~</span>
              <span class="unit estimating-hint">估算中</span>
            </template>
            <template v-else>
              <span class="percent">{{ progress.percentage?.toFixed(1) || 0 }}</span>
              <span class="unit">%</span>
            </template>
          </div>
        </div>
        
        <div class="progress-stats compact-layout">
          <div class="stat-grid">
            <div class="stat-item">
              <span class="label">已迁移Key</span>
              <span class="value stat-number">{{ formatNumber(progress.migrated_keys || 0) }}</span>
            </div>
            <div class="stat-item">
              <span class="label">待迁移Key</span>
              <span class="value highlight" :class="{ warning: progress.keys_to_migrate === 0 && progress.total_keys > 0 }">
                {{ formatNumber(progress.keys_to_migrate || 0) }}
              </span>
              <el-tooltip v-if="progress.keys_to_migrate === 0 && progress.total_keys > 0" effect="dark" placement="top">
                <template #content>没有匹配前缀过滤条件的Key，请检查过滤配置</template>
                <el-icon class="warning-icon"><Warning /></el-icon>
              </el-tooltip>
            </div>
            <div class="stat-item">
              <span class="label">总Key数</span>
              <span class="value muted" v-if="progress.total_keys > 0">{{ formatNumber(progress.total_keys) }}</span>
              <span class="value muted estimating-text" v-else-if="isTaskRunning">估算中...</span>
              <span class="value muted" v-else>0</span>
            </div>
            <div class="stat-item">
              <span class="label">过滤Key</span>
              <span class="value filtered">{{ formatNumber(task.keys_filtered || 0) }}</span>
            </div>
            <div class="stat-item">
              <span class="label">已迁移数据</span>
              <span class="value">{{ formatBytes(progress.migrated_bytes || 0) }}</span>
            </div>
            <div class="stat-item">
              <span class="label">总数据量</span>
              <span class="value">{{ formatBytes(progress.total_bytes || 0) }}</span>
            </div>
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
              <span class="value elapsed">{{ elapsedTimeDisplay }}</span>
            </div>
          </div>
        </div>
      </div>
    </div>
    
    <!-- 迁移前依赖校验（pending 状态显示） -->
    <div class="preflight-check card" v-if="task.status === 'pending'">
      <div class="preflight-header">
        <h3><el-icon><CircleCheck /></el-icon> 迁移前校验</h3>
        <el-button type="primary" size="small" @click="runPreflightCheck" :loading="preflightLoading">
          <el-icon><Refresh /></el-icon> {{ preflightChecked ? '重新校验' : '开始校验' }}
        </el-button>
      </div>
      
      <div v-if="!preflightChecked && !preflightLoading" class="preflight-hint">
        <el-alert type="info" :closable="false" show-icon>
          <template #title>建议在启动任务前执行依赖校验，确认源端/目标端连接、集群拓扑、增量同步等环境是否就绪。</template>
        </el-alert>
      </div>
      
      <div v-if="preflightChecks.length > 0" class="preflight-results">
        <div v-if="preflightResult" class="preflight-summary">
          <el-alert 
            :type="preflightResult.can_start ? (preflightResult.all_passed ? 'success' : 'warning') : 'error'" 
            :closable="false"
            show-icon
          >
            <template #title>
              <span v-if="preflightResult.can_start && preflightResult.all_passed">所有校验项通过，可以启动迁移</span>
              <span v-else-if="preflightResult.can_start">存在警告项，但可以启动迁移（建议关注警告内容）</span>
              <span v-else>存在必须通过的校验项未通过，无法启动迁移</span>
            </template>
          </el-alert>
        </div>
        
        <div class="check-list">
          <div v-for="(check, index) in preflightChecks" :key="index" 
               class="check-item" :class="check.status">
            <div class="check-icon">
              <el-icon v-if="check.status === 'passed'" color="#67c23a"><SuccessFilled /></el-icon>
              <el-icon v-else-if="check.status === 'failed'" color="#f56c6c"><CircleCloseFilled /></el-icon>
              <el-icon v-else color="#e6a23c"><WarningFilled /></el-icon>
            </div>
            <div class="check-content">
              <div class="check-title">
                <span class="check-name">{{ check.name }}</span>
                <el-tag v-if="check.required" size="small" type="danger" effect="plain">必须</el-tag>
                <el-tag v-else size="small" type="info" effect="plain">可选</el-tag>
              </div>
              <div class="check-message">{{ check.message }}</div>
              <div class="check-detail" v-if="check.details">{{ check.details }}</div>
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
            @click="openConfigDialog"
            class="adjust-btn"
          >
            <el-icon><Edit /></el-icon> 调整参数
          </el-button>
        </div>
        <div class="config-info compact">
          <div class="config-grid">
            <div class="config-item">
              <span class="label">迁移模式</span>
              <span class="value">
                <el-tag :type="task.migration_mode === 'full' ? 'info' : 'success'" size="small">
                  {{ getMigrationModeText(task.migration_mode) }}
                </el-tag>
              </span>
            </div>
            <div class="config-item">
              <span class="label">Worker配置</span>
              <span class="value highlight">{{ config?.worker_count || 4 }}</span>
            </div>
            <div class="config-item" v-if="task.status === 'running'">
              <span class="label">活跃Worker</span>
              <span class="value" :class="{'adjusting': task.active_workers !== (config?.worker_count || 4)}">
                {{ task.active_workers || config?.worker_count || 4 }}
                <el-icon v-if="task.active_workers !== (config?.worker_count || 4)" class="adjusting-icon"><Loading /></el-icon>
              </span>
            </div>
            <div class="config-item">
              <span class="label">批次大小</span>
              <span class="value">{{ config?.scan_batch_size || 1000 }}</span>
            </div>
            <div class="config-item">
              <span class="label">源端QPS</span>
              <span class="value">{{ config?.rate_limit?.source_qps || '不限' }}</span>
            </div>
            <div class="config-item">
              <span class="label">目标QPS</span>
              <span class="value">{{ config?.rate_limit?.target_qps || '不限' }}</span>
            </div>
            <div class="config-item">
              <span class="label">源端连接</span>
              <span class="value">{{ config?.rate_limit?.source_connections || 50 }}</span>
            </div>
            <div class="config-item">
              <span class="label">目标连接</span>
              <span class="value">{{ config?.rate_limit?.target_connections || 50 }}</span>
            </div>
            <div class="config-item">
              <span class="label">冲突策略</span>
              <span class="value">{{ getConflictPolicyText(config?.conflict_policy) }}</span>
            </div>
            <div class="config-item">
              <span class="label">大Key阈值</span>
              <span class="value">{{ formatBytes(config?.large_key_threshold || 10485760) }}</span>
            </div>
            <div class="config-item">
              <span class="label">从Slave读取</span>
              <span class="value">
                <el-tag v-if="config?.read_from_slave" type="success" size="small">是</el-tag>
                <el-tag v-else type="info" size="small">否</el-tag>
              </span>
            </div>
            <div class="config-item">
              <span class="label">最大重试</span>
              <span class="value">{{ config?.retry_config?.max_retries ?? 3 }} 次</span>
            </div>
            <div class="config-item">
              <span class="label">全量重试间隔</span>
              <span class="value">{{ config?.retry_config?.full_retry_interval_ms ?? 100 }} ms</span>
            </div>
            <div class="config-item">
              <span class="label">增量重试间隔</span>
              <span class="value">{{ config?.retry_config?.incr_retry_interval_ms ?? 1000 }} ms</span>
            </div>
          </div>
          <!-- Key过滤配置 -->
          <div class="filter-section" v-if="config?.key_filter">
            <div class="filter-title">
              <el-icon><Filter /></el-icon> Key过滤
            </div>
            <div class="filter-content">
              <div class="filter-item" v-if="config?.key_filter?.mode">
                <span class="label">模式:</span>
                <span class="value">{{ getFilterModeText(config?.key_filter?.mode) }}</span>
              </div>
              <div class="filter-item" v-if="config?.key_filter?.prefixes?.length">
                <span class="label">包含前缀:</span>
                <span class="value mono">{{ config?.key_filter?.prefixes?.join(', ') }}</span>
              </div>
              <div class="filter-item" v-if="config?.key_filter?.exclude_prefixes?.length">
                <span class="label">排除前缀:</span>
                <span class="value mono warning">{{ config?.key_filter?.exclude_prefixes?.join(', ') }}</span>
              </div>
              <div class="filter-item" v-if="config?.key_filter?.patterns?.length">
                <span class="label">匹配模式:</span>
                <span class="value mono">{{ config?.key_filter?.patterns?.join(', ') }}</span>
              </div>
              <div class="filter-item no-filter" v-if="!config?.key_filter?.prefixes?.length && !config?.key_filter?.exclude_prefixes?.length && !config?.key_filter?.patterns?.length">
                <span class="value">无过滤规则，迁移所有Key</span>
              </div>
            </div>
          </div>
          <div class="filter-section no-filter-config" v-else>
            <div class="filter-title">
              <el-icon><Filter /></el-icon> Key过滤
            </div>
            <div class="filter-content">
              <span class="no-filter-text">未配置过滤规则，迁移所有Key</span>
            </div>
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
    
    <!-- 影子模式统计（仅在影子模式时显示） -->
    <div class="shadow-stats-section card" v-if="isShadowMode && shadowStats">
      <h3><el-icon><View /></el-icon> 影子模式统计</h3>
      <el-alert type="info" :closable="false" style="margin-bottom: 16px;">
        <template #title>
          <strong>影子模式</strong> - 仅读取源端数据，不写入目标端
        </template>
        用于验证筛选规则和预估迁移工作量，对目标端生产环境零影响。
      </el-alert>
      
      <div class="shadow-stats-grid">
        <div class="stat-item">
          <span class="stat-value">{{ formatNumber(shadowStats.keys_scanned || 0) }}</span>
          <span class="stat-label">已扫描 Key</span>
        </div>
        <div class="stat-item">
          <span class="stat-value highlight">{{ formatNumber(shadowStats.keys_matched || 0) }}</span>
          <span class="stat-label">匹配规则 Key</span>
        </div>
        <div class="stat-item">
          <span class="stat-value warning">{{ formatNumber(shadowStats.keys_skipped || 0) }}</span>
          <span class="stat-label">被过滤 Key</span>
        </div>
        <div class="stat-item">
          <span class="stat-value">{{ formatBytes(shadowStats.bytes_read || 0) }}</span>
          <span class="stat-label">读取数据量</span>
        </div>
        <div class="stat-item">
          <span class="stat-value info">{{ formatNumber(shadowStats.large_keys_found || 0) }}</span>
          <span class="stat-label">大 Key 数量</span>
        </div>
      </div>
      
      <!-- 数据类型分布 -->
      <div class="type-distribution" v-if="shadowStats.type_distribution">
        <h4>数据类型分布</h4>
        <div class="type-grid">
          <div class="type-item" v-for="(count, type) in shadowStats.type_distribution" :key="type">
            <span class="type-name">{{ type }}</span>
            <span class="type-count">{{ formatNumber(count) }}</span>
          </div>
        </div>
      </div>
    </div>
    
    <!-- 增量同步统计（仅在增量同步阶段或有增量数据时显示） -->
    <div class="incremental-stats-section card" v-if="showIncrementalStats">
      <div class="section-header">
        <h3><el-icon><DataLine /></el-icon> 增量同步统计</h3>
        <div class="header-right">
          <el-tag :type="incrSyncMode === 'binlog' ? 'success' : 'warning'" size="small">
            {{ incrSyncMode === 'binlog' ? 'Binlog 模式' : '时间窗口模式' }}
          </el-tag>
          <span class="sync-status" :class="{ syncing: progress?.phase === 'incremental' }">
            <span class="dot"></span>
            {{ incrStatusText }}
          </span>
        </div>
      </div>
      
      <el-alert v-if="incrSyncMode === 'binlog'" type="success" :closable="false" style="margin-bottom: 16px;">
        <template #title>
          <strong>Binlog 实时同步</strong> - 伪装成 Tendis Slave 接收增量数据
        </template>
        低延迟、高效率，适用于 40 亿 Key 级别的大规模数据同步。
      </el-alert>
      <el-alert v-else type="warning" :closable="false" style="margin-bottom: 16px;">
        <template #title>
          <strong>时间窗口模式</strong> - 定时扫描检测变更 Key
        </template>
        备用方案，适用于非 Tendis 数据源或不支持 INCRSYNC 协议的场景。
      </el-alert>
      
      <div class="incr-stats-grid">
        <div class="stat-item">
          <span class="stat-value highlight">{{ formatNumber(task.incr_keys_synced || 0) }}</span>
          <span class="stat-label">已同步 Key</span>
        </div>
        <div class="stat-item">
          <span class="stat-value warning">{{ formatNumber(task.incr_keys_skipped || 0) }}</span>
          <span class="stat-label">冲突跳过</span>
        </div>
        <div class="stat-item">
          <span class="stat-value error">{{ formatNumber(task.incr_keys_failed || 0) }}</span>
          <span class="stat-label">同步失败</span>
        </div>
        <div class="stat-item">
          <span class="stat-value filtered">{{ formatNumber(task.incr_keys_filtered || 0) }}</span>
          <span class="stat-label">被过滤</span>
        </div>
      </div>
      
      <!-- Binlog 模式特有指标 -->
      <div class="binlog-stats" v-if="incrSyncMode === 'binlog'">
        <div class="binlog-stats-row">
          <div class="binlog-item">
            <span class="label">Binlog 位置</span>
            <span class="value mono">{{ task.incr_binlog_pos || '-' }}</span>
          </div>
          <div class="binlog-item">
            <span class="label">同步延迟</span>
            <span class="value" :class="{ 'lag-warning': (task.incr_lag_ms || 0) > 1000 }">
              {{ task.incr_lag_ms ? task.incr_lag_ms + ' ms' : '-' }}
            </span>
          </div>
          <div class="binlog-item">
            <span class="label">心跳数</span>
            <span class="value">{{ formatNumber(task.incr_heartbeats || 0) }}</span>
          </div>
          <div class="binlog-item">
            <span class="label">重连次数</span>
            <span class="value" :class="{ 'warning': (task.incr_reconnects || 0) > 5 }">
              {{ task.incr_reconnects || 0 }}
            </span>
          </div>
        </div>
      </div>
    </div>
    
    <!-- 校验结果 -->
    <div class="verify-section card" v-if="verifyResults.length || verifying">
      <h3><el-icon><Checked /></el-icon> 校验结果</h3>
      
      <!-- 校验进行中状态 -->
      <div v-if="verifying && computedVerifyResults.length === 0" class="verify-loading">
        <el-icon class="is-loading"><Loading /></el-icon>
        <span>正在进行数据校验，请稍候...</span>
      </div>
      
      <el-table :data="computedVerifyResults" style="width: 100%">
        <el-table-column label="校验名称" min-width="180">
          <template #default="{ row }">
            <router-link :to="`/verify`" class="verify-link">
              {{ row.name }}
            </router-link>
          </template>
        </el-table-column>
        <el-table-column label="状态" width="100">
          <template #default="{ row }">
            <el-tag :type="row.status === 'completed' ? 'success' : row.status === 'failed' ? 'danger' : row.status === 'running' ? '' : 'info'" size="small">
              {{ row.status === 'running' ? `${row.progress?.toFixed(0) || 0}%` : row.status }}
            </el-tag>
          </template>
        </el-table-column>
        <el-table-column label="模式" width="90">
          <template #default="{ row }">
            <span>{{ row.verify_mode === 'full' ? '全量' : row.verify_mode === 'sample' ? '采样' : row.verify_mode }}</span>
          </template>
        </el-table-column>
        <el-table-column label="已检查Key" width="120">
          <template #default="{ row }">
            {{ formatNumber(row.sampled_keys) }}
          </template>
        </el-table-column>
        <el-table-column label="匹配数" width="100">
          <template #default="{ row }">
            {{ formatNumber(row.matched_keys) }}
          </template>
        </el-table-column>
        <el-table-column label="值不一致" width="100">
          <template #default="{ row }">
            <span :class="{ 'error-text': row.value_mismatch > 0 }">
              {{ row.value_mismatch }}
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
            <span v-if="row.consistency_rate < 0" class="consistency-rate">
              <el-icon class="is-loading"><Loading /></el-icon> 校验中...
            </span>
            <span v-else :class="['consistency-rate', { high: row.consistency_rate >= 99.9 }]">
              {{ row.consistency_rate?.toFixed(2) || 0 }}%
            </span>
          </template>
        </el-table-column>
      </el-table>
    </div>
    
    <!-- 异常/跳过Key统计（始终显示，无论任务状态） -->
    <div class="error-keys-section card">
      <div class="section-header">
        <h3><el-icon><Warning /></el-icon> 异常/跳过Key统计</h3>
        <div class="header-actions">
          <el-button 
            v-if="errorKeys.failed > 0 && canManualRetry && !retryingKeys"
            type="warning" 
            size="small" 
            @click="retryFailedKeys"
          >
            <el-icon><RefreshRight /></el-icon> 
            重试失败Key ({{ errorKeys.failed }})
          </el-button>
          <el-button
            v-else-if="retryingKeys && retryProgressData"
            type="warning"
            size="small"
            disabled
          >
            <el-icon class="is-loading"><Loading /></el-icon>
            正在重试 {{ retryProgressData.current }}/{{ retryProgressData.total }}
          </el-button>
          <el-button
            v-else-if="retryingKeys"
            type="warning"
            size="small"
            loading
          >
            正在重试...
          </el-button>
          <el-tooltip 
            v-else-if="errorKeys.failed > 0 && !canManualRetry"
            content="运行中会自动重试，暂停任务后可手动重试"
            placement="top"
          >
            <el-button 
              type="info" 
              size="small" 
              disabled
            >
              <el-icon><RefreshRight /></el-icon> 
              重试失败Key ({{ errorKeys.failed }})
            </el-button>
          </el-tooltip>
          <el-button 
            type="primary" 
            size="small" 
            @click="downloadErrorKeys"
            :disabled="errorKeys.total === 0 && errorKeysList.length === 0"
          >
            <el-icon><Download /></el-icon> 下载详情
          </el-button>
        </div>
      </div>
      
      <!-- 重试进度条 -->
      <div class="retry-progress-bar" v-if="retryingKeys && retryProgressData">
        <div class="retry-progress-info">
          <span class="retry-label">重试进度</span>
          <span class="retry-stats">
            成功 <span class="success-text">{{ retryProgressData.success }}</span> / 
            失败 <span class="fail-text">{{ retryProgressData.failed }}</span> / 
            总计 {{ retryProgressData.total }}
          </span>
        </div>
        <el-progress 
          :percentage="Math.round(retryProgressData.percentage || 0)" 
          :stroke-width="16"
          :color="retryProgressColor"
          :format="(p) => `${p}%`"
        />
      </div>
      
      <div class="error-stats">
        <div 
          class="stat-item clickable" 
          :class="{ active: errorFilter === 'failed' }"
          @click="toggleErrorFilter('failed')"
        >
          <span class="stat-value error">{{ errorKeys.failed || 0 }}</span>
          <span class="stat-label">迁移失败</span>
          <span class="click-hint" v-if="errorKeys.failed > 0 && errorKeys.failed <= 1000">点击查看</span>
        </div>
        <div 
          class="stat-item clickable" 
          :class="{ active: errorFilter === 'skipped' }"
          @click="toggleErrorFilter('skipped')"
        >
          <span class="stat-value warning">{{ errorKeys.skipped || 0 }}</span>
          <span class="stat-label">冲突跳过</span>
          <span class="click-hint" v-if="errorKeys.skipped > 0 && errorKeys.skipped <= 1000">点击查看</span>
        </div>
        <div 
          class="stat-item clickable" 
          :class="{ active: errorFilter === 'large_key' }"
          @click="toggleErrorFilter('large_key')"
        >
          <span class="stat-value info">{{ errorKeys.large_keys || 0 }}</span>
          <span class="stat-label">大Key处理</span>
          <span class="click-hint" v-if="errorKeys.large_keys > 0 && errorKeys.large_keys <= 1000">点击查看</span>
        </div>
        <div class="stat-item">
          <span class="stat-value">{{ errorKeys.total || 0 }}</span>
          <span class="stat-label">异常总数</span>
        </div>
      </div>
      
      <!-- 异常Key列表 -->
      <div class="error-keys-list" v-if="filteredErrorKeysList.length > 0">
        <div class="filter-info" v-if="errorFilter">
          <el-tag closable @close="toggleErrorFilter(errorFilter)">
            筛选: {{ getErrorFilterText(errorFilter) }}
          </el-tag>
          <span class="filter-count">共 {{ errorKeysFilteredTotal }} 条</span>
        </div>
        <el-table :data="filteredErrorKeysList" style="width: 100%" max-height="400" :show-overflow-tooltip="false">
          <el-table-column label="Key名称" width="150">
            <template #default="{ row }">
              <el-tooltip :content="row.key" placement="top" :show-after="500">
                <span class="mono key-name">{{ row.key }}</span>
              </el-tooltip>
            </template>
          </el-table-column>
          <el-table-column label="类型" width="85" prop="type" />
          <el-table-column label="原因" width="90">
            <template #default="{ row }">
              <el-tag :type="getErrorTagType(row.reason)" size="small">
                {{ getErrorReasonText(row.reason) }}
              </el-tag>
            </template>
          </el-table-column>
          <el-table-column label="详情" min-width="280">
            <template #default="{ row }">
              <el-tooltip :content="row.detail" placement="top" :show-after="300">
                <span class="error-detail">{{ row.detail }}</span>
              </el-tooltip>
            </template>
          </el-table-column>
        </el-table>
        
        <div class="error-keys-pagination" v-if="errorKeysFilteredTotal > errorKeysPageSize">
          <el-pagination
            v-model:current-page="errorKeysPage"
            v-model:page-size="errorKeysPageSize"
            :total="errorKeysFilteredTotal"
            :page-sizes="[20, 50, 100, 200]"
            layout="total, sizes, prev, pager, next"
            small
            @size-change="handleErrorKeysPageSizeChange"
            @current-change="handleErrorKeysPageChange"
          />
        </div>
        <div class="truncated-tip" v-if="errorKeysTruncated">
          <el-alert
            :title="`页面最多展示 1000 条，实际共 ${errorKeysActualTotal} 条。查看完整数据请点击「下载全部」导出 CSV。`"
            type="warning"
            :closable="false"
            show-icon
          />
        </div>
        <div class="list-footer" v-if="errorKeysFilteredTotal > 0">
          <span>第 {{ (errorKeysPage - 1) * errorKeysPageSize + 1 }}-{{ Math.min(errorKeysPage * errorKeysPageSize, errorKeysFilteredTotal) }} 条，共 {{ errorKeysTruncated ? `${errorKeysActualTotal}（仅展示1000）` : errorKeysFilteredTotal }} 条</span>
          <el-button type="primary" size="small" @click="downloadErrorKeys">下载全部</el-button>
        </div>
      </div>
      
      <div class="no-errors" v-else-if="errorKeys.total === 0 && errorKeysList.length === 0">
        <el-icon><SuccessFilled /></el-icon>
        <span>暂无异常Key</span>
      </div>
      
      <div class="no-errors filter-empty" v-else-if="errorFilter && filteredErrorKeysList.length === 0">
        <el-icon><InfoFilled /></el-icon>
        <span>当前筛选条件无匹配数据</span>
        <el-button text type="primary" @click="toggleErrorFilter(errorFilter)">清除筛选</el-button>
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
          :class="['log-entry', log.level?.toLowerCase(), { 'expanded': expandedLogIds.has(log.id) }]"
          @click="toggleLogExpand(log.id)"
          :title="!expandedLogIds.has(log.id) ? '点击展开完整日志' : '点击收起'"
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
          :max="1024"
          style="width: 100%"
        />
        <div class="form-tip">系统上限1024，增加Worker可提高速度但会增加负载</div>
      </el-form-item>
      
      <el-form-item label="扫描批次大小">
        <el-input-number 
          v-model="configForm.scan_batch_size" 
          :min="100" 
          :max="100000"
          :step="1000"
          style="width: 100%"
        />
        <div class="form-tip">系统上限100000，推荐1000-10000</div>
      </el-form-item>
      
      <el-form-item label="源端QPS限制">
        <el-input-number 
          v-model="configForm.source_qps" 
          :min="0" 
          :max="100000"
          :step="1000"
          style="width: 100%"
        />
        <div class="form-tip">0 表示不限制，系统上限100000</div>
      </el-form-item>
      
      <el-form-item label="目标端QPS限制">
        <el-input-number 
          v-model="configForm.target_qps" 
          :min="0" 
          :max="100000"
          :step="1000"
          style="width: 100%"
        />
        <div class="form-tip">0 表示不限制，系统上限100000</div>
      </el-form-item>
    </el-form>
    
    <template #footer>
      <el-button @click="configDialogVisible = false">取消</el-button>
      <el-button type="primary" @click="applyConfig" :loading="applyingConfig">
        应用参数
      </el-button>
    </template>
  </el-dialog>
  
  <!-- 自动恢复设置对话框 -->
  <el-dialog v-model="showAutoRecoveryDialog" title="自动恢复设置" width="500px">
    <el-alert type="info" :closable="false" style="margin-bottom: 20px">
      <template #title>
        <strong>自动恢复功能说明</strong>
      </template>
      当任务因网络中断等原因暂停时，系统会定期检测集群健康状态，一旦恢复正常会自动继续任务。
    </el-alert>
    
    <!-- 当前状态 -->
    <div class="auto-recovery-status" v-if="autoRecoveryStatus">
      <div class="status-row">
        <span class="label">源端健康:</span>
        <el-tag :type="autoRecoveryStatus.source_healthy ? 'success' : 'danger'" size="small">
          {{ autoRecoveryStatus.source_healthy ? '正常' : '异常' }}
        </el-tag>
      </div>
      <div class="status-row">
        <span class="label">目标端健康:</span>
        <el-tag :type="autoRecoveryStatus.target_healthy ? 'success' : 'danger'" size="small">
          {{ autoRecoveryStatus.target_healthy ? '正常' : '异常' }}
        </el-tag>
      </div>
      <div class="status-row" v-if="autoRecoveryStatus.pause_reason">
        <span class="label">暂停原因:</span>
        <span class="value">{{ autoRecoveryStatus.pause_reason }}</span>
      </div>
      <div class="status-row" v-if="autoRecoveryStatus.resume_attempts > 0">
        <span class="label">恢复尝试:</span>
        <span class="value">{{ autoRecoveryStatus.resume_attempts }} 次</span>
      </div>
    </div>
    
    <el-divider />
    
    <el-form :model="autoRecoveryForm" label-width="140px">
      <el-form-item label="启用自动恢复">
        <el-switch v-model="autoRecoveryForm.enabled" />
      </el-form-item>
      
      <el-form-item label="健康检测间隔" v-if="autoRecoveryForm.enabled">
        <el-input-number 
          v-model="autoRecoveryForm.healthCheckIntervalSec" 
          :min="10" 
          :max="300"
          style="width: 100%"
        />
        <div class="form-tip">单位：秒，推荐 30 秒</div>
      </el-form-item>
      
      <el-form-item label="最大恢复尝试" v-if="autoRecoveryForm.enabled">
        <el-input-number 
          v-model="autoRecoveryForm.maxAutoResumeAttempts" 
          :min="1" 
          :max="100"
          style="width: 100%"
        />
        <div class="form-tip">超过此次数后需手动恢复</div>
      </el-form-item>
    </el-form>
    
    <template #footer>
      <el-button @click="showAutoRecoveryDialog = false">取消</el-button>
      <el-button type="primary" @click="toggleAutoRecovery">
        保存设置
      </el-button>
    </template>
  </el-dialog>
</template>

<script setup>
import { ref, reactive, computed, onMounted, onUnmounted, watch } from 'vue'
import { useRoute } from 'vue-router'
import { ElMessage } from 'element-plus'
import api from '@/api'
import wsService from '@/api/websocket'
import dayjs from 'dayjs'

const route = useRoute()
const taskId = computed(() => route.params.id)

const task = ref(null)
const progress = ref(null)
const verifyResults = ref([])

// 计算属性：是否可以手动重试失败Key
// 只有在增量同步阶段或任务完成/失败后才能手动重试
const canManualRetry = computed(() => {
  if (!task.value) return false
  const status = task.value.status
  // 允许手动重试的状态：增量同步、已完成、失败、已暂停
  // 正在重试中(retrying)不允许再次点击
  return ['incremental', 'completed', 'failed', 'paused'].includes(status)
})

// 重试进度条颜色：根据失败比例显示不同颜色
const retryProgressColor = computed(() => {
  if (!retryProgressData.value) return '#409eff'
  const { success, failed, current } = retryProgressData.value
  if (current === 0) return '#409eff'
  const failRate = failed / current
  if (failRate > 0.5) return '#f56c6c'  // 红色：失败率>50%
  if (failRate > 0.1) return '#e6a23c'  // 橙色：失败率>10%
  return '#67c23a'                        // 绿色：大部分成功
})

// 计算属性：将 VerifyTask 结构映射为表格展示数据
const computedVerifyResults = computed(() => {
  return verifyResults.value.map(vt => {
    const r = vt.result || {}
    const sampled = r.sampled_keys || r.scanned_keys || 0
    const matched = r.matched_keys || 0
    return {
      id: vt.id,
      name: vt.name,
      status: vt.status,
      verify_mode: vt.verify_mode,
      compare_mode: vt.compare_mode,
      created_at: vt.created_at,
      sampled_keys: sampled,
      matched_keys: matched,
      missing_keys: r.missing_keys || 0,
      value_mismatch: r.value_mismatch || 0,
      extra_keys: r.extra_keys || r.target_extra_keys || 0,
      consistency_rate: sampled > 0 ? (matched / sampled) * 100 : (vt.status === 'running' ? -1 : 0),
      progress: r.progress || 0,
      current_speed: r.current_speed || 0,
    }
  })
})
const taskLogs = ref([])
const logLevel = ref('')
const logsContainer = ref(null)
const expandedLogIds = ref(new Set()) // 展开的日志 ID 集合
const refreshInterval = ref(0) // 默认使用 WebSocket，0 表示不轮询
const refreshing = ref(false)
const errorKeys = ref({ total: 0, failed: 0, skipped: 0, large_keys: 0 })
const errorKeysList = ref([])
const errorFilter = ref('')  // 错误类型筛选
const errorKeysPage = ref(1) // 当前页
const errorKeysPageSize = ref(50) // 每页条数
const errorKeysFilteredTotal = ref(0) // 筛选后总数（最多1000）
const errorKeysTruncated = ref(false) // 是否超过1000条被截断
const errorKeysActualTotal = ref(0)   // 实际总数（可能超过1000）
const retryingKeys = ref(false)
const shadowStats = ref(null)
const verifying = ref(false)  // 【修复】校验进行中状态

// 依赖校验相关
const preflightChecks = ref([])
const preflightLoading = ref(false)
const preflightResult = ref(null) // { all_passed, can_start }
const preflightChecked = ref(false) // 是否已执行过校验
let refreshTimer = null
let elapsedTimer = null  // 已耗时间定时器
let verifyCheckTimer = null  // 校验状态检查定时器

// 已耗时间实时显示
const elapsedTimeDisplay = ref('-')

// 自动恢复相关
const showAutoRecoveryDialog = ref(false)
const autoRecoveryStatus = ref(null)
const autoRecoveryForm = reactive({
  enabled: false,
  healthCheckIntervalSec: 30,
  maxAutoResumeAttempts: 10
})

// WebSocket 相关状态
const wsConnected = ref(false)
const wsRealtimeEnabled = ref(true)
let wsUnsubscribers = []

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
  // 后端直接返回对象，不需要 JSON.parse
  if (task.value?.config && typeof task.value.config === 'object') {
    return task.value.config
  }
  // 兼容旧格式：尝试解析 JSON 字符串
  try {
    return JSON.parse(task.value?.config || '{}')
  } catch {
    return {}
  }
})

// 判断任务是否正在运行中
const isTaskRunning = computed(() => {
  if (!task.value) return false
  const status = task.value.status
  return status === 'running' || status === 'migrating' || status === 'paused'
})

// 判断是否处于"估算中"状态：任务正在运行，但 total_keys 为 0
// 此时进度百分比无意义，应显示"估算中"而非 0%
const isEstimating = computed(() => {
  if (!isTaskRunning.value) return false
  const totalKeys = progress.value?.total_keys || 0
  const migratedKeys = progress.value?.migrated_keys || 0
  // total_keys 为 0 但已经有迁移数据，说明 DBSIZE 获取失败
  return totalKeys === 0 && migratedKeys > 0
})

// 判断是否为影子模式
const isShadowMode = computed(() => {
  return task.value?.options?.shadow_mode === true || config.value?.shadow_mode === true
})

// 判断是否显示增量同步统计
const showIncrementalStats = computed(() => {
  // 【修复】如果是 full_only 模式，不显示增量同步面板
  const migrationMode = task.value?.options?.migration_mode || task.value?.migration_mode
  if (migrationMode === 'full_only') {
    return false
  }
  
  // 在增量阶段或有增量数据时显示
  const isIncrPhase = progress.value?.phase === 'incremental'
  const hasIncrData = (task.value?.incr_keys_synced || 0) > 0 || 
                      (task.value?.incr_keys_skipped || 0) > 0 ||
                      (task.value?.incr_keys_failed || 0) > 0 ||
                      (task.value?.incr_keys_filtered || 0) > 0
  // 检查迁移模式是否包含增量
  const hasIncrMode = migrationMode === 'full_and_incremental' ||
                      migrationMode === 'incremental_only'
  // 只要迁移模式包含增量就显示（不限制任务状态）
  return isIncrPhase || hasIncrData || hasIncrMode
})

// 获取增量同步模式
const incrSyncMode = computed(() => {
  return task.value?.incr_sync_mode || 'binlog'
})

// 增量同步状态文本
const incrStatusText = computed(() => {
  if (progress.value?.phase === 'incremental') {
    return '同步中'
  }
  // 如果有增量数据，说明曾经运行过，现在是"已停止"
  const hasIncrData = (task.value?.incr_keys_synced || 0) > 0 || 
                      (task.value?.incr_keys_skipped || 0) > 0 ||
                      (task.value?.incr_keys_failed || 0) > 0
  if (hasIncrData) {
    return '已停止'
  }
  // 没有增量数据，还在全量阶段或尚未开始
  return '未开始'
})

const fetchTask = async () => {
  try {
    refreshing.value = true
    const data = await api.getTask(taskId.value)
    task.value = data
    progress.value = data.progress
    
    // 如果是影子模式，获取影子统计
    if (data?.options?.shadow_mode) {
      fetchShadowStats()
    }
  } catch (err) {
    console.error('Fetch task failed:', err)
  } finally {
    refreshing.value = false
  }
}

// 获取影子模式统计
const fetchShadowStats = async () => {
  try {
    const data = await api.getShadowStats(taskId.value)
    shadowStats.value = data
  } catch (err) {
    console.error('Fetch shadow stats failed:', err)
    shadowStats.value = null
  }
}

// 更多操作处理
const handleMoreActions = async (command) => {
  switch (command) {
    case 'export-report-csv':
      downloadReport('csv')
      break
    case 'export-report-json':
      downloadReport('json')
      break
    case 'export-config':
      exportConfig()
      break
    case 'view-health':
      viewTaskHealth()
      break
    case 'toggle-auto-recovery':
      showAutoRecoveryDialog.value = true
      fetchAutoRecoveryStatus()
      break
    case 'retry-failed':
      retryFailedKeys()
      break
  }
}

// 下载报告
const downloadReport = async (format) => {
  try {
    const blob = await api.downloadTaskReport(taskId.value, format)
    const url = window.URL.createObjectURL(blob)
    const a = document.createElement('a')
    a.href = url
    a.download = `task-report-${taskId.value.substring(0, 8)}-${dayjs().format('YYYYMMDDHHmmss')}.${format}`
    document.body.appendChild(a)
    a.click()
    window.URL.revokeObjectURL(url)
    document.body.removeChild(a)
    ElMessage.success('报告下载成功')
  } catch (err) {
    ElMessage.error('下载失败: ' + (err.message || '未知错误'))
  }
}

// 导出配置
const exportConfig = async () => {
  try {
    const config = await api.exportTaskConfig(taskId.value)
    const blob = new Blob([JSON.stringify(config, null, 2)], { type: 'application/json' })
    const url = window.URL.createObjectURL(blob)
    const a = document.createElement('a')
    a.href = url
    a.download = `task-config-${taskId.value.substring(0, 8)}.json`
    document.body.appendChild(a)
    a.click()
    window.URL.revokeObjectURL(url)
    document.body.removeChild(a)
    ElMessage.success('配置导出成功')
  } catch (err) {
    ElMessage.error('导出失败: ' + (err.message || '未知错误'))
  }
}

// 查看任务健康状态
const viewTaskHealth = async () => {
  try {
    const health = await api.getTaskHealth(taskId.value)
    ElMessage.info(`任务健康状态: ${health.status || 'unknown'}`)
  } catch (err) {
    ElMessage.error('获取健康状态失败')
  }
}

// 获取自动恢复状态
const fetchAutoRecoveryStatus = async () => {
  try {
    const status = await api.getAutoRecoveryStatus(taskId.value)
    autoRecoveryStatus.value = status
    autoRecoveryForm.enabled = status?.auto_resume_enabled || false
    autoRecoveryForm.healthCheckIntervalSec = status?.health_check_interval_sec || 30
    autoRecoveryForm.maxAutoResumeAttempts = status?.max_auto_resume_attempts || 10
  } catch (err) {
    console.error('Fetch auto recovery status failed:', err)
    autoRecoveryStatus.value = null
  }
}

// 切换自动恢复设置
const toggleAutoRecovery = async () => {
  try {
    await api.toggleAutoRecovery(taskId.value, {
      enabled: autoRecoveryForm.enabled,
      health_check_interval_sec: autoRecoveryForm.healthCheckIntervalSec,
      max_auto_resume_attempts: autoRecoveryForm.maxAutoResumeAttempts
    })
    ElMessage.success(autoRecoveryForm.enabled ? '自动恢复已启用' : '自动恢复已禁用')
    showAutoRecoveryDialog.value = false
  } catch (err) {
    ElMessage.error('设置失败: ' + (err.message || '未知错误'))
  }
}

const changeRefreshInterval = () => {
  if (refreshTimer) {
    clearInterval(refreshTimer)
    refreshTimer = null
  }
  
  // 如果选择轮询模式，禁用 WebSocket 实时更新
  if (refreshInterval.value > 0) {
    wsRealtimeEnabled.value = false
    refreshTimer = setInterval(() => {
      // 任务数据仅在活跃状态刷新，日志始终刷新
      const activeStatus = ['running', 'incremental', 'sync_incremental', 'paused']
      if (task.value?.status && activeStatus.includes(task.value.status)) {
        fetchTask()
      }
      fetchTaskLogs()
    }, refreshInterval.value)
  } else {
    // 选择实时模式，启用 WebSocket
    wsRealtimeEnabled.value = true
    initWebSocket()
  }
}

// ========== WebSocket 相关函数 ==========

// 初始化 WebSocket 连接
const initWebSocket = () => {
  if (!wsRealtimeEnabled.value) return
  
  // 连接 WebSocket
  if (!wsService.isConnected) {
    wsService.connect()
  }
  
  // 设置连接状态回调
  wsService.onConnected = () => {
    wsConnected.value = true
    // 连接成功后订阅当前任务
    if (taskId.value) {
      wsService.subscribe(taskId.value)
    }
  }
  
  wsService.onDisconnected = () => {
    wsConnected.value = false
  }
  
  // 注册消息处理器
  const unsubMetrics = wsService.on('metrics', handleMetricsUpdate)
  const unsubLog = wsService.on('log', handleLogUpdate)
  const unsubStatus = wsService.on('status', handleStatusUpdate)
  const unsubProgress = wsService.on('progress', handleProgressUpdate)
  
  wsUnsubscribers = [unsubMetrics, unsubLog, unsubStatus, unsubProgress]
  
  // 如果已连接，直接订阅
  if (wsService.isConnected && taskId.value) {
    wsService.subscribe(taskId.value)
    wsConnected.value = true
  }
  
  // 【修复】WebSocket 模式下也启动日志轮询定时器（因为后端暂未实现 WS 日志推送）
  // 每 5 秒刷新一次日志，确保用户能看到最新日志
  if (!refreshTimer) {
    refreshTimer = setInterval(() => {
      // 日志始终刷新，不受任务状态限制
      fetchTaskLogs()
    }, 5000) // 5 秒轮询一次日志
  }
}

// 清理 WebSocket 连接
const cleanupWebSocket = () => {
  // 取消订阅
  if (taskId.value) {
    wsService.unsubscribe(taskId.value)
  }
  
  // 注销处理器
  wsUnsubscribers.forEach(unsub => unsub && unsub())
  wsUnsubscribers = []
  
  // 重置回调
  wsService.onConnected = null
  wsService.onDisconnected = null
}

// 处理 metrics 更新
const handleMetricsUpdate = ({ taskId: tid, payload }) => {
  if (tid !== taskId.value) return
  
  console.log('[WS] Metrics update:', payload)
  
  // 更新任务状态
  if (task.value && payload) {
    task.value.status = payload.status || task.value.status
    // 更新增量同步相关指标
    if (payload.incr_keys_synced !== undefined) {
      task.value.incr_keys_synced = payload.incr_keys_synced
    }
    if (payload.incr_keys_skipped !== undefined) {
      task.value.incr_keys_skipped = payload.incr_keys_skipped
    }
    if (payload.incr_keys_failed !== undefined) {
      task.value.incr_keys_failed = payload.incr_keys_failed
    }
    if (payload.incr_keys_filtered !== undefined) {
      task.value.incr_keys_filtered = payload.incr_keys_filtered
    }
    if (payload.incr_lag_ms !== undefined) {
      task.value.incr_lag_ms = payload.incr_lag_ms
    }
    // 更新过滤Key数
    if (payload.filtered_keys !== undefined) {
      task.value.keys_filtered = payload.filtered_keys
    }
    // 更新迁移模式
    if (payload.migration_mode && task.value.options) {
      task.value.options.migration_mode = payload.migration_mode
    }
  }
  
  // 更新进度信息
  if (progress.value && payload) {
    // 【修复】检测 phase 变化并提示用户
    const oldPhase = progress.value.phase
    const newPhase = payload.phase || progress.value.phase
    
    if (oldPhase !== newPhase) {
      console.log('[WS] Phase changed:', oldPhase, '->', newPhase)
      ElMessage.success(`迁移阶段已切换: ${getPhaseText(newPhase)}`)
      
      // 阶段切换时刷新完整任务数据
      fetchTask()
      fetchTaskLogs()
    }
    
    progress.value.phase = newPhase
    progress.value.percentage = payload.progress || progress.value.percentage
    progress.value.migrated_keys = payload.processed_keys || progress.value.migrated_keys
    progress.value.total_keys = payload.total_keys || progress.value.total_keys
    progress.value.current_speed = payload.current_qps || progress.value.current_speed
    
    // 【修复】更新待迁移Key数
    if (payload.keys_to_migrate !== undefined) {
      progress.value.keys_to_migrate = payload.keys_to_migrate
    }
    
    // 【修复】更新数据量（已迁移和总量）
    if (payload.bytes_written !== undefined) {
      progress.value.migrated_bytes = payload.bytes_written
    }
    if (payload.total_bytes !== undefined) {
      progress.value.total_bytes = payload.total_bytes
    }
    
    // 【修复】更新预计剩余时间
    if (payload.estimated_eta !== undefined) {
      progress.value.estimated_eta = payload.estimated_eta
    }
    
    // 【修复】更新已耗时间（来自后端计算）
    if (payload.elapsed_time !== undefined) {
      progress.value.elapsed_time = payload.elapsed_time
    }
  }
  
  // 更新异常Key统计
  if (payload) {
    if (payload.failed_keys !== undefined || payload.skipped_keys !== undefined) {
      // 【修复】异常总数 = 失败数 + 跳过数（移除重复的 conflict_keys）
      errorKeys.value = {
        total: (payload.failed_keys || 0) + (payload.skipped_keys || 0),
        failed: payload.failed_keys || 0,
        skipped: payload.skipped_keys || 0,
        large_keys: payload.bigkey_found || 0
      }
    }
  }
}

// 处理日志更新
const handleLogUpdate = ({ taskId: tid, payload }) => {
  if (tid !== taskId.value) return
  
  console.log('[WS] Log update:', payload)
  
  // 根据日志级别过滤
  if (logLevel.value && payload.level !== logLevel.value) {
    return
  }
  
  // 添加新日志到列表开头
  const newLog = {
    id: Date.now(),
    level: payload.level,
    message: payload.message,
    timestamp: payload.timestamp
  }
  
  taskLogs.value = [newLog, ...taskLogs.value.slice(0, 99)]
  
  // 自动滚动到顶部
  if (logsContainer.value) {
    logsContainer.value.scrollTop = 0
  }
}

// 处理状态更新
const handleStatusUpdate = ({ taskId: tid, payload }) => {
  if (tid !== taskId.value) return
  
  console.log('[WS] Status update:', payload)
  
  if (task.value && payload.status) {
    const oldStatus = task.value.status
    task.value.status = payload.status
    
    // 状态变化提示
    if (oldStatus !== payload.status) {
      const statusText = getStatusText(payload.status)
      ElMessage.info(`任务状态已更新: ${statusText}`)
      
      // 如果任务完成或失败，刷新完整数据
      if (payload.status === 'completed' || payload.status === 'failed') {
        fetchTask()
        fetchVerifyResults()
        fetchErrorKeys()
      }
    }
  }
}

// 处理重试进度更新（WebSocket progress 消息）
const retryProgressData = ref(null) // { current, total, success, failed, percentage }

const handleProgressUpdate = ({ taskId: tid, payload }) => {
  if (tid !== taskId.value) return
  
  if (payload?.retry_progress) {
    const rp = payload.retry_progress
    retryProgressData.value = rp
    retryingKeys.value = true
    retryingProgress.value = `正在重试 ${rp.current}/${rp.total}...`
  }
  
  if (payload?.retry_complete) {
    const result = payload.retry_result || {}
    retryProgressData.value = null
    retryingKeys.value = false
    retryingProgress.value = ''
    
    if (result.failed === 0) {
      ElMessage.success(`全部 ${result.total} 个Key重试成功！`)
    } else {
      ElMessage.warning(`重试完成: 成功 ${result.success}, 失败 ${result.failed}`)
    }
    
    // 刷新数据
    fetchTask()
    fetchErrorKeys()
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
    const params = {
      page: errorKeysPage.value,
      page_size: errorKeysPageSize.value,
    }
    if (errorFilter.value) {
      params.filter = errorFilter.value
    }
    const result = await api.getErrorKeys(taskId.value, params)
    errorKeys.value = result?.stats || { total: 0, failed: 0, skipped: 0, large_keys: 0 }
    errorKeysList.value = result?.items || []
    errorKeysFilteredTotal.value = result?.filtered_total || 0
    errorKeysTruncated.value = result?.truncated || false
    errorKeysActualTotal.value = result?.actual_total || result?.filtered_total || 0
  } catch (err) {
    // 忽略错误
    errorKeys.value = { total: 0, failed: 0, skipped: 0, large_keys: 0 }
    errorKeysList.value = []
    errorKeysFilteredTotal.value = 0
    errorKeysTruncated.value = false
    errorKeysActualTotal.value = 0
  }
}

const downloadErrorKeys = async () => {
  try {
    const res = await api.downloadErrorKeys(taskId.value)
    const blob = res.data
    const contentType = res.headers['content-type'] || ''
    const contentDisposition = res.headers['content-disposition'] || ''
    
    // 从 Content-Disposition 提取文件名，或根据 Content-Type 自动判断
    let fileName = ''
    const match = contentDisposition.match(/filename="?([^";\n]+)"?/)
    if (match) {
      fileName = match[1]
    } else if (contentType.includes('application/zip')) {
      fileName = `error-keys-${taskId.value.substring(0, 8)}-${dayjs().format('YYYYMMDDHHmmss')}.zip`
    } else {
      fileName = `error-keys-${taskId.value.substring(0, 8)}-${dayjs().format('YYYYMMDDHHmmss')}.csv`
    }
    
    const url = window.URL.createObjectURL(blob)
    const a = document.createElement('a')
    a.href = url
    a.download = fileName
    document.body.appendChild(a)
    a.click()
    window.URL.revokeObjectURL(url)
    document.body.removeChild(a)
    
    if (contentType.includes('application/zip')) {
      ElMessage.success('下载成功（数据量较大，已分多个CSV文件打包为ZIP）')
    } else {
      ElMessage.success('下载成功')
    }
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

// 筛选和分页都在服务端完成，直接使用 errorKeysList
const filteredErrorKeysList = computed(() => errorKeysList.value)

// 切换错误筛选（重置分页并重新请求）
const toggleErrorFilter = (filter) => {
  if (errorFilter.value === filter) {
    errorFilter.value = ''
  } else {
    errorFilter.value = filter
  }
  errorKeysPage.value = 1
  fetchErrorKeys()
}

// error-keys 分页变化
const handleErrorKeysPageChange = () => {
  fetchErrorKeys()
}

const handleErrorKeysPageSizeChange = () => {
  errorKeysPage.value = 1
  fetchErrorKeys()
}

// 依赖校验
const runPreflightCheck = async () => {
  preflightLoading.value = true
  preflightChecked.value = true
  try {
    const result = await api.preflightCheck(taskId.value)
    preflightChecks.value = result?.checks || []
    preflightResult.value = {
      all_passed: result?.all_passed || false,
      can_start: result?.can_start || false,
    }
  } catch (err) {
    ElMessage.error('依赖校验失败: ' + (err.message || '未知错误'))
    preflightChecks.value = []
    preflightResult.value = null
  } finally {
    preflightLoading.value = false
  }
}

const getCheckStatusIcon = (status) => {
  const map = { passed: 'SuccessFilled', failed: 'CircleCloseFilled', warning: 'WarningFilled' }
  return map[status] || 'InfoFilled'
}

const getCheckStatusColor = (status) => {
  const map = { passed: '#67c23a', failed: '#f56c6c', warning: '#e6a23c' }
  return map[status] || '#909399'
}

// 获取筛选文本
const getErrorFilterText = (filter) => {
  const map = {
    failed: '迁移失败',
    skipped: '冲突跳过',
    large_key: '大Key处理'
  }
  return map[filter] || filter
}

// 重试失败的Key
const retryingProgress = ref('')
let retryCheckInterval = null // 将 interval 提升到外层，方便清理

const retryFailedKeys = async () => {
  if (errorKeys.value.failed === 0) {
    ElMessage.warning('没有失败的Key需要重试')
    return
  }
  
  const failedCount = errorKeys.value.failed
  retryingKeys.value = true
  retryingProgress.value = `正在重试 ${failedCount} 个失败的Key...`
  
  try {
    const result = await api.retryFailedKeys(taskId.value)
    const keysToRetry = result?.keys_to_retry || failedCount
    const totalFailed = result?.total_failed || failedCount
    const workerCount = result?.worker_count || 4
    
    // 显示开始重试的提示（包含 worker 数量）
    ElMessage({
      message: `正在并行重试 ${keysToRetry} 个失败的Key（${workerCount} 个 worker）`,
      type: 'info',
      duration: 5000
    })
    
    retryingProgress.value = `正在重试 0/${keysToRetry}...`
    
    // 【修复】重试完全在后端执行，前端只负责定时刷新显示进度
    // 即使离开页面，后端重试也不会停止
    // 延长检查时间，最多检查 5 分钟（150 次 * 2 秒）
    let retryCheckCount = 0
    const maxChecks = 150
    
    // 清理旧的 interval
    if (retryCheckInterval) {
      clearInterval(retryCheckInterval)
    }
    
    retryCheckInterval = setInterval(async () => {
      retryCheckCount++
      
      // 刷新任务状态和错误 Key 列表
      await fetchTask()
      await fetchErrorKeys()
      
      const newFailedCount = errorKeys.value.failed
      const newStatus = task.value?.status
      
      // 更新进度显示
      if (newFailedCount < failedCount) {
        const successCount = failedCount - newFailedCount
        retryingProgress.value = `已成功重试 ${successCount}/${failedCount} 个Key`
      }
      
      // 如果任务状态不再是 retrying，说明重试已完成
      if (newStatus !== 'retrying') {
        clearInterval(retryCheckInterval)
        retryCheckInterval = null
        retryingKeys.value = false
        retryingProgress.value = ''
        
        if (newFailedCount === 0) {
          ElMessage.success(`全部 ${failedCount} 个Key重试成功！`)
        } else if (newFailedCount < failedCount) {
          const successCount = failedCount - newFailedCount
          ElMessage({
            message: `重试完成：${successCount} 个成功，${newFailedCount} 个仍然失败`,
            type: 'warning',
            duration: 5000
          })
        } else {
          ElMessage.info('重试已完成，请查看最新结果')
        }
        return
      }
      
      // 检查次数达到上限
      if (retryCheckCount >= maxChecks) {
        clearInterval(retryCheckInterval)
        retryCheckInterval = null
        retryingKeys.value = false
        retryingProgress.value = ''
        ElMessage.info('重试仍在后台进行中，请稍后刷新页面查看结果')
      }
    }, 2000)
    
  } catch (err) {
    ElMessage.error('重试失败: ' + (err.message || '未知错误'))
    retryingKeys.value = false
    retryingProgress.value = ''
  }
}

// 清理重试检查 interval
const clearRetryCheckInterval = () => {
  if (retryCheckInterval) {
    clearInterval(retryCheckInterval)
    retryCheckInterval = null
  }
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

// 切换日志展开/收起状态
const toggleLogExpand = (logId) => {
  if (expandedLogIds.value.has(logId)) {
    expandedLogIds.value.delete(logId)
  } else {
    expandedLogIds.value.add(logId)
  }
  // 触发响应式更新
  expandedLogIds.value = new Set(expandedLogIds.value)
}

// 异常Key列表时间格式（不需要毫秒）
const formatErrorTime = (timestamp) => {
  return dayjs(timestamp).format('HH:mm:ss')
}

const formatLogFields = (fields) => {
  if (!fields || typeof fields !== 'object') return ''
  const entries = Object.entries(fields)
  if (entries.length === 0) return ''
  return entries.map(([k, v]) => `${k}=${JSON.stringify(v)}`).join(' ')
}

const startTask = async () => {
  // 如果已执行过校验且有阻断项，不允许启动
  if (preflightChecked.value && preflightResult.value && !preflightResult.value.can_start) {
    ElMessage.error('存在必须通过的校验项未通过，请先解决后再启动')
    return
  }
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
    verifying.value = true
    const resp = await api.triggerVerify(taskId.value)
    const verifyTaskId = resp?.verify_task_id
    ElMessage.success('校验任务已创建并启动')
    
    // 定时检查校验结果（从独立校验任务系统获取）
    if (verifyCheckTimer) {
      clearInterval(verifyCheckTimer)
    }
    verifyCheckTimer = setInterval(async () => {
      try {
        const result = await api.getVerifyResults(taskId.value)
        if (result && result.length > 0) {
          verifyResults.value = result
          // 检查最新的校验任务是否完成
          const latest = result[0] // 按创建时间倒序，第一个是最新的
          if (latest.status === 'completed' || latest.status === 'failed') {
            verifying.value = false
            clearInterval(verifyCheckTimer)
            verifyCheckTimer = null
            ElMessage.success(latest.status === 'completed' ? '校验完成' : '校验失败')
          }
        }
      } catch (e) {
        // 忽略错误
      }
    }, 3000)  // 每3秒检查一次
    
    // 10分钟超时自动停止检查（全量校验可能耗时较长）
    setTimeout(() => {
      if (verifyCheckTimer) {
        clearInterval(verifyCheckTimer)
        verifyCheckTimer = null
        verifying.value = false
        fetchVerifyResults()
      }
    }, 600000)
  } catch (err) {
    verifying.value = false
    ElMessage.error('触发校验失败')
  }
}

// 停止任务（彻底停止，任务不可恢复）
const stopTask = async () => {
  try {
    await api.stopTask(taskId.value)
    ElMessage.success('任务已停止')
    fetchTask()
  } catch (err) {
    ElMessage.error('停止失败: ' + (err.message || '未知错误'))
  }
}

// 停止任务（停止增量同步并标记完成）
const completeTask = async () => {
  try {
    await api.completeTask(taskId.value, false)
    ElMessage.success('任务已停止并完成')
    fetchTask()
    fetchVerifyResults()
  } catch (err) {
    ElMessage.error('停止任务失败: ' + (err.message || '未知错误'))
  }
}

// 标记任务完成（跳过校验）
const markComplete = async () => {
  try {
    await api.completeTask(taskId.value, true)
    ElMessage.success('任务已标记完成')
    fetchTask()
  } catch (err) {
    ElMessage.error('标记完成失败: ' + (err.message || '未知错误'))
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
    failed: '失败',
    incremental: '增量同步',
    incremental_stopped: '增量已停止',
    retrying: '重试中'
  }
  return map[status] || status
}

const getMigrationModeText = (mode) => {
  const map = {
    full: '仅全量',
    full_only: '仅全量',
    full_and_incremental: '全量+增量',
    incremental: '仅增量'
  }
  return map[mode] || mode || '全量+增量'
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

const getFilterModeText = (mode) => {
  const map = {
    prefix: '前缀匹配',
    pattern: '模式匹配',
    all: '全部迁移'
  }
  return map[mode] || mode || '全部迁移'
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
  // 精确显示，不使用 K/M/B 缩写，添加千分位分隔符
  if (num === null || num === undefined) return '0'
  return num.toLocaleString('zh-CN')
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

// 计算并更新已耗时间（每秒调用一次）
const updateElapsedTime = () => {
  if (!task.value?.started_at) {
    elapsedTimeDisplay.value = '-'
    return
  }
  
  const start = dayjs(task.value.started_at)
  // 如果任务已完成，用完成时间；否则用当前时间
  const end = task.value.completed_at ? dayjs(task.value.completed_at) : dayjs()
  const diff = end.diff(start, 'second')
  
  const hours = Math.floor(diff / 3600)
  const minutes = Math.floor((diff % 3600) / 60)
  const seconds = diff % 60
  
  if (hours > 0) {
    elapsedTimeDisplay.value = `${hours}h ${minutes}m ${seconds}s`
  } else if (minutes > 0) {
    elapsedTimeDisplay.value = `${minutes}m ${seconds}s`
  } else {
    elapsedTimeDisplay.value = `${seconds}s`
  }
}

// 启动已耗时间定时器
const startElapsedTimer = () => {
  // 先立即更新一次
  updateElapsedTime()
  
  // 清理旧定时器
  if (elapsedTimer) {
    clearInterval(elapsedTimer)
  }
  
  // 每秒更新一次
  elapsedTimer = setInterval(updateElapsedTime, 1000)
}

// 停止已耗时间定时器
const stopElapsedTimer = () => {
  if (elapsedTimer) {
    clearInterval(elapsedTimer)
    elapsedTimer = null
  }
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
  
  // 启动已耗时间定时器（每秒更新）
  startElapsedTimer()
  
  // 默认使用 WebSocket 实时更新
  if (wsRealtimeEnabled.value) {
    initWebSocket()
  } else if (refreshInterval.value > 0) {
    // 使用轮询模式
    refreshTimer = setInterval(() => {
      // 任务数据和错误Key仅在活跃状态刷新，日志始终刷新
      const activeStatus = ['running', 'incremental', 'sync_incremental', 'paused', 'retrying']
      if (task.value?.status && activeStatus.includes(task.value.status)) {
        fetchTask()
        fetchErrorKeys()
      }
      fetchTaskLogs()
    }, refreshInterval.value)
  }
  
  // 【修复】如果页面加载时任务正在重试，恢复重试进度显示
  setTimeout(() => {
    if (task.value?.status === 'retrying') {
      retryingKeys.value = true
      retryingProgress.value = '重试进行中（后台执行）...'
      
      // 启动定时检查
      let retryCheckCount = 0
      const maxChecks = 150
      
      retryCheckInterval = setInterval(async () => {
        retryCheckCount++
        await fetchTask()
        await fetchErrorKeys()
        
        // 如果任务状态不再是 retrying，说明重试已完成
        if (task.value?.status !== 'retrying') {
          clearInterval(retryCheckInterval)
          retryCheckInterval = null
          retryingKeys.value = false
          retryingProgress.value = ''
          ElMessage.info('重试已完成')
          return
        }
        
        // 检查次数达到上限
        if (retryCheckCount >= maxChecks) {
          clearInterval(retryCheckInterval)
          retryCheckInterval = null
          retryingKeys.value = false
          retryingProgress.value = ''
        }
      }, 2000)
    }
  }, 1000)
})

onUnmounted(() => {
  // 清理轮询定时器
  if (refreshTimer) {
    clearInterval(refreshTimer)
    refreshTimer = null
  }
  
  // 清理已耗时间定时器
  stopElapsedTimer()
  
  // 清理重试检查定时器
  clearRetryCheckInterval()
  
  // 【修复】清理校验检查定时器
  if (verifyCheckTimer) {
    clearInterval(verifyCheckTimer)
    verifyCheckTimer = null
  }
  
  // 清理 WebSocket
  cleanupWebSocket()
})

// 监听任务ID变化，重新订阅
watch(taskId, (newId, oldId) => {
  if (oldId) {
    wsService.unsubscribe(oldId)
  }
  if (newId) {
    fetchTask()
    fetchVerifyResults()
    fetchTaskLogs()
    fetchErrorKeys()
    
    if (wsRealtimeEnabled.value && wsService.isConnected) {
      wsService.subscribe(newId)
    }
  }
})
</script>

<style lang="scss" scoped>
.task-detail {
  max-width: 1400px;
  margin: 0 auto;
}

// 迁移前依赖校验
.preflight-check {
  margin-bottom: 24px;

  .preflight-header {
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin-bottom: 16px;
    
    h3 {
      display: flex;
      align-items: center;
      gap: 8px;
      margin: 0;
      font-size: 16px;
    }
  }

  .preflight-hint {
    margin-bottom: 16px;
  }

  .preflight-summary {
    margin-bottom: 16px;
  }

  .check-list {
    display: flex;
    flex-direction: column;
    gap: 8px;
  }

  .check-item {
    display: flex;
    align-items: flex-start;
    gap: 12px;
    padding: 12px 16px;
    border-radius: 8px;
    background: var(--el-fill-color-lighter, #fafafa);
    border: 1px solid var(--el-border-color-lighter, #ebeef5);
    transition: all 0.2s;

    &.passed {
      background: #f0f9ff;
      border-color: #b3e5c8;
    }
    &.failed {
      background: #fff2f0;
      border-color: #ffccc7;
    }
    &.warning {
      background: #fffbe6;
      border-color: #ffe58f;
    }

    .check-icon {
      flex-shrink: 0;
      font-size: 20px;
      line-height: 1;
      margin-top: 2px;
    }

    .check-content {
      flex: 1;
      min-width: 0;

      .check-title {
        display: flex;
        align-items: center;
        gap: 8px;
        margin-bottom: 4px;

        .check-name {
          font-weight: 600;
          font-size: 14px;
          color: var(--el-text-color-primary);
        }
      }

      .check-message {
        font-size: 13px;
        color: var(--el-text-color-regular);
        line-height: 1.5;
      }

      .check-detail {
        font-size: 12px;
        color: var(--el-text-color-secondary);
        margin-top: 4px;
        line-height: 1.4;
        word-break: break-all;
      }
    }
  }
}

.topology-warnings {
  margin-bottom: 16px;
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
    
    .realtime-option {
      display: flex;
      align-items: center;
      gap: 6px;
      
      .dot {
        width: 6px;
        height: 6px;
        border-radius: 50%;
        background: #ccc;
        
        &.connected {
          background: #52c41a;
        }
      }
    }
    
    .ws-status {
      display: flex;
      align-items: center;
      gap: 4px;
      font-size: 12px;
      color: #999;
      padding: 4px 8px;
      border-radius: 4px;
      background: var(--bg-primary);
      
      .status-dot {
        width: 6px;
        height: 6px;
        border-radius: 50%;
        background: #ccc;
      }
      
      &.connected {
        color: #52c41a;
        background: rgba(82, 196, 26, 0.1);
        
        .status-dot {
          background: #52c41a;
          box-shadow: 0 0 0 2px rgba(82, 196, 26, 0.2);
          animation: pulse 2s infinite;
        }
      }
    }
    
    @keyframes pulse {
      0% {
        box-shadow: 0 0 0 0 rgba(82, 196, 26, 0.4);
      }
      70% {
        box-shadow: 0 0 0 6px rgba(82, 196, 26, 0);
      }
      100% {
        box-shadow: 0 0 0 0 rgba(82, 196, 26, 0);
      }
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
      
      .estimating {
        font-size: 32px;
        color: var(--text-secondary);
        animation: pulse 1.5s ease-in-out infinite;
        -webkit-text-fill-color: var(--text-secondary);
        background: none;
      }
      
      .estimating-hint {
        font-size: 12px;
        color: var(--text-muted, #999);
        display: block;
        margin-top: 2px;
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
        
        &.muted {
          color: var(--text-tertiary);
          font-size: 16px;
        }
        
        &.warning {
          color: var(--el-color-warning);
        }
        
        &.estimating-text {
          color: var(--text-muted, #999);
          font-size: 14px;
          animation: pulse 1.5s ease-in-out infinite;
        }
      }
      
      .warning-icon {
        color: var(--el-color-warning);
        margin-left: 4px;
        vertical-align: middle;
        cursor: help;
      }
    }
    
    // 紧凑布局样式
    &.compact-layout {
      .stat-grid {
        display: grid;
        grid-template-columns: repeat(3, 1fr);
        gap: 12px 24px;
        
        @media (max-width: 600px) {
          grid-template-columns: repeat(2, 1fr);
        }
        
        .stat-item {
          padding: 8px 0;
          
          .label {
            font-size: 12px;
            margin-bottom: 2px;
          }
          
          .value {
            font-size: 18px;
            
            &.muted {
              font-size: 15px;
            }
          }
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
    
    // 紧凑布局样式
    &.compact {
      .config-grid {
        display: grid;
        grid-template-columns: repeat(4, 1fr);
        gap: 8px 16px;
        margin-bottom: 12px;
        
        @media (max-width: 600px) {
          grid-template-columns: repeat(2, 1fr);
        }
        
        .config-item {
          display: flex;
          flex-direction: column;
          padding: 8px 10px;
          background: var(--bg-primary);
          border-radius: var(--radius-sm);
          
          .label {
            font-size: 11px;
            color: var(--text-tertiary);
            margin-bottom: 2px;
          }
          
          .value {
            font-size: 14px;
            font-weight: 600;
            color: var(--text-primary);
            display: flex;
            align-items: center;
            gap: 4px;
            
            &.highlight {
              color: var(--primary-color);
            }
            
            &.adjusting {
              color: var(--el-color-warning);
              
              .adjusting-icon {
                animation: spin 1s linear infinite;
              }
            }
          }
        }
      }
      
      @keyframes spin {
        from { transform: rotate(0deg); }
        to { transform: rotate(360deg); }
      }
      
      .filter-section {
        border-top: 1px solid var(--border-light);
        padding-top: 12px;
        margin-top: 4px;
        
        .filter-title {
          display: flex;
          align-items: center;
          gap: 6px;
          font-size: 13px;
          font-weight: 600;
          color: var(--text-secondary);
          margin-bottom: 8px;
          
          .el-icon {
            font-size: 14px;
          }
        }
        
        .filter-content {
          background: var(--bg-primary);
          border-radius: var(--radius-sm);
          padding: 10px 12px;
          
          .filter-item {
            display: flex;
            align-items: flex-start;
            gap: 8px;
            margin-bottom: 6px;
            font-size: 13px;
            
            &:last-child {
              margin-bottom: 0;
            }
            
            &.no-filter {
              color: var(--text-tertiary);
            }
            
            .label {
              color: var(--text-tertiary);
              flex-shrink: 0;
              min-width: 70px;
            }
            
            .value {
              color: var(--text-primary);
              word-break: break-all;
              
              &.mono {
                font-family: 'Consolas', 'Monaco', monospace;
                font-size: 12px;
                background: rgba(0, 0, 0, 0.05);
                padding: 2px 6px;
                border-radius: 3px;
              }
              
              &.warning {
                color: var(--el-color-warning);
              }
            }
          }
          
          .no-filter-text {
            color: var(--text-tertiary);
            font-size: 13px;
          }
        }
        
        &.no-filter-config {
          .filter-content {
            background: transparent;
            padding: 0;
          }
        }
      }
    }
  }
}

.config-card {
  .config-header {
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin-bottom: 12px;
    
    h3 {
      margin-bottom: 0;
    }
    
    .adjust-btn {
      font-weight: 500;
      padding: 8px 16px;
      
      &:hover {
        transform: translateY(-1px);
        box-shadow: 0 4px 12px rgba(37, 99, 235, 0.3);
      }
    }
  }
}

// 影子模式统计样式
.shadow-stats-section {
  h3 {
    display: flex;
    align-items: center;
    gap: 8px;
    font-size: 16px;
    font-weight: 600;
    margin-bottom: 16px;
  }
  
  .shadow-stats-grid {
    display: grid;
    grid-template-columns: repeat(5, 1fr);
    gap: 16px;
    margin-bottom: 20px;
    
    @media (max-width: 900px) {
      grid-template-columns: repeat(3, 1fr);
    }
    
    .stat-item {
      text-align: center;
      padding: 16px;
      background: var(--bg-primary);
      border-radius: var(--radius-md);
      
      .stat-value {
        display: block;
        font-size: 24px;
        font-weight: 700;
        color: var(--text-primary);
        margin-bottom: 4px;
        
        &.highlight { color: var(--primary-color); }
        &.warning { color: var(--el-color-warning); }
        &.info { color: var(--el-color-info); }
      }
      
      .stat-label {
        font-size: 13px;
        color: var(--text-secondary);
      }
    }
  }
  
  .type-distribution {
    h4 {
      font-size: 14px;
      font-weight: 600;
      color: var(--text-secondary);
      margin-bottom: 12px;
    }
    
    .type-grid {
      display: flex;
      flex-wrap: wrap;
      gap: 12px;
      
      .type-item {
        display: flex;
        align-items: center;
        gap: 8px;
        padding: 8px 16px;
        background: var(--bg-primary);
        border-radius: var(--radius-sm);
        
        .type-name {
          font-size: 13px;
          color: var(--text-secondary);
          text-transform: uppercase;
        }
        
        .type-count {
          font-size: 14px;
          font-weight: 600;
          color: var(--primary-color);
        }
      }
    }
  }
}

// 增量同步统计样式
.incremental-stats-section {
  .section-header {
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
    
    .header-right {
      display: flex;
      align-items: center;
      gap: 12px;
      
      .sync-status {
        display: flex;
        align-items: center;
        gap: 6px;
        font-size: 13px;
        color: var(--text-secondary);
        
        .dot {
          width: 8px;
          height: 8px;
          border-radius: 50%;
          background: var(--text-muted);
        }
        
        &.syncing {
          color: var(--success-color);
          
          .dot {
            background: var(--success-color);
            animation: pulse 1.5s infinite;
          }
        }
      }
    }
  }
  
  .incr-stats-grid {
    display: grid;
    grid-template-columns: repeat(4, 1fr);
    gap: 16px;
    margin-bottom: 20px;
    
    @media (max-width: 768px) {
      grid-template-columns: repeat(2, 1fr);
    }
    
    .stat-item {
      text-align: center;
      padding: 16px;
      background: var(--bg-primary);
      border-radius: var(--radius-md);
      
      .stat-value {
        display: block;
        font-size: 24px;
        font-weight: 700;
        color: var(--text-primary);
        margin-bottom: 4px;
        
        &.highlight { color: var(--success-color); }
        &.warning { color: var(--el-color-warning); }
        &.error { color: var(--error-color); }
        &.filtered { color: var(--el-color-info); }
      }
      
      .stat-label {
        font-size: 13px;
        color: var(--text-secondary);
      }
    }
  }
  
  .binlog-stats {
    background: var(--bg-primary);
    border-radius: var(--radius-md);
    padding: 16px;
    
    .binlog-stats-row {
      display: grid;
      grid-template-columns: repeat(4, 1fr);
      gap: 24px;
      
      @media (max-width: 768px) {
        grid-template-columns: repeat(2, 1fr);
      }
      
      .binlog-item {
        display: flex;
        flex-direction: column;
        gap: 4px;
        
        .label {
          font-size: 12px;
          color: var(--text-secondary);
        }
        
        .value {
          font-size: 15px;
          font-weight: 600;
          color: var(--text-primary);
          
          &.mono {
            font-family: monospace;
            font-size: 13px;
          }
          
          &.lag-warning {
            color: var(--el-color-warning);
          }
          
          &.warning {
            color: var(--error-color);
          }
        }
      }
    }
  }
}

@keyframes pulse {
  0%, 100% { opacity: 1; }
  50% { opacity: 0.4; }
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
  
  /* 【修复】校验进行中样式 */
  .verify-loading {
    display: flex;
    align-items: center;
    justify-content: center;
    gap: 12px;
    padding: 40px;
    color: var(--primary-color);
    font-size: 14px;
    
    .el-icon {
      font-size: 24px;
    }
    
    .is-loading {
      animation: spin 1s linear infinite;
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
    
    .header-actions {
      display: flex;
      gap: 8px;
    }
  }
  
  .retry-progress-bar {
    margin: 12px 0;
    padding: 12px 16px;
    background: #fdf6ec;
    border-radius: 8px;
    border: 1px solid #faecd8;
    
    .retry-progress-info {
      display: flex;
      justify-content: space-between;
      align-items: center;
      margin-bottom: 8px;
      font-size: 13px;
      
      .retry-label {
        font-weight: 600;
        color: #e6a23c;
      }
      
      .retry-stats {
        color: #666;
        
        .success-text {
          color: #67c23a;
          font-weight: 600;
        }
        
        .fail-text {
          color: #f56c6c;
          font-weight: 600;
        }
      }
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
      position: relative;
      transition: all 0.2s ease;
      
      &.clickable {
        cursor: pointer;
        
        &:hover {
          transform: translateY(-2px);
          box-shadow: 0 4px 12px rgba(0, 0, 0, 0.1);
        }
        
        &.active {
          border: 2px solid var(--primary-color);
          background: var(--primary-lighter);
        }
      }
      
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
      
      .click-hint {
        display: block;
        font-size: 11px;
        color: var(--primary-color);
        margin-top: 4px;
        opacity: 0;
        transition: opacity 0.2s;
      }
      
      &:hover .click-hint {
        opacity: 1;
      }
    }
  }
  
  .error-keys-list {
    .filter-info {
      display: flex;
      align-items: center;
      gap: 12px;
      margin-bottom: 12px;
      padding: 8px 12px;
      background: var(--bg-primary);
      border-radius: var(--radius-sm);
      
      .filter-count {
        font-size: 12px;
        color: var(--text-secondary);
      }
    }
    
    .mono {
      font-family: 'Consolas', 'Monaco', monospace;
      font-size: 12px;
    }
    
    .key-name {
      display: inline-block;
      max-width: 160px;
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
      vertical-align: middle;
    }
    
    .error-detail {
      display: block;
      font-size: 12px;
      color: var(--text-secondary);
      word-break: break-all;
      line-height: 1.4;
    }
    
    .truncated-tip {
      margin: 8px 0 0;
    }

    .list-footer {
      display: flex;
      justify-content: space-between;
      align-items: center;
      padding: 12px 0;
      font-size: 13px;
      color: var(--text-secondary);
    }

    .error-keys-pagination {
      display: flex;
      justify-content: center;
      padding: 12px 0 4px;
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
    
    &.filter-empty {
      color: var(--text-secondary);
      flex-direction: column;
      gap: 12px;
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
    padding: 12px 16px;
    max-height: 400px;
    overflow-y: auto;
    overflow-x: hidden;
    font-family: 'Consolas', 'Monaco', monospace;
    font-size: 12px;
    line-height: 1.5;
  }
  
  .no-logs {
    color: #888;
    text-align: center;
    padding: 40px;
  }
  
  .log-entry {
    display: flex;
    align-items: flex-start;
    gap: 8px;
    padding: 3px 0;
    color: #d4d4d4;
    cursor: pointer;
    border-radius: 2px;
    transition: background 0.15s;
    
    &:hover {
      background: rgba(255, 255, 255, 0.05);
    }
    
    &.debug { color: #888; }
    &.info { color: #4fc3f7; }
    &.warn { color: #ffb74d; }
    &.error { color: #ef5350; }
    &.fatal { color: #ff1744; }
    
    .log-time {
      color: #888;
      flex-shrink: 0;
      min-width: 80px;
    }
    
    .log-level {
      width: 45px;
      flex-shrink: 0;
      font-weight: 600;
      text-align: left;
      
      &.debug { color: #888; }
      &.info { color: #4fc3f7; }
      &.warn { color: #ffb74d; }
      &.error { color: #ef5350; }
      &.fatal { color: #ff1744; }
    }
    
    .log-message {
      flex-shrink: 0;
      white-space: nowrap;
      max-width: 400px;
      overflow: hidden;
      text-overflow: ellipsis;
    }
    
    .log-fields {
      color: #9e9e9e;
      font-size: 12px;
      flex: 1;
      min-width: 0;
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
    }
    
    // 展开状态
    &.expanded {
      flex-wrap: wrap;
      background: rgba(255, 255, 255, 0.03);
      padding: 6px 8px;
      margin: 2px 0;
      
      .log-message {
        max-width: none;
        white-space: normal;
        word-break: break-word;
      }
      
      .log-fields {
        flex-basis: 100%;
        margin-top: 4px;
        padding-left: 133px; // log-time(80) + log-level(45) + gap(8)
        white-space: normal;
        word-break: break-word;
        max-width: none;
        overflow: visible;
      }
    }
  }
}

// 自动恢复设置对话框样式
.auto-recovery-status {
  background: var(--bg-primary);
  border-radius: var(--radius-md);
  padding: 16px;
  margin-bottom: 16px;
  
  .status-row {
    display: flex;
    align-items: center;
    gap: 12px;
    padding: 8px 0;
    font-size: 13px;
    
    &:not(:last-child) {
      border-bottom: 1px dashed var(--border-light);
    }
    
    .label {
      width: 100px;
      color: var(--text-secondary);
      flex-shrink: 0;
    }
    
    .value {
      color: var(--text-primary);
    }
  }
}

.form-tip {
  font-size: 12px;
  color: var(--text-tertiary);
  margin-top: 4px;
}
</style>
