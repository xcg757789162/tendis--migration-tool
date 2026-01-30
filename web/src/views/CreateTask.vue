<template>
  <div class="create-task">
    <div class="page-header">
      <el-button text @click="$router.push('/tasks')">
        <el-icon><ArrowLeft /></el-icon> 返回
      </el-button>
      <h1>创建迁移任务</h1>
    </div>
    
    <div class="form-container card">
      <el-form 
        ref="formRef"
        :model="form" 
        :rules="rules" 
        label-width="140px"
        label-position="top"
      >
        <!-- 基本信息 -->
        <div class="form-section">
          <h3><el-icon><Document /></el-icon> 基本信息</h3>
          
          <el-form-item label="任务名称" prop="name">
            <el-input 
              v-model="form.name" 
              placeholder="请输入任务名称，如：生产环境数据迁移"
              maxlength="100"
              show-word-limit
            />
          </el-form-item>
          
          <el-form-item label="迁移模式" prop="migration_mode">
            <el-radio-group v-model="form.migration_mode">
              <el-radio value="full_only">
                <span class="mode-label">全量迁移</span>
                <span class="mode-desc">仅执行全量数据迁移，适用于源端数据不再变化的场景</span>
              </el-radio>
              <el-radio value="full_and_incremental">
                <span class="mode-label">全量+增量迁移</span>
                <span class="mode-desc">先全量迁移，再持续同步增量数据，适用于在线迁移场景</span>
              </el-radio>
            </el-radio-group>
          </el-form-item>
        </div>
        
        <!-- 源集群配置 -->
        <div class="form-section">
          <h3><el-icon><Connection /></el-icon> 源集群配置</h3>
          
          <el-form-item label="集群地址" prop="source_addrs">
            <div class="addrs-input">
              <el-input
                v-for="(addr, index) in form.source_addrs"
                :key="'source-' + index"
                v-model="form.source_addrs[index]"
                placeholder="如: 127.0.0.1:6379"
              >
                <template #append v-if="form.source_addrs.length > 1">
                  <el-button @click="removeAddr('source', index)">
                    <el-icon><Delete /></el-icon>
                  </el-button>
                </template>
              </el-input>
              <el-button text type="primary" @click="addAddr('source')">
                <el-icon><Plus /></el-icon> 添加节点
              </el-button>
            </div>
          </el-form-item>
          
          <el-form-item label="访问密码">
            <el-input 
              v-model="form.source_password" 
              type="password"
              placeholder="无密码可留空"
              show-password
            />
          </el-form-item>
          
          <el-form-item>
            <el-button 
              type="success" 
              plain 
              @click="testConnection('source')"
              :loading="testingSource"
            >
              <el-icon><Connection /></el-icon> 测试连接
            </el-button>
            <span v-if="sourceTestResult" :class="['test-result', sourceTestResult.success ? 'success' : 'error']">
              {{ sourceTestResult.message }}
              <span v-if="sourceTestResult.latency_ms">({{ sourceTestResult.latency_ms }}ms)</span>
            </span>
          </el-form-item>
          
          <!-- 源集群信息展示 -->
          <div v-if="sourceTestResult?.cluster_info" class="cluster-info">
            <div class="info-row">
              <span class="label">连接模式:</span>
              <el-tag :type="sourceTestResult.cluster_info.mode === 'cluster' ? 'success' : 'info'" size="small">
                {{ sourceTestResult.cluster_info.mode === 'cluster' ? '集群模式' : '单机模式' }}
              </el-tag>
            </div>
            <div class="info-row">
              <span class="label">Redis版本:</span>
              <span>{{ sourceTestResult.cluster_info.version }}</span>
            </div>
            <div class="info-row">
              <span class="label">节点数:</span>
              <span>{{ sourceTestResult.cluster_info.node_count }}</span>
            </div>
            <div class="info-row">
              <span class="label">总Key数:</span>
              <span>{{ formatNumber(sourceTestResult.cluster_info.total_keys) }}</span>
            </div>
            <div class="info-row">
              <span class="label">内存使用:</span>
              <span>{{ formatBytes(sourceTestResult.cluster_info.total_memory) }}</span>
            </div>
          </div>
        </div>
        
        <!-- 目标集群配置 -->
        <div class="form-section">
          <h3><el-icon><Connection /></el-icon> 目标集群配置</h3>
          
          <el-form-item label="集群地址" prop="target_addrs">
            <div class="addrs-input">
              <el-input
                v-for="(addr, index) in form.target_addrs"
                :key="'target-' + index"
                v-model="form.target_addrs[index]"
                placeholder="如: 127.0.0.1:6379"
              >
                <template #append v-if="form.target_addrs.length > 1">
                  <el-button @click="removeAddr('target', index)">
                    <el-icon><Delete /></el-icon>
                  </el-button>
                </template>
              </el-input>
              <el-button text type="primary" @click="addAddr('target')">
                <el-icon><Plus /></el-icon> 添加节点
              </el-button>
            </div>
          </el-form-item>
          
          <el-form-item label="访问密码">
            <el-input 
              v-model="form.target_password" 
              type="password"
              placeholder="无密码可留空"
              show-password
            />
          </el-form-item>
          
          <el-form-item>
            <el-button 
              type="success" 
              plain 
              @click="testConnection('target')"
              :loading="testingTarget"
            >
              <el-icon><Connection /></el-icon> 测试连接
            </el-button>
            <span v-if="targetTestResult" :class="['test-result', targetTestResult.success ? 'success' : 'error']">
              {{ targetTestResult.message }}
              <span v-if="targetTestResult.latency_ms">({{ targetTestResult.latency_ms }}ms)</span>
            </span>
          </el-form-item>
          
          <!-- 目标集群信息展示 -->
          <div v-if="targetTestResult?.cluster_info" class="cluster-info">
            <div class="info-row">
              <span class="label">连接模式:</span>
              <el-tag :type="targetTestResult.cluster_info.mode === 'cluster' ? 'success' : 'info'" size="small">
                {{ targetTestResult.cluster_info.mode === 'cluster' ? '集群模式' : '单机模式' }}
              </el-tag>
            </div>
            <div class="info-row">
              <span class="label">Redis版本:</span>
              <span>{{ targetTestResult.cluster_info.version }}</span>
            </div>
            <div class="info-row">
              <span class="label">节点数:</span>
              <span>{{ targetTestResult.cluster_info.node_count }}</span>
            </div>
            <div class="info-row">
              <span class="label">总Key数:</span>
              <span>{{ formatNumber(targetTestResult.cluster_info.total_keys) }}</span>
            </div>
            <div class="info-row">
              <span class="label">内存使用:</span>
              <span>{{ formatBytes(targetTestResult.cluster_info.total_memory) }}</span>
            </div>
          </div>
        </div>
        
        <!-- Key过滤配置 - 独立区块，默认显示 -->
        <div class="form-section">
          <h3><el-icon><Filter /></el-icon> Key过滤配置</h3>
          
          <el-row :gutter="24">
            <el-col :span="12">
              <el-form-item label="过滤模式">
                <el-select v-model="form.options.key_filter.mode" style="width: 100%">
                  <el-option label="all - 迁移所有Key" value="all" />
                  <el-option label="prefix - 按前缀迁移/排除" value="prefix" />
                  <el-option label="pattern - 按正则匹配" value="pattern" />
                </el-select>
                <div class="form-tip">选择如何筛选要迁移的Key</div>
              </el-form-item>
            </el-col>
          </el-row>
          
          <div v-if="form.options.key_filter.mode === 'prefix'" class="filter-options">
            <el-form-item label="迁移前缀（只迁移这些前缀的Key，留空则迁移所有）">
              <div class="prefix-input">
                <el-tag
                  v-for="(prefix, index) in form.options.key_filter.prefixes"
                  :key="'prefix-' + index"
                  closable
                  type="success"
                  @close="removePrefix('prefixes', index)"
                  style="margin-right: 8px; margin-bottom: 8px"
                >
                  {{ prefix }}
                </el-tag>
                <el-input
                  v-model="newPrefix"
                  placeholder="输入前缀后按回车添加，如: user:"
                  style="width: 280px"
                  @keyup.enter="addPrefix('prefixes')"
                >
                  <template #append>
                    <el-button @click="addPrefix('prefixes')">添加</el-button>
                  </template>
                </el-input>
              </div>
            </el-form-item>
            
            <el-form-item label="排除前缀（跳过这些前缀的Key）">
              <div class="prefix-input">
                <el-tag
                  v-for="(prefix, index) in form.options.key_filter.exclude_prefixes"
                  :key="'exclude-' + index"
                  closable
                  type="danger"
                  @close="removePrefix('exclude_prefixes', index)"
                  style="margin-right: 8px; margin-bottom: 8px"
                >
                  {{ prefix }}
                </el-tag>
                <el-input
                  v-model="newExcludePrefix"
                  placeholder="输入前缀后按回车添加，如: temp:"
                  style="width: 280px"
                  @keyup.enter="addPrefix('exclude_prefixes')"
                >
                  <template #append>
                    <el-button @click="addPrefix('exclude_prefixes')">添加</el-button>
                  </template>
                </el-input>
              </div>
            </el-form-item>
          </div>
          
          <div v-if="form.options.key_filter.mode === 'pattern'" class="filter-options">
            <el-form-item label="正则模式（匹配的Key才会被迁移）">
              <div class="prefix-input">
                <el-tag
                  v-for="(pattern, index) in form.options.key_filter.patterns"
                  :key="'pattern-' + index"
                  closable
                  @close="removePrefix('patterns', index)"
                  style="margin-right: 8px; margin-bottom: 8px"
                >
                  {{ pattern }}
                </el-tag>
                <el-input
                  v-model="newPattern"
                  placeholder="输入正则后按回车添加，如: ^user:\d+$"
                  style="width: 280px"
                  @keyup.enter="addPrefix('patterns')"
                >
                  <template #append>
                    <el-button @click="addPrefix('patterns')">添加</el-button>
                  </template>
                </el-input>
              </div>
            </el-form-item>
          </div>
        </div>
        
        <!-- 高级配置 -->
        <div class="form-section">
          <h3>
            <el-icon><Setting /></el-icon> 高级配置
            <el-switch v-model="showAdvanced" style="margin-left: 12px" />
            <el-button 
              v-if="showAdvanced && sourceTestResult?.success && targetTestResult?.success"
              type="primary" 
              size="small" 
              style="margin-left: 16px"
              @click="getRecommendedConfig"
              :loading="loadingRecommend"
            >
              <el-icon><MagicStick /></el-icon> 智能推荐配置
            </el-button>
          </h3>
          
          <!-- 推荐配置结果 -->
          <div v-if="recommendedConfig" class="recommend-result">
            <el-alert type="success" :closable="false" show-icon>
              <template #title>
                <span style="font-weight: bold">推荐配置已生成</span>
              </template>
              <template #default>
                <div class="recommend-info">
                  <div class="recommend-row">
                    <span class="label">预计迁移速度:</span>
                    <span class="value highlight">{{ formatNumber(recommendedConfig.estimated_speed) }} keys/s</span>
                  </div>
                  <div class="recommend-row">
                    <span class="label">预计完成时间:</span>
                    <span class="value highlight">{{ recommendedConfig.estimated_time }}</span>
                  </div>
                  <div class="recommend-row">
                    <span class="label">推荐Worker数:</span>
                    <span class="value">{{ recommendedConfig.worker_count }}</span>
                  </div>
                  <div class="recommend-row">
                    <span class="label">推荐理由:</span>
                    <span class="value reason">{{ recommendedConfig.reason }}</span>
                  </div>
                </div>
                <el-button type="primary" size="small" @click="applyRecommendedConfig" style="margin-top: 12px">
                  <el-icon><Check /></el-icon> 应用推荐配置
                </el-button>
              </template>
            </el-alert>
          </div>
          
          <div v-show="showAdvanced" class="advanced-options">
            <el-row :gutter="24">
              <el-col :span="12">
                <el-form-item label="Worker数量">
                  <el-input-number 
                    v-model="form.options.worker_count" 
                    :min="1" 
                    :max="100"
                    style="width: 100%"
                  />
                  <div class="form-tip">并行处理的Worker进程数，推荐4-64个</div>
                </el-form-item>
              </el-col>
              
              <el-col :span="12">
                <el-form-item label="批次大小">
                  <el-input-number 
                    v-model="form.options.scan_batch_size" 
                    :min="100" 
                    :max="10000"
                    :step="100"
                    style="width: 100%"
                  />
                  <div class="form-tip">每次SCAN扫描的Key数量</div>
                </el-form-item>
              </el-col>
            </el-row>
            
            <el-row :gutter="24">
              <el-col :span="12">
                <el-form-item label="冲突策略">
                  <el-select v-model="form.options.conflict_policy" style="width: 100%">
                    <el-option label="skip_full_only - 全量跳过，增量覆盖" value="skip_full_only" />
                    <el-option label="skip - 跳过并记录（后续单独处理）" value="skip" />
                    <el-option label="replace - 直接覆盖" value="replace" />
                    <el-option label="error - 遇到冲突报错" value="error" />
                  </el-select>
                  <div class="form-tip">目标端已存在相同Key时的处理策略</div>
                </el-form-item>
              </el-col>
              
              <el-col :span="12">
                <el-form-item label="大Key阈值">
                  <el-select v-model="form.options.large_key_threshold" style="width: 100%">
                    <el-option label="1 MB" :value="1048576" />
                    <el-option label="10 MB" :value="10485760" />
                    <el-option label="50 MB" :value="52428800" />
                    <el-option label="100 MB" :value="104857600" />
                  </el-select>
                  <div class="form-tip">超过此大小的Key将特殊处理</div>
                </el-form-item>
              </el-col>
            </el-row>
            
            <el-row :gutter="24">
              <el-col :span="8">
                <el-form-item label="启用压缩">
                  <el-switch v-model="form.options.enable_compression" />
                  <div class="form-tip">对大Key数据启用LZ4压缩传输</div>
                </el-form-item>
              </el-col>
              <el-col :span="8">
                <el-form-item label="跳过全量阶段">
                  <el-switch v-model="form.options.skip_full_sync" />
                  <div class="form-tip">跳过全量迁移，仅执行增量同步</div>
                </el-form-item>
              </el-col>
              <el-col :span="8">
                <el-form-item label="跳过增量阶段">
                  <el-switch v-model="form.options.skip_incremental" />
                  <div class="form-tip">跳过增量同步阶段</div>
                </el-form-item>
              </el-col>
            </el-row>
            
            <!-- 限速配置 -->
            <div class="rate-limit-section">
              <h4>限速配置</h4>
              
              <el-row :gutter="24">
                <el-col :span="12">
                  <el-form-item label="源端QPS限制">
                    <el-input-number 
                      v-model="form.options.rate_limit.source_qps" 
                      :min="0" 
                      :max="100000"
                      :step="1000"
                      style="width: 100%"
                    />
                    <div class="form-tip">0表示不限制</div>
                  </el-form-item>
                </el-col>
                
                <el-col :span="12">
                  <el-form-item label="目标端QPS限制">
                    <el-input-number 
                      v-model="form.options.rate_limit.target_qps" 
                      :min="0" 
                      :max="100000"
                      :step="1000"
                      style="width: 100%"
                    />
                    <div class="form-tip">0表示不限制</div>
                  </el-form-item>
                </el-col>
              </el-row>
              
              <el-row :gutter="24">
                <el-col :span="12">
                  <el-form-item label="源端连接数">
                    <el-input-number 
                      v-model="form.options.rate_limit.source_connections" 
                      :min="1" 
                      :max="500"
                      style="width: 100%"
                    />
                  </el-form-item>
                </el-col>
                
                <el-col :span="12">
                  <el-form-item label="目标端连接数">
                    <el-input-number 
                      v-model="form.options.rate_limit.target_connections" 
                      :min="1" 
                      :max="200"
                      style="width: 100%"
                    />
                  </el-form-item>
                </el-col>
              </el-row>
            </div>
          </div>
        </div>
        
        <!-- 提交按钮 -->
        <div class="form-actions">
          <div class="left-actions">
            <el-dropdown @command="handleTemplateCommand" trigger="click">
              <el-button>
                <el-icon><Document /></el-icon> 模板 <el-icon><ArrowDown /></el-icon>
              </el-button>
              <template #dropdown>
                <el-dropdown-menu>
                  <el-dropdown-item command="save">
                    <el-icon><FolderAdd /></el-icon> 保存为模板
                  </el-dropdown-item>
                  <el-dropdown-item command="load" divided>
                    <el-icon><FolderOpened /></el-icon> 从模板加载
                  </el-dropdown-item>
                </el-dropdown-menu>
              </template>
            </el-dropdown>
          </div>
          <div class="right-actions">
            <el-button @click="$router.push('/tasks')">取消</el-button>
            <el-button type="primary" @click="submitForm" :loading="submitting">
              创建任务
            </el-button>
          </div>
        </div>
      </el-form>
    </div>

    <!-- 保存模板对话框 -->
    <el-dialog v-model="saveTemplateDialog" title="保存为模板" width="500px">
      <el-form :model="templateForm" label-width="80px">
        <el-form-item label="模板名称" required>
          <el-input v-model="templateForm.name" placeholder="请输入模板名称" />
        </el-form-item>
        <el-form-item label="描述">
          <el-input v-model="templateForm.description" type="textarea" :rows="3" placeholder="可选，描述模板用途" />
        </el-form-item>
      </el-form>
      <template #footer>
        <el-button @click="saveTemplateDialog = false">取消</el-button>
        <el-button type="primary" @click="saveTemplate" :loading="savingTemplate">保存</el-button>
      </template>
    </el-dialog>

    <!-- 加载模板对话框 -->
    <el-dialog v-model="loadTemplateDialog" title="从模板加载" width="700px">
      <el-table :data="templateList" v-loading="loadingTemplates" style="width: 100%" max-height="400">
        <el-table-column prop="name" label="模板名称" width="180" />
        <el-table-column prop="description" label="描述" show-overflow-tooltip />
        <el-table-column prop="migration_mode" label="迁移模式" width="120">
          <template #default="{ row }">
            {{ row.migration_mode === 'full_only' ? '全量' : '全量+增量' }}
          </template>
        </el-table-column>
        <el-table-column prop="created_at" label="创建时间" width="160">
          <template #default="{ row }">
            {{ formatTime(row.created_at) }}
          </template>
        </el-table-column>
        <el-table-column label="操作" width="150" fixed="right">
          <template #default="{ row }">
            <el-button type="primary" text @click="loadFromTemplate(row)">加载</el-button>
            <el-button type="danger" text @click="deleteTemplateConfirm(row)">删除</el-button>
          </template>
        </el-table-column>
      </el-table>
      <div v-if="templateList.length === 0 && !loadingTemplates" class="empty-templates">
        暂无保存的模板
      </div>
    </el-dialog>
  </div>
</template>

<script setup>
import { ref, reactive } from 'vue'
import { useRouter } from 'vue-router'
import { ElMessage, ElMessageBox } from 'element-plus'
import api from '@/api'

const router = useRouter()
const formRef = ref()
const showAdvanced = ref(false)
const submitting = ref(false)
const newPrefix = ref('')
const newExcludePrefix = ref('')
const newPattern = ref('')

// 测试连接相关
const testingSource = ref(false)
const testingTarget = ref(false)
const sourceTestResult = ref(null)
const targetTestResult = ref(null)

// 推荐配置相关
const loadingRecommend = ref(false)
const recommendedConfig = ref(null)

// 模板相关
const saveTemplateDialog = ref(false)
const loadTemplateDialog = ref(false)
const savingTemplate = ref(false)
const loadingTemplates = ref(false)
const templateList = ref([])
const templateForm = reactive({
  name: '',
  description: ''
})

const form = reactive({
  name: '',
  migration_mode: 'full_and_incremental',
  source_addrs: [''],
  source_password: '',
  target_addrs: [''],
  target_password: '',
  options: {
    worker_count: 4,
    scan_batch_size: 1000,
    enable_compression: true,
    large_key_threshold: 10485760,
    conflict_policy: 'skip_full_only',
    skip_full_sync: false,
    skip_incremental: false,
    key_filter: {
      mode: 'all',
      prefixes: [],
      exclude_prefixes: [],
      patterns: []
    },
    rate_limit: {
      source_qps: 10000,
      source_connections: 50,
      target_qps: 10000,
      target_connections: 50,
      pipeline_size: 100,
      pipeline_timeout_ms: 5000,
      max_bandwidth_mbps: 0
    }
  }
})

const rules = {
  name: [
    { required: true, message: '请输入任务名称', trigger: 'blur' },
    { min: 2, max: 100, message: '长度在 2 到 100 个字符', trigger: 'blur' }
  ],
  source_addrs: [
    { required: true, message: '请输入源集群地址', trigger: 'blur' }
  ],
  target_addrs: [
    { required: true, message: '请输入目标集群地址', trigger: 'blur' }
  ]
}

// 模板相关函数
const handleTemplateCommand = (command) => {
  if (command === 'save') {
    templateForm.name = form.name || ''
    templateForm.description = ''
    saveTemplateDialog.value = true
  } else if (command === 'load') {
    loadTemplates()
    loadTemplateDialog.value = true
  }
}

const loadTemplates = async () => {
  loadingTemplates.value = true
  try {
    const result = await api.getTemplates()
    templateList.value = result.items || []
  } catch (err) {
    console.error('Failed to load templates:', err)
  } finally {
    loadingTemplates.value = false
  }
}

const saveTemplate = async () => {
  if (!templateForm.name.trim()) {
    ElMessage.error('请输入模板名称')
    return
  }

  savingTemplate.value = true
  try {
    const sourceAddrs = form.source_addrs.filter(a => a.trim())
    const targetAddrs = form.target_addrs.filter(a => a.trim())

    await api.createTemplate({
      name: templateForm.name,
      description: templateForm.description,
      source_cluster: {
        addrs: sourceAddrs,
        password: form.source_password
      },
      target_cluster: {
        addrs: targetAddrs,
        password: form.target_password
      },
      migration_mode: form.migration_mode,
      options: form.options
    })
    
    ElMessage.success('模板保存成功')
    saveTemplateDialog.value = false
  } catch (err) {
    ElMessage.error('保存失败: ' + (err.message || '未知错误'))
  } finally {
    savingTemplate.value = false
  }
}

const loadFromTemplate = (template) => {
  // 加载源集群地址
  if (template.source_cluster) {
    form.source_addrs = template.source_cluster.split(',').filter(a => a.trim())
    if (form.source_addrs.length === 0) form.source_addrs = ['']
    form.source_password = template.source_password || ''
  }
  
  // 加载目标集群地址
  if (template.target_cluster) {
    form.target_addrs = template.target_cluster.split(',').filter(a => a.trim())
    if (form.target_addrs.length === 0) form.target_addrs = ['']
    form.target_password = template.target_password || ''
  }
  
  // 加载迁移模式
  form.migration_mode = template.migration_mode || 'full_and_incremental'
  
  // 加载选项
  if (template.options) {
    Object.assign(form.options, template.options)
    if (template.options.key_filter) {
      form.options.key_filter = { ...template.options.key_filter }
    }
    if (template.options.rate_limit) {
      form.options.rate_limit = { ...template.options.rate_limit }
    }
  }
  
  // 自动生成任务名称
  form.name = template.name + '-' + new Date().toLocaleString('zh-CN', { 
    month: '2-digit', day: '2-digit', hour: '2-digit', minute: '2-digit' 
  }).replace(/[\/\s:]/g, '')
  
  loadTemplateDialog.value = false
  ElMessage.success('已加载模板配置')
}

const deleteTemplateConfirm = (template) => {
  ElMessageBox.confirm(
    `确定要删除模板 "${template.name}" 吗？`,
    '删除确认',
    { confirmButtonText: '删除', cancelButtonText: '取消', type: 'warning' }
  ).then(async () => {
    try {
      await api.deleteTemplate(template.id)
      ElMessage.success('模板已删除')
      loadTemplates()
    } catch (err) {
      ElMessage.error('删除失败')
    }
  }).catch(() => {})
}

const formatTime = (time) => {
  if (!time) return '-'
  return new Date(time).toLocaleString('zh-CN')
}

const addAddr = (type) => {
  if (type === 'source') {
    form.source_addrs.push('')
  } else {
    form.target_addrs.push('')
  }
}

const removeAddr = (type, index) => {
  if (type === 'source') {
    form.source_addrs.splice(index, 1)
  } else {
    form.target_addrs.splice(index, 1)
  }
}

const addPrefix = (type) => {
  let value = ''
  if (type === 'prefixes') {
    value = newPrefix.value.trim()
    if (value && !form.options.key_filter.prefixes.includes(value)) {
      form.options.key_filter.prefixes.push(value)
    }
    newPrefix.value = ''
  } else if (type === 'exclude_prefixes') {
    value = newExcludePrefix.value.trim()
    if (value && !form.options.key_filter.exclude_prefixes.includes(value)) {
      form.options.key_filter.exclude_prefixes.push(value)
    }
    newExcludePrefix.value = ''
  } else if (type === 'patterns') {
    value = newPattern.value.trim()
    if (value && !form.options.key_filter.patterns.includes(value)) {
      form.options.key_filter.patterns.push(value)
    }
    newPattern.value = ''
  }
}

const removePrefix = (type, index) => {
  form.options.key_filter[type].splice(index, 1)
}

// 测试连接
const testConnection = async (type) => {
  const addrs = type === 'source' 
    ? form.source_addrs.filter(a => a.trim())
    : form.target_addrs.filter(a => a.trim())
  const password = type === 'source' ? form.source_password : form.target_password
  
  if (addrs.length === 0) {
    ElMessage.warning('请先输入集群地址')
    return
  }
  
  if (type === 'source') {
    testingSource.value = true
    sourceTestResult.value = null
  } else {
    testingTarget.value = true
    targetTestResult.value = null
  }
  
  try {
    const result = await api.testConnection({ addrs, password })
    if (type === 'source') {
      sourceTestResult.value = result
    } else {
      targetTestResult.value = result
    }
  } catch (err) {
    const errorResult = {
      success: false,
      message: '测试失败: ' + (err.message || '未知错误')
    }
    if (type === 'source') {
      sourceTestResult.value = errorResult
    } else {
      targetTestResult.value = errorResult
    }
  } finally {
    if (type === 'source') {
      testingSource.value = false
    } else {
      testingTarget.value = false
    }
  }
}

// 获取推荐配置
const getRecommendedConfig = async () => {
  const sourceAddrs = form.source_addrs.filter(a => a.trim())
  const targetAddrs = form.target_addrs.filter(a => a.trim())
  
  if (sourceAddrs.length === 0 || targetAddrs.length === 0) {
    ElMessage.warning('请先完成源端和目标端连接测试')
    return
  }
  
  loadingRecommend.value = true
  recommendedConfig.value = null
  
  try {
    const result = await api.getRecommendedConfig({
      source_cluster: {
        addrs: sourceAddrs,
        password: form.source_password
      },
      target_cluster: {
        addrs: targetAddrs,
        password: form.target_password
      }
    })
    recommendedConfig.value = result.recommended
    showAdvanced.value = true
    ElMessage.success('推荐配置已生成')
  } catch (err) {
    ElMessage.error('获取推荐配置失败: ' + (err.message || '未知错误'))
  } finally {
    loadingRecommend.value = false
  }
}

// 应用推荐配置
const applyRecommendedConfig = () => {
  if (!recommendedConfig.value) return
  
  const config = recommendedConfig.value
  form.options.worker_count = config.worker_count
  form.options.scan_batch_size = config.scan_batch_size
  form.options.rate_limit.source_qps = config.source_qps
  form.options.rate_limit.target_qps = config.target_qps
  form.options.rate_limit.source_connections = config.source_connections
  form.options.rate_limit.target_connections = config.target_connections
  if (config.large_key_threshold) {
    form.options.large_key_threshold = config.large_key_threshold
  }
  
  ElMessage.success('推荐配置已应用')
}

// 格式化数字
const formatNumber = (num) => {
  if (num === undefined || num === null) return '-'
  return num.toLocaleString()
}

// 格式化字节
const formatBytes = (bytes) => {
  if (bytes === undefined || bytes === null || bytes === 0) return '-'
  const units = ['B', 'KB', 'MB', 'GB', 'TB']
  let i = 0
  while (bytes >= 1024 && i < units.length - 1) {
    bytes /= 1024
    i++
  }
  return bytes.toFixed(2) + ' ' + units[i]
}

const submitForm = async () => {
  try {
    await formRef.value.validate()
  } catch {
    return
  }

  // 过滤空地址
  const sourceAddrs = form.source_addrs.filter(a => a.trim())
  const targetAddrs = form.target_addrs.filter(a => a.trim())

  if (sourceAddrs.length === 0) {
    ElMessage.error('请输入源集群地址')
    return
  }
  if (targetAddrs.length === 0) {
    ElMessage.error('请输入目标集群地址')
    return
  }

  submitting.value = true

  try {
    const data = {
      name: form.name,
      migration_mode: form.migration_mode,
      source_cluster: {
        addrs: sourceAddrs,
        password: form.source_password
      },
      target_cluster: {
        addrs: targetAddrs,
        password: form.target_password
      },
      options: {
        ...form.options,
        skip_incremental: form.migration_mode === 'full_only'
      }
    }

    const result = await api.createTask(data)
    ElMessage.success('任务创建成功')
    
    // 获取任务ID，支持 task_id 或 id 两种格式
    const taskId = result.task_id || result.id
    if (taskId) {
      router.push(`/tasks/${taskId}`)
    } else {
      // 如果没有返回ID，跳转到任务列表
      router.push('/tasks')
    }
  } catch (err) {
    ElMessage.error('创建失败: ' + (err.message || '未知错误'))
  } finally {
    submitting.value = false
  }
}
</script>

<style lang="scss" scoped>
.create-task {
  max-width: 900px;
  margin: 0 auto;
}

.page-header {
  display: flex;
  align-items: center;
  gap: 16px;
  margin-bottom: 24px;
  
  h1 {
    font-size: 24px;
    font-weight: 700;
    color: var(--text-primary);
  }
}

.card {
  background: var(--bg-card);
  border-radius: var(--radius-lg);
  padding: 32px;
  box-shadow: var(--shadow-card);
  border: 1px solid var(--border-light);
}

.form-section {
  margin-bottom: 32px;
  
  h3 {
    display: flex;
    align-items: center;
    gap: 8px;
    font-size: 16px;
    font-weight: 600;
    color: var(--text-primary);
    margin-bottom: 20px;
    padding-bottom: 12px;
    border-bottom: 1px solid var(--border-light);
  }
  
  h4 {
    font-size: 14px;
    font-weight: 500;
    color: var(--text-secondary);
    margin: 24px 0 16px;
  }
  
  .el-radio-group {
    display: flex;
    flex-direction: column;
    gap: 16px;
    
    .el-radio {
      display: flex;
      align-items: flex-start;
      height: auto;
      padding: 16px;
      background: var(--bg-primary);
      border-radius: var(--radius-md);
      border: 2px solid transparent;
      transition: all 0.2s;
      
      &.is-checked {
        border-color: var(--primary-color);
        background: var(--primary-lighter);
      }
      
      :deep(.el-radio__input) {
        margin-top: 2px;
      }
      
      :deep(.el-radio__label) {
        display: flex;
        flex-direction: column;
        gap: 4px;
        white-space: normal;
        
        .mode-label {
          font-weight: 600;
          color: var(--text-primary);
        }
        
        .mode-desc {
          font-size: 12px;
          color: var(--text-secondary);
        }
      }
    }
  }
}

.addrs-input {
  display: flex;
  flex-direction: column;
  gap: 12px;
  
  .el-input {
    width: 100%;
  }
}

.form-tip {
  font-size: 12px;
  color: var(--text-tertiary);
  margin-top: 4px;
}

.advanced-options {
  padding: 16px;
  background: var(--bg-primary);
  border-radius: var(--radius-md);
  margin-top: 16px;
}

.rate-limit-section {
  margin-top: 24px;
  padding-top: 24px;
  border-top: 1px dashed var(--border-light);
}

.filter-options {
  padding: 16px;
  background: var(--bg-primary);
  border-radius: var(--radius-md);
  margin-top: 12px;
}

.prefix-input {
  display: flex;
  flex-wrap: wrap;
  align-items: center;
  gap: 8px;
}

.form-actions {
  display: flex;
  justify-content: flex-end;
  gap: 16px;
  padding-top: 24px;
  border-top: 1px solid var(--border-light);
}

.test-result {
  margin-left: 12px;
  font-size: 13px;
  
  &.success {
    color: var(--el-color-success);
  }
  
  &.error {
    color: var(--el-color-danger);
  }
}

.cluster-info {
  background: var(--bg-primary);
  border-radius: var(--radius-md);
  padding: 16px;
  margin-top: 12px;
  
  .info-row {
    display: flex;
    align-items: center;
    padding: 6px 0;
    font-size: 13px;
    
    &:not(:last-child) {
      border-bottom: 1px dashed var(--border-light);
    }
    
    .label {
      width: 80px;
      color: var(--text-secondary);
      flex-shrink: 0;
    }
  }
}

.form-actions {
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding-top: 24px;
  border-top: 1px solid var(--border-light);
  
  .left-actions {
    display: flex;
    gap: 12px;
  }
  
  .right-actions {
    display: flex;
    gap: 12px;
  }
}

.empty-templates {
  text-align: center;
  padding: 40px 0;
  color: var(--text-secondary);
}

.recommend-result {
  margin-bottom: 20px;
  
  .recommend-info {
    margin-top: 8px;
    
    .recommend-row {
      display: flex;
      align-items: flex-start;
      padding: 4px 0;
      font-size: 13px;
      
      .label {
        width: 100px;
        color: var(--text-secondary);
        flex-shrink: 0;
      }
      
      .value {
        color: var(--text-primary);
        
        &.highlight {
          color: var(--el-color-success);
          font-weight: 600;
          font-size: 15px;
        }
        
        &.reason {
          color: var(--text-secondary);
          font-size: 12px;
          line-height: 1.5;
        }
      }
    }
  }
}
</style>
