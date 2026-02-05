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
          <h3>
            <el-icon><Document /></el-icon> 基本信息
            <el-dropdown @command="handleTemplateCommand" trigger="click" style="margin-left: auto;">
              <el-button size="small">
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
          </h3>
          
          <el-form-item label="任务名称" prop="name">
            <el-input 
              v-model="form.name" 
              placeholder="请输入任务名称，如：生产环境数据迁移"
              maxlength="100"
              show-word-limit
            />
          </el-form-item>
          
          <el-form-item label="迁移模式" prop="migration_mode">
            <div class="migration-mode-wrapper">
              <el-radio-group v-model="form.migration_mode" class="migration-mode-compact">
                <el-radio value="full_only">全量迁移</el-radio>
                <el-radio value="incremental_only">增量迁移</el-radio>
                <el-radio value="full_and_incremental">全量+增量迁移</el-radio>
              </el-radio-group>
            </div>
            <div class="migration-mode-desc">
              <template v-if="form.migration_mode === 'full_only'">
                仅执行全量数据迁移，适用于源端数据不再变化的场景
              </template>
              <template v-else-if="form.migration_mode === 'incremental_only'">
                仅读取 Binlog 进行增量同步，适用于已完成全量迁移或只需同步增量的场景
              </template>
              <template v-else>
                先全量迁移，再持续同步增量数据，适用于在线迁移场景
              </template>
            </div>
          </el-form-item>
        </div>
        
        <!-- 集群配置 - 双列布局 -->
        <div class="form-section">
          <h3><el-icon><Connection /></el-icon> 集群配置</h3>
          
          <el-row :gutter="20">
            <!-- 源集群 -->
            <el-col :span="12">
              <div class="cluster-config-card source">
                <div class="cluster-title">
                  <el-icon><ArrowRight /></el-icon> 源集群
                </div>
                
                <el-form-item label="集群地址" prop="source_addrs">
                  <div class="addrs-input">
                    <el-input
                      v-for="(addr, index) in form.source_addrs"
                      :key="'source-' + index"
                      v-model="form.source_addrs[index]"
                      placeholder="如: 127.0.0.1:6379"
                      size="small"
                    >
                      <template #append v-if="form.source_addrs.length > 1">
                        <el-button @click="removeAddr('source', index)" size="small">
                          <el-icon><Delete /></el-icon>
                        </el-button>
                      </template>
                    </el-input>
                    <el-button text type="primary" size="small" @click="addAddr('source')">
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
                    size="small"
                  />
                </el-form-item>
                
                <div class="test-connection-row">
                  <el-button 
                    type="success" 
                    plain 
                    size="small"
                    @click="testConnection('source')"
                    :loading="testingSource"
                  >
                    <el-icon><Connection /></el-icon> 测试连接
                  </el-button>
                  <span v-if="sourceTestResult" :class="['test-result', sourceTestResult.success ? 'success' : 'error']">
                    {{ sourceTestResult.success ? '✓' : '✗' }}
                    <span v-if="sourceTestResult.latency_ms">({{ sourceTestResult.latency_ms }}ms)</span>
                  </span>
                </div>
                
                <!-- 源集群信息展示 -->
                <div v-if="sourceTestResult?.cluster_info" class="cluster-info compact">
                  <div class="info-grid">
                    <span class="info-item">
                      <el-tag :type="sourceTestResult.cluster_info.mode === 'cluster' ? 'success' : 'info'" size="small">
                        {{ sourceTestResult.cluster_info.mode === 'cluster' ? '集群' : '单机' }}
                      </el-tag>
                    </span>
                    <span class="info-item">v{{ sourceTestResult.cluster_info.version }}</span>
                    <span class="info-item">{{ sourceTestResult.cluster_info.node_count }}节点</span>
                    <span class="info-item">{{ formatNumber(sourceTestResult.cluster_info.total_keys) }} keys</span>
                    <span class="info-item">{{ formatBytes(sourceTestResult.cluster_info.total_memory) }}</span>
                  </div>
                </div>
              </div>
            </el-col>
            
            <!-- 目标集群 -->
            <el-col :span="12">
              <div class="cluster-config-card target">
                <div class="cluster-title">
                  <el-icon><ArrowRight /></el-icon> 目标集群
                </div>
                
                <el-form-item label="集群地址" prop="target_addrs">
                  <div class="addrs-input">
                    <el-input
                      v-for="(addr, index) in form.target_addrs"
                      :key="'target-' + index"
                      v-model="form.target_addrs[index]"
                      placeholder="如: 127.0.0.1:6379"
                      size="small"
                    >
                      <template #append v-if="form.target_addrs.length > 1">
                        <el-button @click="removeAddr('target', index)" size="small">
                          <el-icon><Delete /></el-icon>
                        </el-button>
                      </template>
                    </el-input>
                    <el-button text type="primary" size="small" @click="addAddr('target')">
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
                    size="small"
                  />
                </el-form-item>
                
                <div class="test-connection-row">
                  <el-button 
                    type="success" 
                    plain 
                    size="small"
                    @click="testConnection('target')"
                    :loading="testingTarget"
                  >
                    <el-icon><Connection /></el-icon> 测试连接
                  </el-button>
                  <span v-if="targetTestResult" :class="['test-result', targetTestResult.success ? 'success' : 'error']">
                    {{ targetTestResult.success ? '✓' : '✗' }}
                    <span v-if="targetTestResult.latency_ms">({{ targetTestResult.latency_ms }}ms)</span>
                  </span>
                </div>
                
                <!-- 目标集群信息展示 -->
                <div v-if="targetTestResult?.cluster_info" class="cluster-info compact">
                  <div class="info-grid">
                    <span class="info-item">
                      <el-tag :type="targetTestResult.cluster_info.mode === 'cluster' ? 'success' : 'info'" size="small">
                        {{ targetTestResult.cluster_info.mode === 'cluster' ? '集群' : '单机' }}
                      </el-tag>
                    </span>
                    <span class="info-item">v{{ targetTestResult.cluster_info.version }}</span>
                    <span class="info-item">{{ targetTestResult.cluster_info.node_count }}节点</span>
                    <span class="info-item">{{ formatNumber(targetTestResult.cluster_info.total_keys) }} keys</span>
                    <span class="info-item">{{ formatBytes(targetTestResult.cluster_info.total_memory) }}</span>
                  </div>
                </div>
              </div>
            </el-col>
          </el-row>
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
                  <el-option label="keylist - 按Key清单迁移" value="keylist" />
                </el-select>
                <div class="form-tip">选择如何筛选要迁移的Key</div>
              </el-form-item>
            </el-col>
          </el-row>
          
          <!-- Key 清单上传（仅在 keylist 模式下显示） -->
          <div v-if="form.options.key_filter.mode === 'keylist'" class="filter-options keylist-section">
            <el-form-item label="Key清单文件">
              <el-upload
                ref="keyListUploadRef"
                :auto-upload="false"
                :limit="1"
                accept=".txt,.csv,.json"
                :on-change="handleKeyListFileChange"
                :on-remove="handleKeyListFileRemove"
                :file-list="keyListFileList"
                drag
                class="keylist-upload"
              >
                <el-icon class="el-icon--upload"><Upload /></el-icon>
                <div class="el-upload__text">
                  拖拽文件到此处，或<em>点击上传</em>
                </div>
                <template #tip>
                  <div class="el-upload__tip">
                    支持 TXT（每行一个Key）、CSV（第一列为Key）、JSON（数组格式）
                  </div>
                </template>
              </el-upload>
            </el-form-item>
            
            <!-- Key清单预览 -->
            <div class="keylist-preview" v-if="keyListPreview.totalCount > 0">
              <div class="preview-header">
                <span class="preview-title">
                  <el-icon><Document /></el-icon>
                  已解析 {{ keyListPreview.totalCount }} 个Key
                </span>
                <el-button text type="primary" @click="showKeyListPreviewDialog = true">
                  预览全部
                </el-button>
              </div>
              <div class="preview-keys">
                <el-tag 
                  v-for="(key, idx) in keyListPreview.sampleKeys" 
                  :key="idx"
                  type="info"
                  size="small"
                  class="preview-key-tag"
                >
                  {{ key }}
                </el-tag>
                <el-tag v-if="keyListPreview.totalCount > 5" type="info" size="small">
                  +{{ keyListPreview.totalCount - 5 }} 更多...
                </el-tag>
              </div>
            </div>
            
            <el-form-item label="或直接输入Key列表">
              <el-input
                v-model="keyListText"
                type="textarea"
                :rows="4"
                placeholder="每行一个Key，例如：&#10;user:1001&#10;user:1002&#10;order:12345"
                @blur="parseKeyListText"
              />
              <div class="form-tip">支持直接粘贴Key列表，每行一个</div>
            </el-form-item>
          </div>
          
          <div v-if="form.options.key_filter.mode === 'prefix'" class="filter-options">
            <el-row :gutter="16">
              <el-col :span="12">
                <el-form-item label="迁移前缀（留空则迁移所有）">
                  <div class="prefix-input">
                    <el-tag
                      v-for="(prefix, index) in form.options.key_filter.prefixes"
                      :key="'prefix-' + index"
                      closable
                      type="success"
                      size="small"
                      @close="removePrefix('prefixes', index)"
                    >
                      {{ prefix }}
                    </el-tag>
                    <el-input
                      v-model="newPrefix"
                      placeholder="如: user:"
                      style="width: 140px"
                      size="small"
                      @keyup.enter="addPrefix('prefixes')"
                    >
                      <template #append>
                        <el-button @click="addPrefix('prefixes')" size="small">添加</el-button>
                      </template>
                    </el-input>
                  </div>
                </el-form-item>
              </el-col>
              <el-col :span="12">
                <el-form-item label="排除前缀（跳过这些Key）">
                  <div class="prefix-input">
                    <el-tag
                      v-for="(prefix, index) in form.options.key_filter.exclude_prefixes"
                      :key="'exclude-' + index"
                      closable
                      type="danger"
                      size="small"
                      @close="removePrefix('exclude_prefixes', index)"
                    >
                      {{ prefix }}
                    </el-tag>
                    <el-input
                      v-model="newExcludePrefix"
                      placeholder="如: temp:"
                      style="width: 140px"
                      size="small"
                      @keyup.enter="addPrefix('exclude_prefixes')"
                    >
                      <template #append>
                        <el-button @click="addPrefix('exclude_prefixes')" size="small">添加</el-button>
                      </template>
                    </el-input>
                  </div>
                </el-form-item>
              </el-col>
            </el-row>
          </div>
          
          <div v-if="form.options.key_filter.mode === 'pattern'" class="filter-options">
            <el-form-item label="正则模式（匹配的Key才会被迁移）">
              <div class="prefix-input">
                <el-tag
                  v-for="(pattern, index) in form.options.key_filter.patterns"
                  :key="'pattern-' + index"
                  closable
                  size="small"
                  @close="removePrefix('patterns', index)"
                >
                  {{ pattern }}
                </el-tag>
                <el-input
                  v-model="newPattern"
                  placeholder="输入正则后按回车添加，如: ^user:\d+$"
                  style="width: 260px"
                  size="small"
                  @keyup.enter="addPrefix('patterns')"
                >
                  <template #append>
                    <el-button @click="addPrefix('patterns')" size="small">添加</el-button>
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
              @click="showHardwareDialog = true"
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
                    <span class="label">推荐批次大小:</span>
                    <span class="value">{{ formatNumber(recommendedConfig.scan_batch_size) }}</span>
                  </div>
                  <div class="recommend-row">
                    <span class="label">推荐连接数:</span>
                    <span class="value">源端 {{ recommendedConfig.source_connections }}，目标端 {{ recommendedConfig.target_connections }}</span>
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
            <!-- 第一行：Worker数量、批次大小、冲突策略、大Key阈值 -->
            <el-row :gutter="16">
              <el-col :span="6">
                <el-form-item>
                  <template #label>
                    <span>Worker数量</span>
                    <el-tooltip placement="top" effect="dark">
                      <template #content>
                        <div style="max-width: 280px; line-height: 1.5;">
                          <p><strong>系统支持：1-1024</strong></p>
                          <p style="margin-top: 6px;">• 小规模(&lt;1000万Key): 8-16</p>
                          <p>• 中等规模(1000万-1亿): 16-64</p>
                          <p>• 大规模(&gt;1亿): 64-256</p>
                        </div>
                      </template>
                      <el-icon style="margin-left: 4px; color: #909399; cursor: help;"><QuestionFilled /></el-icon>
                    </el-tooltip>
                  </template>
                  <el-input-number 
                    v-model="form.options.worker_count" 
                    :min="1" 
                    :max="1024"
                    style="width: 100%"
                    size="small"
                  />
                </el-form-item>
              </el-col>
              
              <el-col :span="6">
                <el-form-item>
                  <template #label>
                    <span>批次大小</span>
                    <el-tooltip placement="top" effect="dark">
                      <template #content>
                        <div style="max-width: 280px; line-height: 1.5;">
                          <p><strong>系统支持：100-100000</strong></p>
                          <p style="margin-top: 6px;">• 小Key(&lt;1KB)：5000-10000</p>
                          <p>• 中等Key(1-10KB)：1000-5000</p>
                          <p>• 大Key(&gt;10KB)：500-1000</p>
                        </div>
                      </template>
                      <el-icon style="margin-left: 4px; color: #909399; cursor: help;"><QuestionFilled /></el-icon>
                    </el-tooltip>
                  </template>
                  <el-input-number 
                    v-model="form.options.scan_batch_size" 
                    :min="100" 
                    :max="100000"
                    :step="1000"
                    style="width: 100%"
                    size="small"
                  />
                </el-form-item>
              </el-col>
              
              <el-col :span="6">
                <el-form-item label="冲突策略">
                  <el-select v-model="form.options.conflict_policy" style="width: 100%" size="small">
                    <el-option label="全量跳过增量覆盖" value="skip_full_only" />
                    <el-option label="跳过并记录" value="skip" />
                    <el-option label="直接覆盖" value="replace" />
                    <el-option label="遇冲突报错" value="error" />
                  </el-select>
                </el-form-item>
              </el-col>
              
              <el-col :span="6">
                <el-form-item label="大Key阈值">
                  <el-select v-model="form.options.large_key_threshold" style="width: 100%" size="small">
                    <el-option label="1 MB" :value="1048576" />
                    <el-option label="10 MB" :value="10485760" />
                    <el-option label="50 MB" :value="52428800" />
                    <el-option label="100 MB" :value="104857600" />
                  </el-select>
                </el-form-item>
              </el-col>
            </el-row>
            
            <!-- 限速配置：4个参数一行 -->
            <div class="rate-limit-section">
              <h4>限速配置</h4>
              
              <el-row :gutter="16">
                <el-col :span="6">
                  <el-form-item label="源端QPS限制">
                    <el-input-number 
                      v-model="form.options.rate_limit.source_qps" 
                      :min="0" 
                      :max="100000"
                      :step="1000"
                      style="width: 100%"
                      size="small"
                    />
                    <div class="form-tip">0表示不限制</div>
                  </el-form-item>
                </el-col>
                
                <el-col :span="6">
                  <el-form-item label="目标端QPS限制">
                    <el-input-number 
                      v-model="form.options.rate_limit.target_qps" 
                      :min="0" 
                      :max="100000"
                      :step="1000"
                      style="width: 100%"
                      size="small"
                    />
                    <div class="form-tip">0表示不限制</div>
                  </el-form-item>
                </el-col>
                
                <el-col :span="6">
                  <el-form-item>
                    <template #label>
                      <span>源端连接数</span>
                      <el-tooltip placement="top" effect="dark">
                        <template #content>
                          <div style="max-width: 240px; line-height: 1.5;">
                            <p><strong>系统支持：1-5000</strong></p>
                            <p style="margin-top: 6px;">推荐值：Worker数 × 3</p>
                          </div>
                        </template>
                        <el-icon style="margin-left: 4px; color: #909399; cursor: help;"><QuestionFilled /></el-icon>
                      </el-tooltip>
                    </template>
                    <el-input-number 
                      v-model="form.options.rate_limit.source_connections" 
                      :min="1" 
                      :max="5000"
                      style="width: 100%"
                      size="small"
                    />
                  </el-form-item>
                </el-col>
                
                <el-col :span="6">
                  <el-form-item>
                    <template #label>
                      <span>目标端连接数</span>
                      <el-tooltip placement="top" effect="dark">
                        <template #content>
                          <div style="max-width: 240px; line-height: 1.5;">
                            <p><strong>系统支持：1-5000</strong></p>
                            <p style="margin-top: 6px;">推荐值：Worker数 × 3</p>
                          </div>
                        </template>
                        <el-icon style="margin-left: 4px; color: #909399; cursor: help;"><QuestionFilled /></el-icon>
                      </el-tooltip>
                    </template>
                    <el-input-number 
                      v-model="form.options.rate_limit.target_connections" 
                      :min="1" 
                      :max="5000"
                      style="width: 100%"
                      size="small"
                    />
                  </el-form-item>
                </el-col>
              </el-row>
            </div>
            
            <!-- 其他配置：影子模式、启用压缩、重试配置 -->
            <div class="other-config-section">
              <h4>其他配置</h4>
              
              <el-row :gutter="16">
                <el-col :span="4">
                  <el-form-item>
                    <template #label>
                      <span>影子模式</span>
                      <el-tooltip placement="top" effect="dark">
                        <template #content>
                          <div style="max-width: 240px; line-height: 1.5;">
                            <p>只读取源端数据，不写入目标端</p>
                            <p style="margin-top: 6px;">用于验证筛选规则、性能预估</p>
                          </div>
                        </template>
                        <el-icon style="margin-left: 4px; color: #909399; cursor: help;"><QuestionFilled /></el-icon>
                      </el-tooltip>
                    </template>
                    <el-switch v-model="form.options.shadow_mode" />
                  </el-form-item>
                </el-col>
                <el-col :span="4">
                  <el-form-item label="启用压缩">
                    <el-switch v-model="form.options.enable_compression" />
                  </el-form-item>
                </el-col>
                <el-col :span="5">
                  <el-form-item label="最大重试次数">
                    <el-input-number 
                      v-model="form.options.retry_config.max_retries" 
                      :min="0" 
                      :max="10"
                      style="width: 100%"
                      size="small"
                    />
                  </el-form-item>
                </el-col>
                <el-col :span="5">
                  <el-form-item label="全量重试间隔(ms)">
                    <el-input-number 
                      v-model="form.options.retry_config.full_retry_interval_ms" 
                      :min="50" 
                      :max="5000"
                      :step="50"
                      style="width: 100%"
                      size="small"
                    />
                  </el-form-item>
                </el-col>
                <el-col :span="6">
                  <el-form-item label="增量重试间隔(ms)">
                    <el-input-number 
                      v-model="form.options.retry_config.incr_retry_interval_ms" 
                      :min="100" 
                      :max="10000"
                      :step="100"
                      style="width: 100%"
                      size="small"
                    />
                  </el-form-item>
                </el-col>
              </el-row>
            </div>
          </div>
        </div>
        
        <!-- 提交按钮 -->
        <div class="form-actions">
          <div class="right-actions">
            <el-button @click="$router.push('/tasks')">取消</el-button>
            <el-button type="primary" @click="submitForm" :loading="submitting">
              创建任务
            </el-button>
          </div>
        </div>
      </el-form>
    </div>

    <!-- 硬件参数输入对话框（智能推荐前） -->
    <el-dialog v-model="showHardwareDialog" title="智能推荐配置" width="560px">
      <div class="hardware-dialog-tip">
        <el-alert type="info" :closable="false" show-icon>
          <template #title>填写以下信息可获得更精准的推荐配置</template>
          <template #default>
            <div style="font-size: 12px; color: #606266; margin-top: 4px;">
              系统会根据网络带宽、可用内存、Key 大小等因素，综合计算最优的 Worker 数量。
              如果不确定，可直接跳过使用默认值。
            </div>
          </template>
        </el-alert>
      </div>
      <el-form :model="hardwareForm" label-width="120px" style="margin-top: 20px;">
        <el-form-item label="大部分 Key 大小">
          <el-select v-model="hardwareForm.key_size_level" placeholder="请选择" style="width: 220px">
            <el-option label="小 Key（<1KB）- 推荐" value="small" />
            <el-option label="中 Key（1KB~10KB）" value="medium" />
            <el-option label="大 Key（10KB~100KB）" value="large" />
            <el-option label="超大 Key（>100KB）" value="xlarge" />
          </el-select>
          <div class="form-tip hardware-tip">指约 80% 以上 Key 的大小，少量大 Key 系统会自动单独处理</div>
        </el-form-item>
        <el-form-item label="网络带宽">
          <div class="hardware-input-row">
            <el-input-number 
              v-model="hardwareForm.bandwidth_mbps" 
              :min="0" 
              :max="100000"
              :step="100"
              placeholder="Mbps"
              style="width: 140px"
            />
            <span class="hardware-unit">Mbps</span>
          </div>
          <div class="form-tip hardware-tip">常见：百兆=100，千兆=1000，万兆=10000</div>
        </el-form-item>
        <el-form-item label="可用内存">
          <div class="hardware-input-row">
            <el-input-number 
              v-model="hardwareForm.memory_gb" 
              :min="0" 
              :max="1024"
              :step="1"
              placeholder="GB"
              style="width: 140px"
            />
            <span class="hardware-unit">GB</span>
          </div>
          <div class="form-tip hardware-tip">迁移工具部署机器的可用内存</div>
        </el-form-item>
      </el-form>
      <template #footer>
        <el-button @click="showHardwareDialog = false">取消</el-button>
        <el-button @click="getRecommendedConfigWithHardware(true)">跳过，使用默认值</el-button>
        <el-button type="primary" @click="getRecommendedConfigWithHardware(false)">
          <el-icon><MagicStick /></el-icon> 开始推荐
        </el-button>
      </template>
    </el-dialog>

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
    <el-dialog v-model="loadTemplateDialog" title="从模板加载" width="800px">
      <el-table :data="templateList" v-loading="loadingTemplates" style="width: 100%" max-height="400">
        <el-table-column prop="name" label="模板名称" min-width="120" />
        <el-table-column prop="description" label="描述" min-width="200" show-overflow-tooltip />
        <el-table-column prop="migration_mode" label="迁移模式" width="120" align="center">
          <template #default="{ row }">
            <el-tag :type="row.migration_mode === 'full_only' ? 'info' : (row.migration_mode === 'incremental_only' ? 'warning' : 'success')" size="small">
              {{ row.migration_mode === 'full_only' ? '全量' : (row.migration_mode === 'incremental_only' ? '增量' : '全量+增量') }}
            </el-tag>
          </template>
        </el-table-column>
        <el-table-column prop="created_at" label="创建时间" width="170" align="center">
          <template #default="{ row }">
            {{ formatTime(row.created_at) }}
          </template>
        </el-table-column>
        <el-table-column label="操作" width="140" align="center" fixed="right">
          <template #default="{ row }">
            <el-button type="primary" size="small" @click="loadFromTemplate(row)">
              <el-icon><Check /></el-icon> 加载
            </el-button>
            <el-button type="danger" size="small" text @click="deleteTemplateConfirm(row)">删除</el-button>
          </template>
        </el-table-column>
      </el-table>
      <div v-if="templateList.length === 0 && !loadingTemplates" class="empty-templates">
        暂无保存的模板
      </div>
    </el-dialog>

    <!-- Key清单预览对话框 -->
    <el-dialog v-model="showKeyListPreviewDialog" title="Key清单预览" width="600px">
      <div class="keylist-full-preview">
        <div class="preview-stats">
          <span>总计 {{ keyListPreview.totalCount }} 个Key</span>
          <span v-if="keyListPreview.format">格式: {{ keyListPreview.format.toUpperCase() }}</span>
        </div>
        <div class="preview-list">
          <div 
            v-for="(key, idx) in keyListPreview.keys" 
            :key="idx"
            class="preview-list-item"
          >
            <span class="key-index">{{ idx + 1 }}</span>
            <span class="key-value">{{ key }}</span>
          </div>
        </div>
      </div>
      <template #footer>
        <el-button @click="showKeyListPreviewDialog = false">关闭</el-button>
        <el-button type="danger" @click="clearKeyList">清空清单</el-button>
      </template>
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
const showHardwareDialog = ref(false)
const hardwareForm = reactive({
  key_size_level: 'small',  // small, medium, large, xlarge
  bandwidth_mbps: 0,
  memory_gb: 0
})

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

// Key清单相关
const keyListUploadRef = ref(null)
const keyListFileList = ref([])
const keyListText = ref('')
const showKeyListPreviewDialog = ref(false)
const keyListPreview = reactive({
  keys: [],
  sampleKeys: [],
  totalCount: 0,
  format: ''
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
    shadow_mode: false,
    key_filter: {
      mode: 'all',
      prefixes: [],
      exclude_prefixes: [],
      patterns: []
    },
    key_list: [],  // Key 清单（keylist 模式使用）
    rate_limit: {
      source_qps: 10000,
      source_connections: 50,
      target_qps: 10000,
      target_connections: 50,
      pipeline_size: 100,
      pipeline_timeout_ms: 5000,
      max_bandwidth_mbps: 0
    },
    retry_config: {
      max_retries: 3,
      full_retry_interval_ms: 100,
      incr_retry_interval_ms: 1000
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

const loadFromTemplate = async (template) => {
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
    if (template.options.retry_config) {
      form.options.retry_config = { ...template.options.retry_config }
    }
  }
  
  // 自动生成任务名称
  form.name = template.name + '-' + new Date().toLocaleString('zh-CN', { 
    month: '2-digit', day: '2-digit', hour: '2-digit', minute: '2-digit' 
  }).replace(/[\/\s:]/g, '')
  
  loadTemplateDialog.value = false
  ElMessage.success('已加载模板配置，正在测试连接...')
  
  // 自动测试连接
  const sourceAddrs = form.source_addrs.filter(a => a.trim())
  const targetAddrs = form.target_addrs.filter(a => a.trim())
  
  // 并行测试源端和目标端
  const testPromises = []
  
  if (sourceAddrs.length > 0) {
    testPromises.push(
      (async () => {
        testingSource.value = true
        sourceTestResult.value = null
        try {
          const result = await api.testConnection({ addrs: sourceAddrs, password: form.source_password })
          sourceTestResult.value = result
        } catch (err) {
          sourceTestResult.value = {
            success: false,
            message: '测试失败: ' + (err.message || '未知错误')
          }
        } finally {
          testingSource.value = false
        }
      })()
    )
  }
  
  if (targetAddrs.length > 0) {
    testPromises.push(
      (async () => {
        testingTarget.value = true
        targetTestResult.value = null
        try {
          const result = await api.testConnection({ addrs: targetAddrs, password: form.target_password })
          targetTestResult.value = result
        } catch (err) {
          targetTestResult.value = {
            success: false,
            message: '测试失败: ' + (err.message || '未知错误')
          }
        } finally {
          testingTarget.value = false
        }
      })()
    )
  }
  
  // 等待所有测试完成
  if (testPromises.length > 0) {
    await Promise.all(testPromises)
    
    // 显示测试结果
    const sourceOk = sourceTestResult.value?.success
    const targetOk = targetTestResult.value?.success
    
    if (sourceOk && targetOk) {
      ElMessage.success('源端和目标端连接测试通过')
    } else if (!sourceOk && !targetOk) {
      ElMessage.warning('源端和目标端连接测试均失败，请检查配置')
    } else if (!sourceOk) {
      ElMessage.warning('源端连接测试失败，请检查配置')
    } else {
      ElMessage.warning('目标端连接测试失败，请检查配置')
    }
  }
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

// 获取推荐配置（支持硬件参数）
const getRecommendedConfigWithHardware = async (skipHardware = false) => {
  const sourceAddrs = form.source_addrs.filter(a => a.trim())
  const targetAddrs = form.target_addrs.filter(a => a.trim())
  
  if (sourceAddrs.length === 0 || targetAddrs.length === 0) {
    ElMessage.warning('请先完成源端和目标端连接测试')
    return
  }
  
  showHardwareDialog.value = false
  loadingRecommend.value = true
  recommendedConfig.value = null
  
  try {
    const requestData = {
      source_cluster: {
        addrs: sourceAddrs,
        password: form.source_password
      },
      target_cluster: {
        addrs: targetAddrs,
        password: form.target_password
      }
    }
    
    // 如果用户填写了硬件参数，添加到请求中
    if (!skipHardware) {
      const hw = hardwareForm
      if (hw.bandwidth_mbps > 0 || hw.memory_gb > 0 || hw.key_size_level) {
        requestData.hardware_info = {
          bandwidth_mbps: hw.bandwidth_mbps || 0,
          memory_gb: hw.memory_gb || 0,
          key_size_level: hw.key_size_level || 'small'  // small, medium, large, xlarge
        }
      }
    }
    
    const result = await api.getRecommendedConfig(requestData)
    recommendedConfig.value = result.recommended
    showAdvanced.value = true
    ElMessage.success('推荐配置已生成')
  } catch (err) {
    ElMessage.error('获取推荐配置失败: ' + (err.message || '未知错误'))
  } finally {
    loadingRecommend.value = false
  }
}

// 获取推荐配置（旧版兼容）
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

// Key清单文件变更处理
const handleKeyListFileChange = async (file) => {
  if (!file.raw) return
  
  const reader = new FileReader()
  reader.onload = (e) => {
    const content = e.target.result
    parseKeyListContent(content, file.name)
  }
  reader.readAsText(file.raw)
}

// 解析Key清单内容
const parseKeyListContent = (content, fileName = '') => {
  let keys = []
  let format = 'txt'
  
  // 检测格式
  const lowerName = fileName.toLowerCase()
  if (lowerName.endsWith('.json')) {
    format = 'json'
    try {
      const parsed = JSON.parse(content)
      if (Array.isArray(parsed)) {
        keys = parsed.map(item => {
          if (typeof item === 'string') return item
          return item.key || item.name || item.Key || ''
        }).filter(k => k)
      }
    } catch (e) {
      ElMessage.error('JSON 格式解析失败')
      return
    }
  } else if (lowerName.endsWith('.csv')) {
    format = 'csv'
    const lines = content.split('\n')
    lines.forEach((line, idx) => {
      const fields = line.split(',')
      if (fields.length > 0) {
        let key = fields[0].trim().replace(/^["']|["']$/g, '')
        // 跳过标题行
        if (idx === 0 && ['key', 'name', 'redis_key'].includes(key.toLowerCase())) {
          return
        }
        if (key) keys.push(key)
      }
    })
  } else {
    format = 'txt'
    keys = content.split('\n')
      .map(line => line.trim())
      .filter(line => line && !line.startsWith('#') && !line.startsWith('//'))
  }
  
  // 去重
  const uniqueKeys = [...new Set(keys)]
  
  keyListPreview.keys = uniqueKeys
  keyListPreview.sampleKeys = uniqueKeys.slice(0, 5)
  keyListPreview.totalCount = uniqueKeys.length
  keyListPreview.format = format
  
  // 更新表单数据
  form.options.key_list = uniqueKeys
  
  if (uniqueKeys.length > 0) {
    ElMessage.success(`成功解析 ${uniqueKeys.length} 个Key`)
  }
}

// 移除Key清单文件
const handleKeyListFileRemove = () => {
  clearKeyList()
}

// 清空Key清单
const clearKeyList = () => {
  keyListPreview.keys = []
  keyListPreview.sampleKeys = []
  keyListPreview.totalCount = 0
  keyListPreview.format = ''
  keyListFileList.value = []
  keyListText.value = ''
  form.options.key_list = []
  showKeyListPreviewDialog.value = false
}

// 解析文本输入的Key清单
const parseKeyListText = () => {
  if (!keyListText.value.trim()) return
  parseKeyListContent(keyListText.value, 'input.txt')
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
        skip_full_sync: form.migration_mode === 'incremental_only',
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
  max-width: 1100px;
  margin: 0 auto;
}

.page-header {
  display: flex;
  align-items: center;
  gap: 16px;
  margin-bottom: 20px;
  
  h1 {
    font-size: 22px;
    font-weight: 600;
    color: var(--text-primary);
  }
}

.card {
  background: var(--bg-card);
  border-radius: var(--radius-md);
  padding: 28px 32px;
  box-shadow: var(--shadow-card);
  border: 1px solid var(--border-light);
}

.form-section {
  margin-bottom: 24px;
  
  &:last-of-type {
    margin-bottom: 0;
  }
  
  h3 {
    display: flex;
    align-items: center;
    gap: 8px;
    font-size: 16px;
    font-weight: 600;
    color: var(--text-primary);
    margin-bottom: 16px;
    padding-bottom: 10px;
    border-bottom: 1px solid var(--border-light);
  }
  
  h4 {
    font-size: 14px;
    font-weight: 500;
    color: var(--text-secondary);
    margin: 12px 0 10px;
  }
  
  // 紧凑表单项
  :deep(.el-form-item) {
    margin-bottom: 16px;
    
    .el-form-item__label {
      padding-bottom: 6px;
      font-size: 14px;
      line-height: 1.4;
    }
  }
}

.addrs-input {
  display: flex;
  flex-direction: column;
  gap: 8px;
  
  .el-input {
    width: 100%;
  }
}

.form-tip {
  font-size: 12px;
  color: var(--text-tertiary);
  margin-top: 4px;
  line-height: 1.4;
}

/* 紧凑版迁移模式选择 */
.migration-mode-wrapper {
  width: 100%;
}

.migration-mode-compact {
  display: flex !important;
  flex-direction: row !important;
  gap: 20px !important;
  
  .el-radio {
    display: flex !important;
    align-items: center !important;
    height: auto !important;
    padding: 0 !important;
    background: transparent !important;
    border: none !important;
    
    &.is-checked {
      border: none !important;
      background: transparent !important;
    }
  }
}

/* 迁移模式说明文字 */
.migration-mode-desc {
  display: block;
  width: 100%;
  font-size: 11px;
  color: #f56c6c;
  margin-top: 8px;
  padding: 6px 10px;
  background: #fef0f0;
  border-radius: 4px;
  border-left: 2px solid #f56c6c;
  line-height: 1.4;
}

/* 硬件参数输入行 */
.hardware-input-row {
  display: flex;
  align-items: center;
  gap: 6px;
}

.hardware-unit {
  color: #909399;
  font-size: 13px;
  min-width: 36px;
}

/* 硬件参数提示文字 */
.hardware-tip {
  display: block;
  width: 100%;
  margin-top: 4px;
  color: #f56c6c;
}

.advanced-options {
  padding: 16px 18px;
  background: var(--bg-primary);
  border-radius: var(--radius-md);
  margin-top: 12px;
  
  :deep(.el-form-item) {
    margin-bottom: 12px;
    
    .el-form-item__label {
      padding-bottom: 4px;
      font-size: 13px;
      line-height: 1.4;
    }
  }
  
  :deep(.el-row) {
    margin-bottom: 8px;
    
    &:last-child {
      margin-bottom: 0;
    }
  }
}

.rate-limit-section {
  margin-top: 16px;
  padding-top: 16px;
  border-top: 1px dashed var(--border-light);
  
  h4 {
    margin: 0 0 12px !important;
  }
}

.other-config-section {
  margin-top: 16px;
  padding-top: 16px;
  border-top: 1px dashed var(--border-light);
  
  h4 {
    margin: 0 0 12px !important;
  }
}

.filter-options {
  padding: 16px 18px;
  background: var(--bg-primary);
  border-radius: var(--radius-md);
  margin-top: 10px;
  
  :deep(.el-form-item) {
    margin-bottom: 12px;
    
    &:last-child {
      margin-bottom: 0;
    }
    
    .el-form-item__label {
      padding-bottom: 4px;
      font-size: 13px;
      line-height: 1.4;
    }
  }
}

.prefix-input {
  display: flex;
  flex-wrap: wrap;
  align-items: center;
  gap: 4px 6px;
  
  .el-tag {
    margin-bottom: 0 !important;
  }
}

.form-actions {
  display: flex;
  justify-content: flex-end;
  align-items: center;
  padding-top: 20px;
  margin-top: 12px;
  border-top: 1px solid var(--border-light);
  
  .right-actions {
    display: flex;
    gap: 12px;
  }
}

.test-result {
  margin-left: 10px;
  font-size: 12px;
  
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
  padding: 10px 12px;
  margin-top: 8px;
  
  &.compact {
    padding: 8px;
    margin-top: 6px;
    
    .info-grid {
      display: flex;
      flex-wrap: wrap;
      gap: 6px 10px;
      font-size: 11px;
      
      .info-item {
        color: var(--text-secondary);
      }
    }
  }
  
  .info-row {
    display: flex;
    align-items: center;
    padding: 4px 0;
    font-size: 12px;
    
    &:not(:last-child) {
      border-bottom: 1px dashed var(--border-light);
    }
    
    .label {
      width: 70px;
      color: var(--text-secondary);
      flex-shrink: 0;
    }
  }
}

/* 集群配置卡片 */
.cluster-config-card {
  background: var(--bg-primary);
  border-radius: var(--radius-md);
  padding: 16px;
  border: 1px solid var(--border-light);
  
  &.source {
    border-left: 3px solid #409EFF;
  }
  
  &.target {
    border-left: 3px solid #67C23A;
  }
  
  .cluster-title {
    display: flex;
    align-items: center;
    gap: 6px;
    font-size: 15px;
    font-weight: 600;
    color: var(--text-primary);
    margin-bottom: 14px;
    padding-bottom: 8px;
    border-bottom: 1px dashed var(--border-light);
  }
  
  :deep(.el-form-item) {
    margin-bottom: 12px;
    
    .el-form-item__label {
      padding-bottom: 4px;
      font-size: 13px;
    }
  }
  
  .test-connection-row {
    display: flex;
    align-items: center;
    gap: 10px;
    margin-top: 6px;
  }
}

.empty-templates {
  text-align: center;
  padding: 30px 0;
  color: var(--text-secondary);
  font-size: 13px;
}

.recommend-result {
  margin-bottom: 12px;
  
  .recommend-info {
    margin-top: 6px;
    
    .recommend-row {
      display: flex;
      align-items: flex-start;
      padding: 3px 0;
      font-size: 12px;
      
      .label {
        width: 90px;
        color: var(--text-secondary);
        flex-shrink: 0;
      }
      
      .value {
        color: var(--text-primary);
        
        &.highlight {
          color: var(--el-color-success);
          font-weight: 600;
          font-size: 14px;
        }
        
        &.reason {
          color: var(--text-secondary);
          font-size: 11px;
          line-height: 1.4;
        }
      }
    }
  }
}

// Key清单上传样式
.keylist-section {
  .keylist-upload {
    width: 100%;
    
    :deep(.el-upload-dragger) {
      padding: 16px;
    }
  }
  
  .keylist-preview {
    background: var(--bg-primary);
    border-radius: var(--radius-md);
    padding: 12px;
    margin-top: 8px;
    
    .preview-header {
      display: flex;
      justify-content: space-between;
      align-items: center;
      margin-bottom: 8px;
      
      .preview-title {
        display: flex;
        align-items: center;
        gap: 4px;
        font-weight: 500;
        font-size: 13px;
        color: var(--text-primary);
      }
    }
    
    .preview-keys {
      display: flex;
      flex-wrap: wrap;
      gap: 6px;
      
      .preview-key-tag {
        font-family: 'Consolas', 'Monaco', monospace;
        max-width: 180px;
        overflow: hidden;
        text-overflow: ellipsis;
      }
    }
  }
}

// Key清单预览对话框样式
.keylist-full-preview {
  .preview-stats {
    display: flex;
    justify-content: space-between;
    padding: 10px 14px;
    background: var(--bg-primary);
    border-radius: var(--radius-sm);
    margin-bottom: 12px;
    font-size: 13px;
    color: var(--text-secondary);
  }
  
  .preview-list {
    max-height: 360px;
    overflow-y: auto;
    border: 1px solid var(--border-light);
    border-radius: var(--radius-sm);
    
    .preview-list-item {
      display: flex;
      align-items: center;
      padding: 6px 10px;
      border-bottom: 1px solid var(--border-light);
      
      &:last-child {
        border-bottom: none;
      }
      
      &:hover {
        background: var(--bg-hover);
      }
      
      .key-index {
        width: 36px;
        color: var(--text-tertiary);
        font-size: 11px;
      }
      
      .key-value {
        font-family: 'Consolas', 'Monaco', monospace;
        font-size: 12px;
        color: var(--text-primary);
        word-break: break-all;
      }
    }
  }
}
</style>
