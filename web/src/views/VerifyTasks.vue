<template>
  <div class="verify-tasks">
    <div class="page-header">
      <h1>数据校验</h1>
      <el-button type="primary" @click="showCreateDialog = true">
        <el-icon><Plus /></el-icon>
        创建校验任务
      </el-button>
    </div>

    <!-- 任务列表 -->
    <el-table :data="tasks" v-loading="loading" style="width: 100%">
      <el-table-column label="任务名称" min-width="180">
        <template #default="{ row }">
          <span class="task-name" @click="viewTask(row.id)">{{ row.name }}</span>
        </template>
      </el-table-column>
      
      <el-table-column label="校验模式" width="120">
        <template #default="{ row }">
          <el-tag :type="getModeType(row.verify_mode)" size="small">
            {{ getModeText(row.verify_mode) }}
          </el-tag>
        </template>
      </el-table-column>
      
      <el-table-column label="状态" width="100">
        <template #default="{ row }">
          <el-tag :type="getStatusType(row.status)" size="small">
            {{ getStatusText(row.status) }}
          </el-tag>
        </template>
      </el-table-column>
      
      <el-table-column label="进度" width="150">
        <template #default="{ row }">
          <el-progress 
            :percentage="row.result?.progress || 0" 
            :status="getProgressStatus(row.status)"
            :stroke-width="6"
          />
        </template>
      </el-table-column>
      
      <el-table-column label="一致性" width="100">
        <template #default="{ row }">
          <span 
            v-if="row.result?.sampled_keys > 0" 
            :class="['consistency', { high: row.result?.consistency_rate >= 99.9 }]"
          >
            {{ row.result?.consistency_rate?.toFixed(2) || 0 }}%
          </span>
          <span v-else class="no-data">-</span>
        </template>
      </el-table-column>
      
      <el-table-column label="源端Key数" width="120">
        <template #default="{ row }">
          {{ formatNumber(row.result?.source_key_count) || '-' }}
        </template>
      </el-table-column>
      
      <el-table-column label="目标端Key数" width="120">
        <template #default="{ row }">
          {{ formatNumber(row.result?.target_key_count) || '-' }}
        </template>
      </el-table-column>
      
      <el-table-column label="创建时间" width="170">
        <template #default="{ row }">
          {{ formatTime(row.created_at) }}
        </template>
      </el-table-column>
      
      <el-table-column label="操作" width="200" fixed="right">
        <template #default="{ row }">
          <el-button 
            v-if="row.status === 'pending'" 
            type="primary" 
            size="small" 
            @click="startTask(row.id)"
          >
            启动
          </el-button>
          <el-button 
            v-if="row.status === 'running'" 
            type="warning" 
            size="small" 
            @click="stopTask(row.id)"
          >
            停止
          </el-button>
          <el-button 
            size="small" 
            @click="viewTask(row.id)"
          >
            详情
          </el-button>
          <el-button 
            v-if="row.status !== 'running'"
            type="danger" 
            size="small" 
            @click="deleteTask(row.id)"
          >
            删除
          </el-button>
        </template>
      </el-table-column>
    </el-table>

    <!-- 创建校验任务对话框 -->
    <el-dialog 
      v-model="showCreateDialog" 
      title="创建校验任务" 
      width="700px"
      destroy-on-close
    >
      <el-form :model="createForm" label-width="120px">
        <el-form-item label="任务名称">
          <el-input v-model="createForm.name" placeholder="可选，留空自动生成" />
        </el-form-item>
        
        <el-divider content-position="left">源端集群</el-divider>
        
        <el-form-item label="地址" required>
          <el-input 
            v-model="createForm.source_addrs" 
            placeholder="多个地址用逗号分隔，如：10.0.0.1:6379,10.0.0.2:6379"
          />
        </el-form-item>
        <el-form-item label="密码">
          <el-input 
            v-model="createForm.source_password" 
            type="password" 
            show-password
            placeholder="无密码留空"
          />
        </el-form-item>
        
        <el-divider content-position="left">目标端集群</el-divider>
        
        <el-form-item label="地址" required>
          <el-input 
            v-model="createForm.target_addrs" 
            placeholder="多个地址用逗号分隔"
          />
        </el-form-item>
        <el-form-item label="密码">
          <el-input 
            v-model="createForm.target_password" 
            type="password" 
            show-password
            placeholder="无密码留空"
          />
        </el-form-item>
        
        <el-divider content-position="left">校验配置</el-divider>
        
        <el-form-item label="校验模式">
          <el-radio-group v-model="createForm.verify_mode">
            <el-radio value="count_only">仅统计数量</el-radio>
            <el-radio value="sample">采样校验</el-radio>
            <el-radio value="full">全量校验</el-radio>
          </el-radio-group>
        </el-form-item>
        
        <el-form-item label="采样率" v-if="createForm.verify_mode === 'sample'">
          <el-slider 
            v-model="createForm.sample_rate_percent" 
            :min="0.1" 
            :max="100" 
            :step="0.1"
            :format-tooltip="(val) => `${val}%`"
          />
          <span class="hint">{{ createForm.sample_rate_percent }}%</span>
        </el-form-item>
        
        <el-form-item label="最大Key数" v-if="createForm.verify_mode !== 'count_only'">
          <el-input-number 
            v-model="createForm.max_keys" 
            :min="100" 
            :max="10000000"
            :step="10000"
          />
          <span class="hint">最多校验的 Key 数量</span>
        </el-form-item>
        
        <el-form-item label="比较模式" v-if="createForm.verify_mode !== 'count_only'">
          <el-radio-group v-model="createForm.compare_mode">
            <el-radio value="full_value">全量比较</el-radio>
            <el-radio value="length_only">仅比较长度</el-radio>
            <el-radio value="exists_only">仅比较存在性</el-radio>
          </el-radio-group>
          <div class="hint-block">
            <span v-if="createForm.compare_mode === 'full_value'">比较完整的 Key 值内容</span>
            <span v-else-if="createForm.compare_mode === 'length_only'">只比较值的长度，适合大 Value 场景</span>
            <span v-else>只检查 Key 是否存在，不比较值</span>
          </div>
        </el-form-item>
        
        <el-form-item label="比较TTL" v-if="createForm.verify_mode !== 'count_only'">
          <el-switch v-model="createForm.compare_ttl" />
          <span class="hint">比较 Key 的过期时间</span>
        </el-form-item>
        
        <el-form-item label="TTL容差" v-if="createForm.compare_ttl">
          <el-input-number 
            v-model="createForm.ttl_tolerance" 
            :min="0" 
            :max="3600"
          />
          <span class="hint">秒，TTL 差异在此范围内视为一致</span>
        </el-form-item>
        
        <el-divider content-position="left">大Key处理</el-divider>
        
        <el-form-item label="跳过大Key" v-if="createForm.compare_mode === 'full_value'">
          <el-switch v-model="createForm.skip_large_key" />
          <span class="hint">全量比较时跳过超大 Key，避免内存问题</span>
        </el-form-item>
        
        <el-form-item label="大Key阈值" v-if="createForm.skip_large_key && createForm.compare_mode === 'full_value'">
          <el-input-number 
            v-model="createForm.large_key_threshold_mb" 
            :min="1" 
            :max="100"
          />
          <span class="hint">MB，超过此大小的 Key 将被跳过</span>
        </el-form-item>
        
        <el-divider content-position="left">性能控制</el-divider>
        
        <el-form-item label="并发数">
          <el-input-number 
            v-model="createForm.concurrency" 
            :min="1" 
            :max="100"
          />
          <span class="hint">同时校验的 Key 数量（1-100）</span>
        </el-form-item>
        
        <el-form-item label="QPS限制">
          <el-input-number 
            v-model="createForm.qps" 
            :min="0" 
            :max="100000"
            :step="100"
          />
          <span class="hint">每秒最大请求数，0 表示不限制</span>
        </el-form-item>
        
        <el-divider content-position="left">DB选择</el-divider>
        
        <el-form-item label="指定DB">
          <el-input 
            v-model="createForm.db_list" 
            placeholder="留空校验所有DB，多个用分号分隔，如：0;5;15"
          />
          <div class="hint-block">
            仅对非集群模式有效，集群模式固定使用 DB 0
          </div>
        </el-form-item>
        
        <el-divider content-position="left">多轮迭代收敛（借鉴 redis-full-check）</el-divider>
        
        <el-form-item label="比较轮数">
          <el-input-number 
            v-model="createForm.compare_rounds" 
            :min="1" 
            :max="5"
          />
          <span class="hint">1-5轮，推荐3轮</span>
          <div class="hint-block">
            多轮复查可排除因增量同步延迟造成的"假不一致"
          </div>
        </el-form-item>
        
        <el-form-item label="轮次间隔">
          <el-input-number 
            v-model="createForm.round_interval" 
            :min="1" 
            :max="60"
          />
          <span class="hint">秒，给增量同步追赶时间</span>
        </el-form-item>
        
        <el-divider content-position="left">高级功能（借鉴 redis-full-check）</el-divider>
        
        <el-form-item label="校验方向">
          <el-radio-group v-model="createForm.direction">
            <el-radio value="source_to_target">源→目标（默认）</el-radio>
            <el-radio value="bidirectional">双向校验</el-radio>
          </el-radio-group>
          <div class="hint-block">
            双向校验会额外检测目标端多余的 Key
          </div>
        </el-form-item>
        
        <el-form-item label="智能比较">
          <el-switch v-model="createForm.smart_compare" />
          <span class="hint">根据 Key 大小自动选择比较策略</span>
          <div class="hint-block">
            大 Key 只比较长度，小 Key 全量比较
          </div>
        </el-form-item>
        
        <el-form-item label="Field级别比对">
          <el-switch v-model="createForm.field_level_compare" />
          <span class="hint">对 Hash/Set/ZSet 进行细粒度比对</span>
          <div class="hint-block">
            可精确定位哪个 Field 不一致
          </div>
        </el-form-item>
        
        <el-form-item label="Field扫描阈值" v-if="createForm.field_level_compare">
          <el-input-number 
            v-model="createForm.field_scan_threshold" 
            :min="100" 
            :max="50000"
            :step="1000"
          />
          <span class="hint">元素数，超过此阈值使用 SCAN 获取</span>
        </el-form-item>
        
        <el-form-item label="启用SQLite存储">
          <el-switch v-model="createForm.enable_sqlite" />
          <span class="hint">将结果存入 SQLite（支持断点续传和审计）</span>
        </el-form-item>
        
        <el-divider content-position="left">Key 过滤（可选）</el-divider>
        
        <el-form-item label="只校验前缀">
          <el-input 
            v-model="createForm.key_prefixes" 
            placeholder="多个前缀用逗号分隔，如：user:,order:"
          />
        </el-form-item>
        
        <el-form-item label="排除前缀">
          <el-input 
            v-model="createForm.exclude_prefixes" 
            placeholder="多个前缀用逗号分隔"
          />
        </el-form-item>
        
        <el-form-item label="立即启动">
          <el-switch v-model="createForm.auto_start" />
        </el-form-item>
      </el-form>
      
      <template #footer>
        <el-button @click="showCreateDialog = false">取消</el-button>
        <el-button type="primary" @click="createTask" :loading="creating">创建</el-button>
      </template>
    </el-dialog>

    <!-- 任务详情对话框 -->
    <el-dialog 
      v-model="showDetailDialog" 
      :title="currentTask?.name || '任务详情'" 
      width="800px"
    >
      <div v-if="currentTask" class="task-detail">
        <el-descriptions :column="2" border>
          <el-descriptions-item label="任务ID">{{ currentTask.id }}</el-descriptions-item>
          <el-descriptions-item label="状态">
            <el-tag :type="getStatusType(currentTask.status)">
              {{ getStatusText(currentTask.status) }}
            </el-tag>
          </el-descriptions-item>
          <el-descriptions-item label="校验模式">{{ getModeText(currentTask.verify_mode) }}</el-descriptions-item>
          <el-descriptions-item label="采样率">{{ (currentTask.sample_rate * 100).toFixed(2) }}%</el-descriptions-item>
          <el-descriptions-item label="比较模式">{{ getCompareModeText(currentTask.compare_mode) }}</el-descriptions-item>
          <el-descriptions-item label="并发数/QPS">{{ currentTask.concurrency || 10 }} / {{ currentTask.qps || '不限' }}</el-descriptions-item>
          <el-descriptions-item label="源端集群" :span="2">{{ currentTask.source_cluster }}</el-descriptions-item>
          <el-descriptions-item label="目标端集群" :span="2">{{ currentTask.target_cluster }}</el-descriptions-item>
          <el-descriptions-item label="校验DB">{{ currentTask.db_list || '全部' }}</el-descriptions-item>
          <el-descriptions-item label="跳过大Key">{{ currentTask.skip_large_key ? `是（>${formatBytes(currentTask.large_key_threshold || 10485760)}）` : '否' }}</el-descriptions-item>
          <el-descriptions-item label="创建时间">{{ formatTime(currentTask.created_at) }}</el-descriptions-item>
          <el-descriptions-item label="开始时间">{{ formatTime(currentTask.started_at) || '-' }}</el-descriptions-item>
          <el-descriptions-item label="完成时间" :span="2">{{ formatTime(currentTask.completed_at) || '-' }}</el-descriptions-item>
        </el-descriptions>
        
        <div v-if="currentTask.result" class="result-section">
          <h3>校验结果</h3>
          <el-descriptions :column="3" border>
            <el-descriptions-item label="源端Key数">
              {{ formatNumber(currentTask.result.source_key_count) }}
            </el-descriptions-item>
            <el-descriptions-item label="目标端Key数">
              {{ formatNumber(currentTask.result.target_key_count) }}
            </el-descriptions-item>
            <el-descriptions-item label="Key数量差">
              <span :class="{ 'error-text': currentTask.result.source_key_count !== currentTask.result.target_key_count }">
                {{ Math.abs(currentTask.result.source_key_count - currentTask.result.target_key_count) }}
              </span>
            </el-descriptions-item>
            <el-descriptions-item label="已扫描Key">
              {{ formatNumber(currentTask.result.scanned_keys) }}
            </el-descriptions-item>
            <el-descriptions-item label="已采样Key">
              {{ formatNumber(currentTask.result.sampled_keys) }}
            </el-descriptions-item>
            <el-descriptions-item label="匹配Key">
              {{ formatNumber(currentTask.result.matched_keys) }}
            </el-descriptions-item>
            <el-descriptions-item label="缺失Key">
              <span :class="{ 'error-text': currentTask.result.missing_keys > 0 }">
                {{ currentTask.result.missing_keys }}
              </span>
            </el-descriptions-item>
            <el-descriptions-item label="值不匹配">
              <span :class="{ 'error-text': currentTask.result.value_mismatch > 0 }">
                {{ currentTask.result.value_mismatch }}
              </span>
            </el-descriptions-item>
            <el-descriptions-item label="长度不匹配">
              <span :class="{ 'error-text': currentTask.result.length_mismatch > 0 }">
                {{ currentTask.result.length_mismatch || 0 }}
              </span>
            </el-descriptions-item>
            <el-descriptions-item label="TTL不匹配">
              <span :class="{ 'error-text': currentTask.result.ttl_mismatch > 0 }">
                {{ currentTask.result.ttl_mismatch }}
              </span>
            </el-descriptions-item>
            <el-descriptions-item label="跳过大Key数">
              {{ formatNumber(currentTask.result.large_key_skipped || 0) }}
            </el-descriptions-item>
            <el-descriptions-item label="校验DB列表">
              {{ currentTask.result.dbs_verified?.join(', ') || 'DB 0' }}
            </el-descriptions-item>
            <el-descriptions-item label="比较轮次">
              {{ currentTask.result.current_round || 1 }} / {{ currentTask.result.total_rounds || 1 }}
            </el-descriptions-item>
            <el-descriptions-item label="最终不一致">
              <span :class="{ 'error-text': currentTask.result.final_mismatch_keys?.length > 0 }">
                {{ currentTask.result.final_mismatch_keys?.length || 0 }} 个Key
              </span>
            </el-descriptions-item>
            <el-descriptions-item label="一致性" :span="2">
              <span 
                class="consistency-large" 
                :class="{ high: currentTask.result.consistency_rate >= 99.9 }"
              >
                {{ currentTask.result.consistency_rate?.toFixed(4) || 0 }}%
              </span>
            </el-descriptions-item>
          </el-descriptions>
          
          <!-- 多轮迭代收敛详情 -->
          <div v-if="currentTask.result.rounds?.length > 0" class="rounds-section">
            <h4>
              <el-icon><TrendCharts /></el-icon>
              多轮迭代收敛过程（借鉴 redis-full-check）
            </h4>
            <el-table :data="currentTask.result.rounds" size="small" border>
              <el-table-column prop="round_no" label="轮次" width="60" align="center" />
              <el-table-column prop="keys_to_check" label="待检查Key" width="100">
                <template #default="{ row }">
                  {{ formatNumber(row.keys_to_check) }}
                </template>
              </el-table-column>
              <el-table-column prop="mismatch_count" label="不一致数" width="100">
                <template #default="{ row }">
                  <span :class="{ 'error-text': row.mismatch_count > 0 }">
                    {{ row.mismatch_count }}
                  </span>
                </template>
              </el-table-column>
              <el-table-column prop="converge_rate" label="收敛率" width="100">
                <template #default="{ row }">
                  <span v-if="row.round_no > 1" :class="{ 'success-text': row.converge_rate > 50 }">
                    {{ row.converge_rate?.toFixed(1) }}%
                  </span>
                  <span v-else>-</span>
                </template>
              </el-table-column>
              <el-table-column label="耗时" width="120">
                <template #default="{ row }">
                  {{ formatDuration(row.start_time, row.end_time) }}
                </template>
              </el-table-column>
              <el-table-column label="说明" min-width="200">
                <template #default="{ row }">
                  <span v-if="row.round_no === 1">首轮全量扫描比对</span>
                  <span v-else-if="row.mismatch_count === 0" class="success-text">完全收敛，无不一致</span>
                  <span v-else>复查上轮不一致Key</span>
                </template>
              </el-table-column>
            </el-table>
            <div class="converge-hint">
              <el-icon><InfoFilled /></el-icon>
              多轮复查可排除因增量同步延迟造成的"假不一致"，最后一轮的结果才是真正的不一致数据
            </div>
          </div>
          
          <!-- 不匹配详情 -->
          <div v-if="currentTask.result.details?.length > 0" class="mismatch-details">
            <h4>不匹配详情（最多显示100条）</h4>
            <el-table :data="currentTask.result.details" max-height="300">
              <el-table-column prop="key" label="Key" min-width="200" />
              <el-table-column prop="type" label="类型" width="120">
                <template #default="{ row }">
                  <el-tag :type="getMismatchType(row.type)" size="small">
                    {{ getMismatchText(row.type) }}
                  </el-tag>
                </template>
              </el-table-column>
              <el-table-column label="源端" min-width="150">
                <template #default="{ row }">
                  {{ row.source_value || (row.source_ttl !== undefined ? `TTL: ${row.source_ttl}s` : '-') }}
                </template>
              </el-table-column>
              <el-table-column label="目标端" min-width="150">
                <template #default="{ row }">
                  {{ row.target_value || (row.target_ttl !== undefined ? `TTL: ${row.target_ttl}s` : '-') }}
                </template>
              </el-table-column>
            </el-table>
          </div>
          
          <!-- P2: 双向校验结果 - 目标端多余 Key -->
          <div v-if="currentTask.result.target_extra_keys > 0" class="extra-keys-section">
            <h4>
              <el-icon><WarningFilled /></el-icon>
              目标端多余 Key（双向校验）
            </h4>
            <el-alert
              :title="`发现 ${formatNumber(currentTask.result.target_extra_keys)} 个目标端多余的 Key`"
              type="warning"
              :closable="false"
              show-icon
            />
            <el-table 
              v-if="currentTask.result.extra_key_details?.length > 0" 
              :data="currentTask.result.extra_key_details" 
              max-height="200" 
              size="small"
              style="margin-top: 10px"
            >
              <el-table-column prop="key" label="多余的 Key" min-width="300" />
            </el-table>
          </div>
          
          <!-- P1: Field 级别不一致详情 -->
          <div v-if="currentTask.result.field_mismatches?.length > 0" class="field-mismatch-section">
            <h4>
              <el-icon><Document /></el-icon>
              Field 级别不一致详情（Hash/Set/ZSet）
            </h4>
            <el-collapse accordion>
              <el-collapse-item 
                v-for="(fm, idx) in currentTask.result.field_mismatches" 
                :key="idx"
                :title="`${fm.key} (${fm.key_type}) - ${fm.mismatch_fields?.length || 0} 个 Field 不一致`"
              >
                <el-table :data="fm.mismatch_fields" size="small">
                  <el-table-column prop="field" label="Field/成员" min-width="150" />
                  <el-table-column prop="type" label="差异类型" width="120">
                    <template #default="{ row }">
                      <el-tag :type="getFieldDiffType(row.type)" size="small">
                        {{ getFieldDiffText(row.type) }}
                      </el-tag>
                    </template>
                  </el-table-column>
                  <el-table-column label="源端值" min-width="150">
                    <template #default="{ row }">
                      {{ row.source_value || (row.source_score !== undefined ? `Score: ${row.source_score}` : '-') }}
                    </template>
                  </el-table-column>
                  <el-table-column label="目标端值" min-width="150">
                    <template #default="{ row }">
                      {{ row.target_value || (row.target_score !== undefined ? `Score: ${row.target_score}` : '-') }}
                    </template>
                  </el-table-column>
                </el-table>
              </el-collapse-item>
            </el-collapse>
          </div>
          
          <!-- P3: 性能指标监控 -->
          <div v-if="currentTask.result.metrics" class="metrics-section">
            <h4>
              <el-icon><DataLine /></el-icon>
              性能指标
            </h4>
            <el-descriptions :column="3" border size="small">
              <el-descriptions-item label="总耗时">{{ currentTask.result.metrics.duration || '-' }}</el-descriptions-item>
              <el-descriptions-item label="平均速度">{{ formatNumber(Math.round(currentTask.result.metrics.keys_per_second || 0)) }} keys/s</el-descriptions-item>
              <el-descriptions-item label="Redis命令数">{{ formatNumber(currentTask.result.metrics.redis_commands || 0) }}</el-descriptions-item>
              <el-descriptions-item label="Pipeline批次">{{ formatNumber(currentTask.result.metrics.pipeline_batches || 0) }}</el-descriptions-item>
              <el-descriptions-item label="网络往返">{{ formatNumber(currentTask.result.metrics.network_round_trips || 0) }}</el-descriptions-item>
              <el-descriptions-item label="峰值内存">{{ (currentTask.result.metrics.peak_memory_mb || 0).toFixed(1) }} MB</el-descriptions-item>
            </el-descriptions>
            
            <!-- Key 类型分布 -->
            <div v-if="currentTask.result.metrics.type_distribution" class="type-distribution">
              <h5>Key 类型分布</h5>
              <el-tag 
                v-for="(count, type) in currentTask.result.metrics.type_distribution" 
                :key="type"
                class="type-tag"
                size="small"
              >
                {{ type }}: {{ formatNumber(count) }}
              </el-tag>
            </div>
          </div>
        </div>
      </div>
    </el-dialog>
  </div>
</template>

<script setup>
import { ref, onMounted } from 'vue'
import { ElMessage, ElMessageBox } from 'element-plus'
import { Plus, TrendCharts, InfoFilled, WarningFilled, Document, DataLine } from '@element-plus/icons-vue'
import api from '@/api'
import dayjs from 'dayjs'

const loading = ref(false)
const creating = ref(false)
const tasks = ref([])
const showCreateDialog = ref(false)
const showDetailDialog = ref(false)
const currentTask = ref(null)

const createForm = ref({
  name: '',
  source_addrs: '',
  source_password: '',
  target_addrs: '',
  target_password: '',
  verify_mode: 'sample',
  sample_rate_percent: 1, // 界面用百分比显示
  max_keys: 100000,
  compare_mode: 'full_value', // full_value / length_only / exists_only
  compare_ttl: false,
  ttl_tolerance: 5,
  skip_large_key: true,
  large_key_threshold_mb: 10, // MB
  concurrency: 10,
  qps: 0, // 0 表示不限制
  db_list: '', // 分号分隔，如 "0;5;15"
  compare_rounds: 3, // 多轮迭代收敛（借鉴 redis-full-check）
  round_interval: 5, // 轮次间隔秒数
  // P1/P2 新增功能
  direction: 'source_to_target', // source_to_target / bidirectional
  smart_compare: false,
  field_level_compare: false,
  field_scan_threshold: 5000,
  enable_sqlite: false,
  // Key 过滤
  key_prefixes: '',
  exclude_prefixes: '',
  auto_start: true
})

const fetchTasks = async () => {
  loading.value = true
  try {
    tasks.value = await api.getVerifyTasks() || []
  } catch (err) {
    ElMessage.error('获取校验任务列表失败')
  } finally {
    loading.value = false
  }
}

const createTask = async () => {
  if (!createForm.value.source_addrs || !createForm.value.target_addrs) {
    ElMessage.warning('请填写源端和目标端地址')
    return
  }
  
  creating.value = true
  try {
    const data = {
      name: createForm.value.name,
      source_cluster: {
        addrs: createForm.value.source_addrs.split(',').map(s => s.trim()).filter(Boolean),
        password: createForm.value.source_password
      },
      target_cluster: {
        addrs: createForm.value.target_addrs.split(',').map(s => s.trim()).filter(Boolean),
        password: createForm.value.target_password
      },
      verify_mode: createForm.value.verify_mode,
      sample_rate: createForm.value.sample_rate_percent / 100, // 转换为小数
      max_keys: createForm.value.max_keys,
      compare_mode: createForm.value.compare_mode,
      compare_ttl: createForm.value.compare_ttl,
      ttl_tolerance: createForm.value.ttl_tolerance,
      skip_large_key: createForm.value.skip_large_key,
      large_key_threshold: createForm.value.large_key_threshold_mb * 1024 * 1024, // 转换为字节
      concurrency: createForm.value.concurrency,
      qps: createForm.value.qps,
      db_list: createForm.value.db_list,
      compare_rounds: createForm.value.compare_rounds,
      round_interval: createForm.value.round_interval,
      // P1/P2 新增功能
      direction: createForm.value.direction,
      smart_compare: createForm.value.smart_compare,
      field_level_compare: createForm.value.field_level_compare,
      field_scan_threshold: createForm.value.field_scan_threshold,
      enable_sqlite: createForm.value.enable_sqlite,
      auto_start: createForm.value.auto_start
    }
    
    // Key 过滤
    if (createForm.value.key_prefixes || createForm.value.exclude_prefixes) {
      data.key_filter = {}
      if (createForm.value.key_prefixes) {
        data.key_filter.prefixes = createForm.value.key_prefixes.split(',').map(s => s.trim()).filter(Boolean)
      }
      if (createForm.value.exclude_prefixes) {
        data.key_filter.exclude_prefixes = createForm.value.exclude_prefixes.split(',').map(s => s.trim()).filter(Boolean)
      }
    }
    
    await api.createVerifyTask(data)
    ElMessage.success('校验任务创建成功')
    showCreateDialog.value = false
    resetCreateForm()
    fetchTasks()
  } catch (err) {
    ElMessage.error('创建失败: ' + (err.message || '未知错误'))
  } finally {
    creating.value = false
  }
}

const resetCreateForm = () => {
  createForm.value = {
    name: '',
    source_addrs: '',
    source_password: '',
    target_addrs: '',
    target_password: '',
    verify_mode: 'sample',
    sample_rate_percent: 1,
    max_keys: 100000,
    compare_mode: 'full_value',
    compare_ttl: false,
    ttl_tolerance: 5,
    skip_large_key: true,
    large_key_threshold_mb: 10,
    concurrency: 10,
    qps: 0,
    db_list: '',
    compare_rounds: 3,
    round_interval: 5,
    direction: 'source_to_target',
    smart_compare: false,
    field_level_compare: false,
    field_scan_threshold: 5000,
    enable_sqlite: false,
    key_prefixes: '',
    exclude_prefixes: '',
    auto_start: true
  }
}

const startTask = async (id) => {
  try {
    await api.startVerifyTask(id)
    ElMessage.success('校验任务已启动')
    fetchTasks()
  } catch (err) {
    ElMessage.error('启动失败')
  }
}

const stopTask = async (id) => {
  try {
    await api.stopVerifyTask(id)
    ElMessage.success('校验任务已停止')
    fetchTasks()
  } catch (err) {
    ElMessage.error('停止失败')
  }
}

const deleteTask = async (id) => {
  try {
    await ElMessageBox.confirm('确定要删除这个校验任务吗？', '确认删除', {
      type: 'warning'
    })
    await api.deleteVerifyTask(id)
    ElMessage.success('校验任务已删除')
    fetchTasks()
  } catch (err) {
    if (err !== 'cancel') {
      ElMessage.error('删除失败')
    }
  }
}

const viewTask = async (id) => {
  try {
    currentTask.value = await api.getVerifyTask(id)
    showDetailDialog.value = true
  } catch (err) {
    ElMessage.error('获取任务详情失败')
  }
}

const formatNumber = (num) => {
  if (num === undefined || num === null) return '-'
  return num.toLocaleString()
}

const formatTime = (time) => {
  if (!time) return ''
  return dayjs(time).format('YYYY-MM-DD HH:mm:ss')
}

const formatDuration = (startTime, endTime) => {
  if (!startTime || !endTime) return '-'
  const start = dayjs(startTime)
  const end = dayjs(endTime)
  const seconds = end.diff(start, 'second')
  if (seconds < 60) return `${seconds}秒`
  const minutes = Math.floor(seconds / 60)
  const remainSeconds = seconds % 60
  if (minutes < 60) return `${minutes}分${remainSeconds}秒`
  const hours = Math.floor(minutes / 60)
  const remainMinutes = minutes % 60
  return `${hours}时${remainMinutes}分`
}

const getModeType = (mode) => {
  const map = {
    'count_only': 'info',
    'sample': 'primary',
    'full': 'success'
  }
  return map[mode] || 'info'
}

const getModeText = (mode) => {
  const map = {
    'count_only': '仅统计数量',
    'sample': '采样校验',
    'full': '全量校验'
  }
  return map[mode] || mode
}

const getCompareModeText = (mode) => {
  const map = {
    'full_value': '全量比较',
    'length_only': '仅比较长度',
    'exists_only': '仅比较存在性'
  }
  return map[mode] || mode || '全量比较'
}

const formatBytes = (bytes) => {
  if (!bytes || bytes === 0) return '0 B'
  const k = 1024
  const sizes = ['B', 'KB', 'MB', 'GB']
  const i = Math.floor(Math.log(bytes) / Math.log(k))
  return parseFloat((bytes / Math.pow(k, i)).toFixed(1)) + ' ' + sizes[i]
}

const getStatusType = (status) => {
  const map = {
    'pending': 'info',
    'running': 'primary',
    'completed': 'success',
    'failed': 'danger',
    'cancelled': 'warning'
  }
  return map[status] || 'info'
}

const getStatusText = (status) => {
  const map = {
    'pending': '待启动',
    'running': '运行中',
    'completed': '已完成',
    'failed': '失败',
    'cancelled': '已取消'
  }
  return map[status] || status
}

const getProgressStatus = (status) => {
  if (status === 'completed') return 'success'
  if (status === 'failed') return 'exception'
  return ''
}

const getMismatchType = (type) => {
  const map = {
    'missing': 'danger',
    'extra': 'warning',
    'value_mismatch': 'danger',
    'ttl_mismatch': 'warning'
  }
  return map[type] || 'info'
}

const getMismatchText = (type) => {
  const map = {
    'missing': '缺失',
    'extra': '多余',
    'value_mismatch': '值不匹配',
    'ttl_mismatch': 'TTL不匹配',
    'length_mismatch': '长度不匹配'
  }
  return map[type] || type
}

// P1: Field 级别差异类型辅助函数
const getFieldDiffType = (type) => {
  const map = {
    'lack_source': 'warning',
    'lack_target': 'danger',
    'value_mismatch': 'danger',
    'score_mismatch': 'warning',
    'length_mismatch': 'warning'
  }
  return map[type] || 'info'
}

const getFieldDiffText = (type) => {
  const map = {
    'lack_source': '源端缺失',
    'lack_target': '目标端缺失',
    'value_mismatch': '值不一致',
    'score_mismatch': '分数不一致',
    'length_mismatch': '长度不一致'
  }
  return map[type] || type
}

// 定时刷新运行中的任务
let refreshTimer = null

onMounted(() => {
  fetchTasks()
  refreshTimer = setInterval(() => {
    if (tasks.value.some(t => t.status === 'running')) {
      fetchTasks()
    }
  }, 3000)
})

// 清理定时器
import { onUnmounted } from 'vue'
onUnmounted(() => {
  if (refreshTimer) {
    clearInterval(refreshTimer)
  }
})
</script>

<style lang="scss" scoped>
.verify-tasks {
  padding: 20px;
}

.page-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 20px;
  
  h1 {
    margin: 0;
    font-size: 24px;
  }
}

.task-name {
  color: var(--el-color-primary);
  cursor: pointer;
  
  &:hover {
    text-decoration: underline;
  }
}

.consistency {
  font-weight: 600;
  color: var(--el-color-warning);
  
  &.high {
    color: var(--el-color-success);
  }
}

.no-data {
  color: var(--el-text-color-placeholder);
}

.hint {
  margin-left: 10px;
  color: var(--el-text-color-secondary);
  font-size: 12px;
}

.hint-block {
  margin-top: 5px;
  color: var(--el-text-color-secondary);
  font-size: 12px;
  line-height: 1.5;
}

.task-detail {
  .result-section {
    margin-top: 20px;
    
    h3 {
      margin-bottom: 15px;
      font-size: 16px;
    }
  }
  
  .mismatch-details {
    margin-top: 20px;
    
    h4 {
      margin-bottom: 10px;
      font-size: 14px;
      color: var(--el-text-color-secondary);
    }
  }
}

.consistency-large {
  font-size: 20px;
  font-weight: bold;
  color: var(--el-color-warning);
  
  &.high {
    color: var(--el-color-success);
  }
}

.error-text {
  color: var(--el-color-danger);
  font-weight: 500;
}

.success-text {
  color: var(--el-color-success);
  font-weight: 500;
}

.rounds-section {
  margin-top: 20px;
  
  h4 {
    display: flex;
    align-items: center;
    gap: 8px;
    margin-bottom: 12px;
    font-size: 14px;
    color: var(--el-text-color-primary);
    
    .el-icon {
      color: var(--el-color-primary);
    }
  }
}

.converge-hint {
  margin-top: 12px;
  padding: 10px 14px;
  background: var(--el-color-info-light-9);
  border-radius: 4px;
  font-size: 12px;
  color: var(--el-text-color-secondary);
  display: flex;
  align-items: center;
  gap: 6px;
  
  .el-icon {
    color: var(--el-color-info);
    font-size: 14px;
  }
}

// P2: 双向校验 - 目标端多余 Key 样式
.extra-keys-section {
  margin-top: 20px;
  padding: 15px;
  background: var(--el-color-warning-light-9);
  border-radius: 8px;
  border-left: 4px solid var(--el-color-warning);
  
  h4 {
    display: flex;
    align-items: center;
    gap: 8px;
    margin-bottom: 12px;
    font-size: 14px;
    color: var(--el-color-warning-dark-2);
    
    .el-icon {
      color: var(--el-color-warning);
      font-size: 16px;
    }
  }
}

// P1: Field 级别不一致详情样式
.field-mismatch-section {
  margin-top: 20px;
  padding: 15px;
  background: var(--el-fill-color-light);
  border-radius: 8px;
  
  h4 {
    display: flex;
    align-items: center;
    gap: 8px;
    margin-bottom: 12px;
    font-size: 14px;
    color: var(--el-text-color-primary);
    
    .el-icon {
      color: var(--el-color-primary);
      font-size: 16px;
    }
  }
  
  :deep(.el-collapse-item__header) {
    font-size: 13px;
    font-weight: 500;
    color: var(--el-text-color-regular);
  }
  
  :deep(.el-collapse-item__content) {
    padding: 10px 0;
  }
}

// P3: 性能指标监控样式
.metrics-section {
  margin-top: 20px;
  padding: 15px;
  background: linear-gradient(135deg, var(--el-color-primary-light-9) 0%, var(--el-fill-color-light) 100%);
  border-radius: 8px;
  border: 1px solid var(--el-border-color-lighter);
  
  h4 {
    display: flex;
    align-items: center;
    gap: 8px;
    margin-bottom: 15px;
    font-size: 14px;
    color: var(--el-color-primary);
    
    .el-icon {
      font-size: 18px;
    }
  }
  
  :deep(.el-descriptions__label) {
    font-weight: 500;
    color: var(--el-text-color-secondary);
  }
  
  :deep(.el-descriptions__content) {
    color: var(--el-text-color-primary);
    font-weight: 600;
  }
}

// Key 类型分布样式
.type-distribution {
  margin-top: 15px;
  padding-top: 15px;
  border-top: 1px dashed var(--el-border-color);
  
  h5 {
    margin: 0 0 10px 0;
    font-size: 13px;
    color: var(--el-text-color-secondary);
    font-weight: 500;
  }
}

.type-tag {
  margin-right: 8px;
  margin-bottom: 6px;
  
  &:last-child {
    margin-right: 0;
  }
}

:deep(.el-divider__text) {
  font-weight: 500;
  color: var(--el-text-color-primary);
}
</style>
