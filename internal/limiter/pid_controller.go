package limiter

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

// PIDController PID控制器 - 用于动态调整迁移速率
type PIDController struct {
	Kp         float64 // 比例系数
	Ki         float64 // 积分系数
	Kd         float64 // 微分系数
	TargetLoad float64 // 目标负载率 (0-1)

	integral  float64
	lastError float64
	lastTime  time.Time

	mu sync.Mutex
}

// NewPIDController 创建PID控制器
func NewPIDController(kp, ki, kd, targetLoad float64) *PIDController {
	return &PIDController{
		Kp:         kp,
		Ki:         ki,
		Kd:         kd,
		TargetLoad: targetLoad,
		lastTime:   time.Now(),
	}
}

// Calculate 计算PID输出调整值
// currentLoad: 当前负载率 (0-1)
// 返回: 速率调整因子 (正值表示加速，负值表示减速)
func (pid *PIDController) Calculate(currentLoad float64) float64 {
	pid.mu.Lock()
	defer pid.mu.Unlock()

	now := time.Now()
	dt := now.Sub(pid.lastTime).Seconds()
	if dt <= 0 {
		dt = 0.001 // 防止除零
	}
	pid.lastTime = now

	// 计算误差 (目标负载 - 当前负载，负载越低可迁移越快)
	error := pid.TargetLoad - currentLoad

	// 积分项 (累积误差)
	pid.integral += error * dt

	// 限制积分累积，防止积分饱和
	maxIntegral := 10.0
	if pid.integral > maxIntegral {
		pid.integral = maxIntegral
	} else if pid.integral < -maxIntegral {
		pid.integral = -maxIntegral
	}

	// 微分项 (误差变化率)
	derivative := 0.0
	if dt > 0 {
		derivative = (error - pid.lastError) / dt
	}
	pid.lastError = error

	// PID输出
	output := pid.Kp*error + pid.Ki*pid.integral + pid.Kd*derivative

	// 限制输出范围 [-0.5, 0.5]，防止速率剧烈变化
	if output > 0.5 {
		output = 0.5
	} else if output < -0.5 {
		output = -0.5
	}

	return output
}

// Reset 重置控制器状态
func (pid *PIDController) Reset() {
	pid.mu.Lock()
	defer pid.mu.Unlock()

	pid.integral = 0
	pid.lastError = 0
	pid.lastTime = time.Now()
}

// RateLimiter 令牌桶速率限制器
type RateLimiter struct {
	sourceQPS    int64 // 源端QPS限制
	targetQPS    int64 // 目标端QPS限制
	bandwidthMB  int64 // 带宽限制(MB/s)

	// 当前动态调整后的速率
	currentSourceQPS atomic.Int64
	currentTargetQPS atomic.Int64

	// 令牌桶
	sourceTokens atomic.Int64
	targetTokens atomic.Int64
	bandwidthTokens atomic.Int64

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewRateLimiter 创建速率限制器
func NewRateLimiter(sourceQPS, targetQPS int64, bandwidthMB int64) *RateLimiter {
	ctx, cancel := context.WithCancel(context.Background())

	rl := &RateLimiter{
		sourceQPS:   sourceQPS,
		targetQPS:   targetQPS,
		bandwidthMB: bandwidthMB,
		ctx:         ctx,
		cancel:      cancel,
	}

	rl.currentSourceQPS.Store(sourceQPS)
	rl.currentTargetQPS.Store(targetQPS)
	rl.sourceTokens.Store(sourceQPS)
	rl.targetTokens.Store(targetQPS)
	rl.bandwidthTokens.Store(bandwidthMB * 1024 * 1024)

	// 启动令牌补充循环
	rl.wg.Add(1)
	go rl.tokenRefillLoop()

	return rl
}

// tokenRefillLoop 令牌补充循环 (每100ms补充1/10的令牌)
func (rl *RateLimiter) tokenRefillLoop() {
	defer rl.wg.Done()

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-rl.ctx.Done():
			return
		case <-ticker.C:
			// 补充源端令牌
			srcQPS := rl.currentSourceQPS.Load()
			currentSrc := rl.sourceTokens.Load()
			newSrc := currentSrc + srcQPS/10
			if newSrc > srcQPS {
				newSrc = srcQPS
			}
			rl.sourceTokens.Store(newSrc)

			// 补充目标端令牌
			tgtQPS := rl.currentTargetQPS.Load()
			currentTgt := rl.targetTokens.Load()
			newTgt := currentTgt + tgtQPS/10
			if newTgt > tgtQPS {
				newTgt = tgtQPS
			}
			rl.targetTokens.Store(newTgt)

			// 补充带宽令牌
			maxBandwidth := rl.bandwidthMB * 1024 * 1024
			currentBw := rl.bandwidthTokens.Load()
			newBw := currentBw + maxBandwidth/10
			if newBw > maxBandwidth {
				newBw = maxBandwidth
			}
			rl.bandwidthTokens.Store(newBw)
		}
	}
}

// AcquireSource 获取源端读取令牌（消耗1个令牌）
func (rl *RateLimiter) AcquireSource() {
	rl.AcquireSourceN(1)
}

// AcquireSourceN 获取 n 个源端读取令牌
func (rl *RateLimiter) AcquireSourceN(n int64) {
	if n <= 0 {
		return
	}
	remaining := n
	for remaining > 0 {
		tokens := rl.sourceTokens.Load()
		if tokens > 0 {
			// 尽可能多地消耗令牌，但不超过可用量
			consume := remaining
			if consume > tokens {
				consume = tokens
			}
			if rl.sourceTokens.CompareAndSwap(tokens, tokens-consume) {
				remaining -= consume
				if remaining <= 0 {
					return
				}
			}
			continue
		}

		select {
		case <-rl.ctx.Done():
			return
		case <-time.After(5 * time.Millisecond):
			continue
		}
	}
}

// AcquireTarget 获取目标端写入令牌（消耗1个令牌）
func (rl *RateLimiter) AcquireTarget() {
	rl.AcquireTargetN(1)
}

// AcquireTargetN 获取 n 个目标端写入令牌
func (rl *RateLimiter) AcquireTargetN(n int64) {
	if n <= 0 {
		return
	}
	remaining := n
	for remaining > 0 {
		tokens := rl.targetTokens.Load()
		if tokens > 0 {
			consume := remaining
			if consume > tokens {
				consume = tokens
			}
			if rl.targetTokens.CompareAndSwap(tokens, tokens-consume) {
				remaining -= consume
				if remaining <= 0 {
					return
				}
			}
			continue
		}

		select {
		case <-rl.ctx.Done():
			return
		case <-time.After(5 * time.Millisecond):
			continue
		}
	}
}

// AcquireBandwidth 获取带宽令牌
func (rl *RateLimiter) AcquireBandwidth(bytes int64) {
	for {
		tokens := rl.bandwidthTokens.Load()
		if tokens >= bytes {
			if rl.bandwidthTokens.CompareAndSwap(tokens, tokens-bytes) {
				return
			}
		}

		select {
		case <-rl.ctx.Done():
			return
		case <-time.After(5 * time.Millisecond):
			continue
		}
	}
}

// AdjustRate 动态调整速率
func (rl *RateLimiter) AdjustRate(sourceQPS, targetQPS int64) {
	if sourceQPS > 0 {
		// 限制在原始配置的10%-200%范围内
		minQPS := rl.sourceQPS / 10
		maxQPS := rl.sourceQPS * 2
		if sourceQPS < minQPS {
			sourceQPS = minQPS
		}
		if sourceQPS > maxQPS {
			sourceQPS = maxQPS
		}
		rl.currentSourceQPS.Store(sourceQPS)
	}

	if targetQPS > 0 {
		minQPS := rl.targetQPS / 10
		maxQPS := rl.targetQPS * 2
		if targetQPS < minQPS {
			targetQPS = minQPS
		}
		if targetQPS > maxQPS {
			targetQPS = maxQPS
		}
		rl.currentTargetQPS.Store(targetQPS)
	}
}

// GetCurrentRate 获取当前速率
func (rl *RateLimiter) GetCurrentRate() (sourceQPS, targetQPS int64) {
	return rl.currentSourceQPS.Load(), rl.currentTargetQPS.Load()
}

// Stop 停止限流器
func (rl *RateLimiter) Stop() {
	rl.cancel()
	rl.wg.Wait()
}

// AdaptiveRateLimiter 自适应速率限制器 (结合PID控制器)
type AdaptiveRateLimiter struct {
	limiter     *RateLimiter
	pid         *PIDController
	client      *redis.ClusterClient
	enabled     bool
	interval    time.Duration

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// 监控指标
	lastOps       atomic.Int64
	lastOpsTime   atomic.Int64
	currentLoad   atomic.Int64 // 存储 currentLoad * 1000 的整数
}

// AdaptiveConfig 自适应限流配置
type AdaptiveConfig struct {
	Enabled        bool          // 是否启用
	Kp             float64       // PID比例系数
	Ki             float64       // PID积分系数
	Kd             float64       // PID微分系数
	TargetLoad     float64       // 目标负载率
	AdjustInterval time.Duration // 调整间隔
}

// DefaultAdaptiveConfig 默认自适应配置
func DefaultAdaptiveConfig() *AdaptiveConfig {
	return &AdaptiveConfig{
		Enabled:        true,
		Kp:             0.5,
		Ki:             0.1,
		Kd:             0.05,
		TargetLoad:     0.7, // 目标负载70%
		AdjustInterval: 5 * time.Second,
	}
}

// NewAdaptiveRateLimiter 创建自适应速率限制器
func NewAdaptiveRateLimiter(limiter *RateLimiter, client *redis.ClusterClient, cfg *AdaptiveConfig) *AdaptiveRateLimiter {
	if cfg == nil {
		cfg = DefaultAdaptiveConfig()
	}

	ctx, cancel := context.WithCancel(context.Background())

	arl := &AdaptiveRateLimiter{
		limiter:  limiter,
		pid:      NewPIDController(cfg.Kp, cfg.Ki, cfg.Kd, cfg.TargetLoad),
		client:   client,
		enabled:  cfg.Enabled,
		interval: cfg.AdjustInterval,
		ctx:      ctx,
		cancel:   cancel,
	}

	return arl
}

// Start 启动自适应调整
func (arl *AdaptiveRateLimiter) Start() {
	if !arl.enabled {
		return
	}

	arl.wg.Add(1)
	go arl.adjustLoop()
}

// adjustLoop 自适应调整循环
func (arl *AdaptiveRateLimiter) adjustLoop() {
	defer arl.wg.Done()

	ticker := time.NewTicker(arl.interval)
	defer ticker.Stop()

	for {
		select {
		case <-arl.ctx.Done():
			return
		case <-ticker.C:
			arl.adjust()
		}
	}
}

// adjust 执行一次调整
func (arl *AdaptiveRateLimiter) adjust() {
	// 获取当前负载
	load, err := arl.getCurrentLoad()
	if err != nil {
		return
	}

	// 存储当前负载
	arl.currentLoad.Store(int64(load * 1000))

	// 计算PID调整值
	adjustment := arl.pid.Calculate(load)

	// 获取当前速率
	sourceQPS, targetQPS := arl.limiter.GetCurrentRate()

	// 应用调整
	newSourceQPS := int64(float64(sourceQPS) * (1 + adjustment))
	newTargetQPS := int64(float64(targetQPS) * (1 + adjustment))

	arl.limiter.AdjustRate(newSourceQPS, newTargetQPS)
}

// getCurrentLoad 获取集群当前负载
func (arl *AdaptiveRateLimiter) getCurrentLoad() (float64, error) {
	if arl.client == nil {
		return 0.5, nil // 默认50%负载
	}

	ctx, cancel := context.WithTimeout(arl.ctx, 5*time.Second)
	defer cancel()

	// 获取INFO stats
	info, err := arl.client.Info(ctx, "stats").Result()
	if err != nil {
		return 0, fmt.Errorf("get info failed: %w", err)
	}

	// 解析 instantaneous_ops_per_sec
	ops := parseInfoValue(info, "instantaneous_ops_per_sec")

	// 计算负载（基于配置的最大QPS）
	maxOps := float64(arl.limiter.sourceQPS)
	if maxOps == 0 {
		maxOps = 100000 // 默认10万
	}

	load := float64(ops) / maxOps
	if load > 1.0 {
		load = 1.0
	}

	return load, nil
}

// GetCurrentLoad 获取当前负载率 (外部调用)
func (arl *AdaptiveRateLimiter) GetCurrentLoad() float64 {
	return float64(arl.currentLoad.Load()) / 1000.0
}

// Stop 停止自适应调整
func (arl *AdaptiveRateLimiter) Stop() {
	arl.cancel()
	arl.wg.Wait()
}

// parseInfoValue 解析INFO命令返回值中的指定字段
func parseInfoValue(info, key string) int64 {
	// 简单解析，实际需要更完善的解析逻辑
	var value int64
	lines := splitLines(info)
	for _, line := range lines {
		if len(line) > len(key)+1 && line[:len(key)] == key && line[len(key)] == ':' {
			fmt.Sscanf(line[len(key)+1:], "%d", &value)
			break
		}
	}
	return value
}

func splitLines(s string) []string {
	var lines []string
	start := 0
	for i := 0; i < len(s); i++ {
		if s[i] == '\n' {
			line := s[start:i]
			if len(line) > 0 && line[len(line)-1] == '\r' {
				line = line[:len(line)-1]
			}
			if len(line) > 0 {
				lines = append(lines, line)
			}
			start = i + 1
		}
	}
	if start < len(s) {
		lines = append(lines, s[start:])
	}
	return lines
}
