package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"
)

// 延迟测量结果
type LatencyResult struct {
	CIDR    string  `json:"cidr"`
	Latency float64 `json:"latency_ms"`
	Error   string  `json:"error,omitempty"`
}

// 响应结构
type Response struct {
	ServerLocation string          `json:"server_location"`
	Timestamp      time.Time       `json:"timestamp"`
	Results        []LatencyResult `json:"results"`
}

// 服务器位置（从环境变量获取）
var serverLocation string

// 下载URL
var cidrListURL = "https://core.telegram.org/resources/cidr.txt"

// 缓存延迟结果
var (
	resultsCache      []LatencyResult
	resultsCacheMutex sync.RWMutex
	lastUpdated       time.Time
	updating          bool
	updateMutex       sync.RWMutex
)

func init() {
	// 从环境变量获取服务器位置
	serverLocation = os.Getenv("SERVER_LOCATION")
	if serverLocation == "" {
		serverLocation = "UNKNOWN"
		log.Println("Warning: SERVER_LOCATION environment variable not set")
	}
}

func main() {
	// 启动初始测量
	go updateMeasurements()

	// 定义API端点
	http.HandleFunc("/latency", latencyHandler)
	http.HandleFunc("/health", healthCheckHandler)

	// 设置定期更新
	go func() {
		for {
			time.Sleep(6 * time.Hour) // 每6小时更新一次
			updateMeasurements()
		}
	}()

	// 获取端口
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	// 启动服务器
	log.Printf("Starting latency measurement API on port %s...\n", port)
	log.Fatal(http.ListenAndServe(":"+port, nil))
}

// 健康检查接口
func healthCheckHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("OK"))
}

// 延迟结果接口
func latencyHandler(w http.ResponseWriter, r *http.Request) {
	// 只允许GET方法
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// 检查授权
	authHeader := r.Header.Get("CF-Access-Client-Id")
	if authHeader == "" {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	// 检查是否需要强制更新
	forceUpdate := r.URL.Query().Get("force") == "true"
	if forceUpdate {
		go updateMeasurements()
		w.WriteHeader(http.StatusAccepted)
		w.Write([]byte("Update initiated. Please check back in a few minutes."))
		return
	}

	// 返回缓存的结果
	resultsCacheMutex.RLock()
	defer resultsCacheMutex.RUnlock()

	// 检查是否有测量结果
	if len(resultsCache) == 0 {
		// 如果没有结果，且正在更新，返回202
		if updating {
			w.WriteHeader(http.StatusAccepted)
			w.Write([]byte("Initial measurement is in progress. Please check back in a few minutes."))
			return
		}
		// 如果没有结果，且不在更新中，返回500
		http.Error(w, "No measurement results available", http.StatusInternalServerError)
		return
	}

	// 构建响应
	response := Response{
		ServerLocation: serverLocation,
		Timestamp:      lastUpdated,
		Results:        resultsCache,
	}

	// 返回JSON响应
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

// 更新延迟测量
func updateMeasurements() {
	updateMutex.Lock()
	if updating {
		updateMutex.Unlock()
		return
	}
	updating = true
	updateMutex.Unlock()

	defer func() {
		updateMutex.Lock()
		updating = false
		updateMutex.Unlock()
	}()

	log.Println("Starting measurement update...")

	// 下载CIDR列表
	cidrs, err := downloadCIDRList()
	if err != nil {
		log.Printf("Error downloading CIDR list: %v", err)
		return
	}

	log.Printf("Downloaded %d CIDRs for testing", len(cidrs))

	// 并行测量所有CIDR的延迟
	var wg sync.WaitGroup
	results := make([]LatencyResult, len(cidrs))
	semaphore := make(chan struct{}, 10) // 限制并发数

	for i, cidr := range cidrs {
		wg.Add(1)
		go func(index int, cidrBlock string) {
			defer wg.Done()
			semaphore <- struct{}{} // 获取信号量
			results[index] = measureLatency(cidrBlock)
			<-semaphore // 释放信号量
		}(i, cidr)
	}

	wg.Wait()

	// 更新缓存
	resultsCacheMutex.Lock()
	resultsCache = results
	lastUpdated = time.Now()
	resultsCacheMutex.Unlock()

	log.Printf("Measurement update completed with %d results", len(results))
}

// 下载CIDR列表
func downloadCIDRList() ([]string, error) {
	// 发送GET请求
	resp, err := http.Get(cidrListURL)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	// 检查响应状态
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("bad response status: %s", resp.Status)
	}

	// 读取响应内容
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	// 解析CIDR列表
	lines := strings.Split(string(body), "\n")
	var cidrs []string

	for _, line := range lines {
		// 清理空白并跳过空行
		line = strings.TrimSpace(line)
		if line != "" && !strings.HasPrefix(line, "#") {
			cidrs = append(cidrs, line)
		}
	}

	return cidrs, nil
}

// 测量指定CIDR的延迟
func measureLatency(cidr string) LatencyResult {
	// 解析CIDR
	_, ipNet, err := net.ParseCIDR(cidr)
	if err != nil {
		return LatencyResult{
			CIDR:  cidr,
			Error: fmt.Sprintf("Invalid CIDR format: %v", err),
		}
	}

	// 获取CIDR中的第一个IP
	ip := getFirstIP(ipNet)
	if ip == nil {
		return LatencyResult{
			CIDR:  cidr,
			Error: "Failed to get first IP from CIDR",
		}
	}

	// 对于IPv6，我们使用ping6
	var pingCmd string
	if ip.To4() == nil {
		pingCmd = "ping6"
	} else {
		pingCmd = "ping"
	}

	// 执行traceroute获取最后一跳延迟
	latency, err := measureWithTraceroute(ip.String(), pingCmd == "ping6")
	result := LatencyResult{
		CIDR: cidr,
	}

	if err != nil {
		// 如果traceroute失败，尝试使用ping
		latency, err = measureWithPing(ip.String(), pingCmd)
		if err != nil {
			result.Error = fmt.Sprintf("Measurement failed: %v", err)
		} else {
			result.Latency = latency
		}
	} else {
		result.Latency = latency
	}

	return result
}

// 使用traceroute测量延迟（取最后一跳）
func measureWithTraceroute(ip string, isIPv6 bool) (float64, error) {
	var cmd *exec.Cmd
	if isIPv6 {
		cmd = exec.Command("traceroute6", "-I", "-n", "-q", "1", ip)
	} else {
		cmd = exec.Command("traceroute", "-I", "-n", "-q", "1", ip)
	}

	output, err := cmd.CombinedOutput()
	if err != nil {
		return 0, fmt.Errorf("traceroute failed: %v", err)
	}

	// 解析输出获取最后一跳延迟
	lines := strings.Split(string(output), "\n")
	var lastValidLine string
	for i := len(lines) - 1; i >= 0; i-- {
		line := lines[i]
		if strings.Contains(line, "ms") && !strings.Contains(line, "*") {
			lastValidLine = line
			break
		}
	}

	if lastValidLine == "" {
		return 0, fmt.Errorf("no valid traceroute hop found")
	}

	// 解析延迟
	fields := strings.Fields(lastValidLine)
	for _, field := range fields {
		if strings.HasSuffix(field, "ms") {
			latencyStr := strings.TrimSuffix(field, "ms")
			latency, err := strconv.ParseFloat(latencyStr, 64)
			if err == nil {
				return latency, nil
			}
		}
	}

	return 0, fmt.Errorf("could not parse latency from traceroute output")
}

// 使用ping测量延迟
func measureWithPing(ip string, pingCmd string) (float64, error) {
	// 执行ping命令 (5次ping, 超时2秒)
	cmd := exec.Command(pingCmd, "-c", "5", "-W", "2", ip)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return 0, fmt.Errorf("ping failed: %v", err)
	}

	// 解析ping输出获取平均延迟
	outputStr := string(output)
	avgIndex := strings.Index(outputStr, "min/avg/max")
	if avgIndex == -1 {
		return 0, fmt.Errorf("could not find avg ping time")
	}

	statsLine := outputStr[avgIndex:]
	statsParts := strings.Split(statsLine, " = ")
	if len(statsParts) < 2 {
		return 0, fmt.Errorf("invalid ping statistics format")
	}

	timeParts := strings.Split(statsParts[1], "/")
	if len(timeParts) < 3 {
		return 0, fmt.Errorf("invalid ping time format")
	}

	avgLatency, err := strconv.ParseFloat(timeParts[1], 64)
	if err != nil {
		return 0, fmt.Errorf("could not parse average latency: %v", err)
	}

	return avgLatency, nil
}

// 获取CIDR范围内的第一个IP地址
func getFirstIP(ipNet *net.IPNet) net.IP {
	ip := ipNet.IP
	return ip
}
