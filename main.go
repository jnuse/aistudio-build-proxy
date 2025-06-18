package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
)

// --- Constants ---
const (
	wsPath              = "/v1/ws"
	proxyListenAddr     = ":5345"
	wsReadTimeout       = 60 * time.Second
	proxyRequestTimeout = 600 * time.Second

	// 默认转发的基础URL
	defaultBaseURL = "https://generativelanguage.googleapis.com"

	// 路径重写规则
	sourcePathRewrite = "/v1beta/models"
	targetPathRewrite = "/v1alpha/models"
)

// --- 1. 连接管理与负载均衡 ---

// UserConnection 存储单个WebSocket连接及其元数据
type UserConnection struct {
	Conn       *websocket.Conn
	UserID     string
	LastActive time.Time
	writeMutex sync.Mutex // 保护对此单个连接的并发写入
}

// safeWriteJSON 线程安全地向单个WebSocket连接写入JSON
func (uc *UserConnection) safeWriteJSON(v interface{}) error {
	uc.writeMutex.Lock()
	defer uc.writeMutex.Unlock()
	return uc.Conn.WriteJSON(v)
}

// UserConnections 维护单个用户的所有连接和负载均衡状态
type UserConnections struct {
	sync.Mutex
	Connections []*UserConnection
	NextIndex   int // 用于轮询 (round-robin)
}

// ConnectionPool 全局连接池，并发安全
type ConnectionPool struct {
	sync.RWMutex
	Users map[string]*UserConnections
}

var globalPool = &ConnectionPool{
	Users: make(map[string]*UserConnections),
}

// AddConnection 将新连接添加到池中
func (p *ConnectionPool) AddConnection(userID string, conn *websocket.Conn) *UserConnection {
	userConn := &UserConnection{
		Conn:       conn,
		UserID:     userID,
		LastActive: time.Now(),
	}

	p.Lock()
	defer p.Unlock()

	userConns, exists := p.Users[userID]
	if !exists {
		userConns = &UserConnections{
			Connections: make([]*UserConnection, 0),
			NextIndex:   0,
		}
		p.Users[userID] = userConns
	}

	userConns.Lock()
	userConns.Connections = append(userConns.Connections, userConn)
	userConns.Unlock()

	log.Printf("WebSocket connected: UserID=%s, Total connections for user: %d", userID, len(userConns.Connections))
	return userConn
}

// RemoveConnection 从池中移除连接
func (p *ConnectionPool) RemoveConnection(userID string, conn *websocket.Conn) {
	p.Lock()
	defer p.Unlock()

	userConns, exists := p.Users[userID]
	if !exists {
		return
	}

	userConns.Lock()
	defer userConns.Unlock()

	for i, uc := range userConns.Connections {
		if uc.Conn == conn {
			userConns.Connections[i] = userConns.Connections[len(userConns.Connections)-1]
			userConns.Connections = userConns.Connections[:len(userConns.Connections)-1]
			log.Printf("WebSocket disconnected: UserID=%s, Remaining connections for user: %d", userID, len(userConns.Connections))
			break
		}
	}

	if len(userConns.Connections) == 0 {
		delete(p.Users, userID)
	}
}

// GetConnection 使用轮询策略为用户选择一个连接
func (p *ConnectionPool) GetConnection(userID string) (*UserConnection, error) {
	p.RLock()
	userConns, exists := p.Users[userID]
	p.RUnlock()

	if !exists {
		return nil, errors.New("no available client for this user")
	}

	userConns.Lock()
	defer userConns.Unlock()

	numConns := len(userConns.Connections)
	if numConns == 0 {
		return nil, errors.New("no available client for this user")
	}

	idx := userConns.NextIndex % numConns
	selectedConn := userConns.Connections[idx]
	userConns.NextIndex = (userConns.NextIndex + 1) % numConns

	return selectedConn, nil
}

// --- 2. WebSocket 消息结构 & 待处理请求 ---

type WSMessage struct {
	ID      string                 `json:"id"`
	Type    string                 `json:"type"`
	Payload map[string]interface{} `json:"payload"`
}

var pendingRequests sync.Map

// --- 3. WebSocket 处理器和心跳 ---

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin:     func(r *http.Request) bool { return true },
}

func handleWebSocket(w http.ResponseWriter, r *http.Request) {
	authToken := r.URL.Query().Get("auth_token")
	userID, err := validateJWT(authToken)
	if err != nil {
		log.Printf("WebSocket authentication failed: %v", err)
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("Failed to upgrade to WebSocket: %v", err)
		return
	}

	userConn := globalPool.AddConnection(userID, conn)
	go readPump(userConn)
}

func readPump(uc *UserConnection) {
	defer func() {
		globalPool.RemoveConnection(uc.UserID, uc.Conn)
		uc.Conn.Close()
		log.Printf("readPump closed for user %s", uc.UserID)
	}()

	uc.Conn.SetReadDeadline(time.Now().Add(wsReadTimeout))

	for {
		_, message, err := uc.Conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				log.Printf("WebSocket read error for user %s: %v", uc.UserID, err)
			} else {
				log.Printf("WebSocket closed for user %s: %v", uc.UserID, err)
			}
			break
		}

		uc.Conn.SetReadDeadline(time.Now().Add(wsReadTimeout))
		uc.LastActive = time.Now()

		var msg WSMessage
		if err := json.Unmarshal(message, &msg); err != nil {
			log.Printf("Error unmarshalling WebSocket message: %v", err)
			continue
		}

		switch msg.Type {
		case "ping":
			err := uc.safeWriteJSON(map[string]string{"type": "pong", "id": msg.ID})
			if err != nil {
				log.Printf("Error sending pong: %v", err)
				return
			}
		case "http_response", "stream_start", "stream_chunk", "stream_end", "error":
			if ch, ok := pendingRequests.Load(msg.ID); ok {
				respChan := ch.(chan *WSMessage)
				select {
				case respChan <- &msg:
				default:
					log.Printf("Warning: Response channel full for request ID %s, dropping message type %s", msg.ID, msg.Type)
				}
			} else {
				log.Printf("Received response for unknown or timed-out request ID: %s", msg.ID)
			}
		default:
			log.Printf("Received unknown message type from client: %s", msg.Type)
		}
	}
}

// --- 4. HTTP 反向代理与 WS 隧道 ---

func handleProxyRequest(w http.ResponseWriter, r *http.Request) {
	userID, err := authenticateHTTPRequest(r)
	if err != nil {
		http.Error(w, "Proxy authentication failed", http.StatusUnauthorized)
		return
	}

	reqID := uuid.NewString()

	respChan := make(chan *WSMessage, 10)
	pendingRequests.Store(reqID, respChan)
	defer pendingRequests.Delete(reqID)

	selectedConn, err := globalPool.GetConnection(userID)
	if err != nil {
		log.Printf("Error getting connection for user %s: %v", userID, err)
		http.Error(w, "Service Unavailable: No active client connected", http.StatusServiceUnavailable)
		return
	}

	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Failed to read request body", http.StatusInternalServerError)
		return
	}
	defer r.Body.Close()

	headers := make(map[string][]string)
	for k, v := range r.Header {
		if k != "Connection" && k != "Keep-Alive" && k != "Proxy-Authenticate" && k != "Proxy-Authorization" && k != "Te" && k != "Trailers" && k != "Transfer-Encoding" && k != "Upgrade" {
			headers[k] = v
		}
	}

	var targetURL string
	if r.URL.Path == sourcePathRewrite {
		// 直接构建目标URL，丢弃所有GET参数，并在末尾添加一个问号
		targetURL = defaultBaseURL + targetPathRewrite + "?"
		log.Printf("Path rewritten and query stripped: %s -> %s?. Routing to: %s", r.URL.Path, targetPathRewrite, targetURL)
	} else {
		// 对于所有其他请求，使用默认的基础URL和原始请求URI
		targetURL = defaultBaseURL + r.URL.String()
	}

	requestPayload := WSMessage{
		ID:   reqID,
		Type: "http_request",
		Payload: map[string]interface{}{
			"method":  r.Method,
			"url":     targetURL,
			"headers": headers,
			"body":    string(bodyBytes),
		},
	}

	prettyPayload, err := json.MarshalIndent(requestPayload, "", "  ")
	if err != nil {
		log.Printf("!!! Error marshalling payload for logging: %v", err)
	} else {
		log.Printf("--- Sending WebSocket Request Payload ---\n%s\n---------------------------------------", string(prettyPayload))
	}

	if err := selectedConn.safeWriteJSON(requestPayload); err != nil {
		log.Printf("Failed to send request over WebSocket: %v", err)
		http.Error(w, "Bad Gateway: Failed to send request to client", http.StatusBadGateway)
		return
	}

	processWebSocketResponse(w, r, respChan)
}

func processWebSocketResponse(w http.ResponseWriter, r *http.Request, respChan chan *WSMessage) {
	ctx, cancel := context.WithTimeout(r.Context(), proxyRequestTimeout)
	defer cancel()

	flusher, ok := w.(http.Flusher)
	if !ok {
		log.Println("Warning: ResponseWriter does not support flushing, streaming will be buffered.")
	}

	headersSet := false

	for {
		select {
		case msg, ok := <-respChan:
			if !ok {
				if !headersSet {
					http.Error(w, "Internal Server Error: Response channel closed unexpectedly", http.StatusInternalServerError)
				}
				return
			}

			switch msg.Type {
			case "http_response":
				if headersSet {
					log.Println("Received http_response after headers were already set. Ignoring.")
					return
				}
				setResponseHeaders(w, msg.Payload)
				writeStatusCode(w, msg.Payload)
				writeBody(w, msg.Payload)
				return

			case "stream_start":
				if headersSet {
					log.Println("Received stream_start after headers were already set. Ignoring.")
					continue
				}
				setResponseHeaders(w, msg.Payload)
				writeStatusCode(w, msg.Payload)
				headersSet = true
				if flusher != nil {
					flusher.Flush()
				}

			case "stream_chunk":
				if !headersSet {
					log.Println("Warning: Received stream_chunk before stream_start. Using default 200 OK.")
					w.WriteHeader(http.StatusOK)
					headersSet = true
				}
				writeBody(w, msg.Payload)
				if flusher != nil {
					flusher.Flush()
				}

			case "stream_end":
				if !headersSet {
					w.WriteHeader(http.StatusOK)
				}
				return

			case "error":
				if !headersSet {
					errMsg := "Bad Gateway: Client reported an error"
					if payloadErr, ok := msg.Payload["error"].(string); ok {
						errMsg = payloadErr
					}
					statusCode := http.StatusBadGateway
					if code, ok := msg.Payload["status"].(float64); ok {
						statusCode = int(code)
					}
					http.Error(w, errMsg, statusCode)
				} else {
					log.Printf("Error received from client after stream started: %v", msg.Payload)
				}
				return

			default:
				log.Printf("Received unexpected message type %s while waiting for response", msg.Type)
			}

		case <-ctx.Done():
			if !headersSet {
				log.Printf("Gateway Timeout: No response from client for request %s", r.URL.Path)
				http.Error(w, "Gateway Timeout", http.StatusGatewayTimeout)
			} else {
				log.Printf("Gateway Timeout: Stream incomplete for request %s", r.URL.Path)
			}
			return
		}
	}
}

// --- 辅助函数 ---

func setResponseHeaders(w http.ResponseWriter, payload map[string]interface{}) {
	headers, ok := payload["headers"].(map[string]interface{})
	if !ok {
		return
	}
	for key, value := range headers {
		if values, ok := value.([]interface{}); ok {
			for _, v := range values {
				if strV, ok := v.(string); ok {
					w.Header().Add(key, strV)
				}
			}
		} else if strV, ok := value.(string); ok {
			w.Header().Set(key, strV)
		}
	}
}

func writeStatusCode(w http.ResponseWriter, payload map[string]interface{}) {
	status, ok := payload["status"].(float64)
	if !ok {
		w.WriteHeader(http.StatusOK)
		return
	}
	w.WriteHeader(int(status))
}

func writeBody(w http.ResponseWriter, payload map[string]interface{}) {
	var bodyData []byte
	if body, ok := payload["body"].(string); ok {
		bodyData = []byte(body)
	}
	if data, ok := payload["data"].(string); ok {
		bodyData = []byte(data)
	}

	if len(bodyData) > 0 {
		w.Write(bodyData)
	}
}

func validateJWT(token string) (string, error) {
	if token == "" {
		return "", errors.New("missing auth_token")
	}
	if token == "valid-token-user-1" {
		return "user-1", nil
	}
	return "", errors.New("invalid token")
}

func authenticateHTTPRequest(r *http.Request) (string, error) {
	return "user-1", nil
}

// --- 主函数 ---

func main() {
	http.HandleFunc(wsPath, handleWebSocket)
	http.HandleFunc("/", handleProxyRequest)

	log.Printf("Starting server on %s", proxyListenAddr)
	log.Printf("WebSocket endpoint available at ws://%s%s", proxyListenAddr, wsPath)
	log.Printf("HTTP proxy available at http://%s/", proxyListenAddr)
	log.Printf("Path rewrite rule enabled: %s -> %s (and strips query params)", sourcePathRewrite, targetPathRewrite)

	if err := http.ListenAndServe(proxyListenAddr, nil); err != nil {
		log.Fatalf("Could not start server: %s\n", err)
	}
}
