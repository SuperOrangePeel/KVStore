package main

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

// KVSClient 是 KVStore 的 Go 客户端封装。
//
// 客户端统一使用 RESP array/bulk string 协议发送命令，因此可以安全传输二进制向量。
type KVSClient struct {
	conn net.Conn
	r    *bufio.Reader
}

// SearchResult 对应 GETV 返回的一条结果。
type SearchResult struct {
	Member string
	Score  float64
	Value  string // 只有 WITHVALUES 时由服务端返回；普通 GETV 时为空。
}

// VectorInfo 对应 VINFO 返回的 collection 元信息。
type VectorInfo struct {
	Name         string
	Dim          uint32
	Metric       string
	Index        string
	BlockSize    uint64
	BlockCount   uint64
	TotalCount   uint64
	ActiveCount  uint64
	DeletedCount uint64
}

// NewKVSClient 创建一个 TCP client。addr 示例："127.0.0.1:2000"。
func NewKVSClient(addr string) (*KVSClient, error) {
	conn, err := net.DialTimeout("tcp", addr, 3*time.Second)
	if err != nil {
		return nil, err
	}
	return &KVSClient{conn: conn, r: bufio.NewReader(conn)}, nil
}

func (c *KVSClient) Close() error {
	if c == nil || c.conn == nil {
		return nil
	}
	return c.conn.Close()
}

// Set 执行 SET key value。
func (c *KVSClient) Set(key, value string) error {
	_, err := c.do("SET", key, value)
	return err
}

// SetEX 执行 SETEX key seconds value。
func (c *KVSClient) SetEX(key string, seconds int, value string) error {
	if seconds <= 0 {
		return errors.New("seconds must be positive")
	}
	_, err := c.do("SETEX", key, strconv.Itoa(seconds), value)
	return err
}

// Get 执行 GET key。
func (c *KVSClient) Get(key string) (string, error) {
	resp, err := c.do("GET", key)
	if err != nil {
		return "", err
	}
	value, ok := resp.(string)
	if !ok {
		return "", fmt.Errorf("unexpected GET response: %T", resp)
	}
	return value, nil
}

// Del 执行 DEL key。
func (c *KVSClient) Del(key string) error {
	_, err := c.do("DEL", key)
	return err
}

// Mod 执行 MOD key value。
func (c *KVSClient) Mod(key, value string) error {
	_, err := c.do("MOD", key, value)
	return err
}

// Exist 执行 EXIST key。
func (c *KVSClient) Exist(key string) (bool, error) {
	_, err := c.do("EXIST", key)
	if err == nil {
		return true, nil
	}
	if strings.Contains(err.Error(), "EXIST") && !strings.Contains(err.Error(), "NOT") {
		return true, nil
	}
	if strings.Contains(err.Error(), "NOT FOUND") || strings.Contains(err.Error(), "NOT EXIST") {
		return false, nil
	}
	return false, err
}

// CreateVector 执行 CREATEV name DIM dim METRIC metric INDEX indexType。
//
// 例：CreateVector("qa_index", 768, "COSINE", "FLAT")。
// 当前服务端只实现 FLAT；HNSW 预留但还未实现。
func (c *KVSClient) CreateVector(name string, dim int, metric, indexType string) error {
	if dim <= 0 {
		return errors.New("dim must be positive")
	}
	_, err := c.do("CREATEV", name, "DIM", strconv.Itoa(dim), "METRIC", metric, "INDEX", indexType)
	return err
}

// SetVectorFloat32 执行 SETV collection member FLOAT32 <vector bytes>。
//
// vector 会按 little-endian float32 编码；服务端当前直接把 bulk bytes cast 为 float 数组。
func (c *KVSClient) SetVectorFloat32(collection, member string, vector []float32) error {
	if len(vector) == 0 {
		return errors.New("vector must not be empty")
	}
	_, err := c.doBulk([][]byte{
		[]byte("SETV"),
		[]byte(collection),
		[]byte(member),
		[]byte("FLOAT32"),
		float32Bytes(vector),
	})
	return err
}

// SetVectorFloat32WithValue 执行 SETV collection member FLOAT32 <vector bytes> VALUE <value>。
//
// 这个接口会让服务端同时写入：
//   1. vector collection 中的 member -> vector
//   2. 主 hash 中的 member -> value
//
// 因此后续 SearchVectorWithValues 命中 member 时，可以直接返回对应 value。
func (c *KVSClient) SetVectorFloat32WithValue(collection, member string, vector []float32, value string) error {
	if len(vector) == 0 {
		return errors.New("vector must not be empty")
	}
	if value == "" {
		return errors.New("value must not be empty")
	}
	_, err := c.doBulk([][]byte{
		[]byte("SETV"),
		[]byte(collection),
		[]byte(member),
		[]byte("FLOAT32"),
		float32Bytes(vector),
		[]byte("VALUE"),
		[]byte(value),
	})
	return err
}

// SearchVector 执行 GETV collection FLOAT32 <query_vector> TOPK topK。
func (c *KVSClient) SearchVector(collection string, query []float32, topK int) ([]SearchResult, error) {
	return c.searchVector(collection, query, topK, false)
}

// SearchVectorWithValues 执行 GETV collection FLOAT32 <query_vector> TOPK topK WITHVALUES。
//
// WITHVALUES 会让服务端用命中的 member 作为普通 KV key，再把对应 value 一起返回。
func (c *KVSClient) SearchVectorWithValues(collection string, query []float32, topK int) ([]SearchResult, error) {
	return c.searchVector(collection, query, topK, true)
}

// DeleteVector 执行 DELV collection member。
func (c *KVSClient) DeleteVector(collection, member string) error {
	_, err := c.do("DELV", collection, member)
	return err
}

// VectorInfo 执行 VINFO collection。
func (c *KVSClient) VectorInfo(collection string) (VectorInfo, error) {
	resp, err := c.do("VINFO", collection)
	if err != nil {
		return VectorInfo{}, err
	}
	fields, ok := resp.([]any)
	if !ok {
		return VectorInfo{}, fmt.Errorf("unexpected VINFO response: %T", resp)
	}
	return parseVectorInfo(fields)
}

// SetQAID 执行 SETQA index ID member QUESTION question ANSWER answer。
//
// 适合业务方已有稳定 ID 的场景。服务端 Master 会请求 embedding，完成后写入 answer、question 和 vector，
// 并把确定性的 SETQA.APPLY 写入 AOF/复制给 Slave。
func (c *KVSClient) SetQAID(index, member, question, answer string) error {
	if index == "" || member == "" || question == "" || answer == "" {
		return errors.New("index/member/question/answer must not be empty")
	}
	_, err := c.do("SETQA", index, "ID", member, "QUESTION", question, "ANSWER", answer)
	return err
}

// SetQAAuto 执行 SETQA index AUTO QUESTION question ANSWER answer。
//
// member 由 Master 生成并返回，例如 qa:1000001。Slave 不会自行生成 member。
func (c *KVSClient) SetQAAuto(index, question, answer string) (string, error) {
	if index == "" || question == "" || answer == "" {
		return "", errors.New("index/question/answer must not be empty")
	}
	resp, err := c.do("SETQA", index, "AUTO", "QUESTION", question, "ANSWER", answer)
	if err != nil {
		return "", err
	}
	member, ok := resp.(string)
	if !ok {
		return "", fmt.Errorf("unexpected SETQA AUTO response: %T", resp)
	}
	return member, nil
}

// GetQA 执行 GETQA index QUESTION question TOPK topK。
//
// 返回结果格式与 GETV WITHVALUES 一致：member、score、answer。
func (c *KVSClient) GetQA(index, question string, topK int) ([]SearchResult, error) {
	if index == "" || question == "" {
		return nil, errors.New("index/question must not be empty")
	}
	if topK <= 0 {
		return nil, errors.New("topK must be positive")
	}
	resp, err := c.do("GETQA", index, "QUESTION", question, "TOPK", strconv.Itoa(topK))
	if err != nil {
		return nil, err
	}
	arr, ok := resp.([]any)
	if !ok {
		return nil, fmt.Errorf("unexpected GETQA response: %T", resp)
	}
	return parseSearchResults(arr, true)
}

// DelQA 执行 DELQA index member。
//
// 服务端会删除 vector，并删除主 hash 中的 member -> answer 和 member:q -> question。
func (c *KVSClient) DelQA(index, member string) error {
	if index == "" || member == "" {
		return errors.New("index/member must not be empty")
	}
	_, err := c.do("DELQA", index, member)
	return err
}

func (c *KVSClient) searchVector(collection string, query []float32, topK int, withValues bool) ([]SearchResult, error) {
	if len(query) == 0 {
		return nil, errors.New("query vector must not be empty")
	}
	if topK <= 0 {
		return nil, errors.New("topK must be positive")
	}

	args := [][]byte{
		[]byte("GETV"),
		[]byte(collection),
		[]byte("FLOAT32"),
		float32Bytes(query),
		[]byte("TOPK"),
		[]byte(strconv.Itoa(topK)),
	}
	if withValues {
		args = append(args, []byte("WITHVALUES"))
	}

	resp, err := c.doBulk(args)
	if err != nil {
		return nil, err
	}
	arr, ok := resp.([]any)
	if !ok {
		return nil, fmt.Errorf("unexpected GETV response: %T", resp)
	}
	return parseSearchResults(arr, withValues)
}

func (c *KVSClient) do(args ...string) (any, error) {
	bulkArgs := make([][]byte, len(args))
	for i, arg := range args {
		bulkArgs[i] = []byte(arg)
	}
	return c.doBulk(bulkArgs)
}

func (c *KVSClient) doBulk(args [][]byte) (any, error) {
	if c == nil || c.conn == nil || c.r == nil {
		return nil, errors.New("client is closed")
	}
	if len(args) == 0 {
		return nil, errors.New("empty command")
	}

	if err := writeRESPArray(c.conn, args); err != nil {
		return nil, err
	}

	resp, err := readRESP(c.r)
	if err != nil {
		return nil, err
	}

	// +OK 这种 simple string 直接视为成功；其他 simple string 也原样返回。
	return resp, nil
}

func writeRESPArray(w io.Writer, args [][]byte) error {
	if _, err := fmt.Fprintf(w, "*%d\r\n", len(args)); err != nil {
		return err
	}
	for _, arg := range args {
		if _, err := fmt.Fprintf(w, "$%d\r\n", len(arg)); err != nil {
			return err
		}
		if _, err := w.Write(arg); err != nil {
			return err
		}
		if _, err := io.WriteString(w, "\r\n"); err != nil {
			return err
		}
	}
	return nil
}

func readRESP(r *bufio.Reader) (any, error) {
	prefix, err := r.ReadByte()
	if err != nil {
		return nil, err
	}

	switch prefix {
	case '+':
		line, err := readLine(r)
		if err != nil {
			return nil, err
		}
		return line, nil
	case '-':
		line, err := readLine(r)
		if err != nil {
			return nil, err
		}
		return nil, errors.New(line)
	case '$':
		line, err := readLine(r)
		if err != nil {
			return nil, err
		}
		n, err := strconv.Atoi(line)
		if err != nil {
			return nil, err
		}
		if n < 0 {
			return "", nil
		}
		buf := make([]byte, n+2)
		if _, err := io.ReadFull(r, buf); err != nil {
			return nil, err
		}
		if string(buf[n:]) != "\r\n" {
			return nil, errors.New("invalid bulk string terminator")
		}
		return string(buf[:n]), nil
	case '*':
		line, err := readLine(r)
		if err != nil {
			return nil, err
		}
		n, err := strconv.Atoi(line)
		if err != nil {
			return nil, err
		}
		if n < 0 {
			return []any(nil), nil
		}
		arr := make([]any, 0, n)
		for i := 0; i < n; i++ {
			v, err := readRESP(r)
			if err != nil {
				return nil, err
			}
			arr = append(arr, v)
		}
		return arr, nil
	default:
		return nil, fmt.Errorf("unsupported RESP prefix %q", prefix)
	}
}

func readLine(r *bufio.Reader) (string, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return "", err
	}
	if !strings.HasSuffix(line, "\r\n") {
		return "", errors.New("invalid RESP line terminator")
	}
	return strings.TrimSuffix(line, "\r\n"), nil
}

func float32Bytes(values []float32) []byte {
	buf := make([]byte, len(values)*4)
	for i, v := range values {
		binary.LittleEndian.PutUint32(buf[i*4:], math.Float32bits(v))
	}
	return buf
}

func parseSearchResults(arr []any, withValues bool) ([]SearchResult, error) {
	results := make([]SearchResult, 0, len(arr))
	for _, item := range arr {
		row, ok := item.([]any)
		if !ok {
			return nil, fmt.Errorf("unexpected GETV row type: %T", item)
		}
		expected := 2
		if withValues {
			expected = 3
		}
		if len(row) != expected {
			return nil, fmt.Errorf("unexpected GETV row length: got %d want %d", len(row), expected)
		}
		member, err := asString(row[0])
		if err != nil {
			return nil, err
		}
		scoreText, err := asString(row[1])
		if err != nil {
			return nil, err
		}
		score, err := strconv.ParseFloat(scoreText, 64)
		if err != nil {
			return nil, err
		}
		result := SearchResult{Member: member, Score: score}
		if withValues {
			value, err := asString(row[2])
			if err != nil {
				return nil, err
			}
			result.Value = value
		}
		results = append(results, result)
	}
	return results, nil
}

func parseVectorInfo(fields []any) (VectorInfo, error) {
	if len(fields)%2 != 0 {
		return VectorInfo{}, fmt.Errorf("invalid VINFO field count: %d", len(fields))
	}
	m := make(map[string]string, len(fields)/2)
	for i := 0; i < len(fields); i += 2 {
		k, err := asString(fields[i])
		if err != nil {
			return VectorInfo{}, err
		}
		v, err := asString(fields[i+1])
		if err != nil {
			return VectorInfo{}, err
		}
		m[k] = v
	}

	dim, err := parseUint32Field(m, "dim")
	if err != nil {
		return VectorInfo{}, err
	}
	blockSize, err := parseUint64Field(m, "block_size")
	if err != nil {
		return VectorInfo{}, err
	}
	blockCount, err := parseUint64Field(m, "block_count")
	if err != nil {
		return VectorInfo{}, err
	}
	totalCount, err := parseUint64Field(m, "total_count")
	if err != nil {
		return VectorInfo{}, err
	}
	activeCount, err := parseUint64Field(m, "active_count")
	if err != nil {
		return VectorInfo{}, err
	}
	deletedCount, err := parseUint64Field(m, "deleted_count")
	if err != nil {
		return VectorInfo{}, err
	}

	return VectorInfo{
		Name:         m["name"],
		Dim:          dim,
		Metric:       m["metric"],
		Index:        m["index"],
		BlockSize:    blockSize,
		BlockCount:   blockCount,
		TotalCount:   totalCount,
		ActiveCount:  activeCount,
		DeletedCount: deletedCount,
	}, nil
}

func parseUint32Field(m map[string]string, key string) (uint32, error) {
	v, err := parseUint64Field(m, key)
	if err != nil {
		return 0, err
	}
	if v > 1<<32-1 {
		return 0, fmt.Errorf("field %s overflows uint32", key)
	}
	return uint32(v), nil
}

func parseUint64Field(m map[string]string, key string) (uint64, error) {
	text, ok := m[key]
	if !ok {
		return 0, fmt.Errorf("missing VINFO field %s", key)
	}
	v, err := strconv.ParseUint(text, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid VINFO field %s=%q: %w", key, text, err)
	}
	return v, nil
}

func asString(v any) (string, error) {
	s, ok := v.(string)
	if !ok {
		return "", fmt.Errorf("unexpected string field type: %T", v)
	}
	return s, nil
}

func main() {
	addr := "127.0.0.1:2000"
	if len(os.Args) >= 2 {
		addr = os.Args[1]
	}

	client, err := NewKVSClient(addr)
	if err != nil {
		fmt.Printf("connect %s failed: %v\n", addr, err)
		os.Exit(1)
	}
	defer client.Close()

	must := func(name string, err error) {
		if err != nil {
			fmt.Printf("%s failed: %v\n", name, err)
			os.Exit(1)
		}
		fmt.Printf("%s OK\n", name)
	}

	// 普通 KV 接口样例。
	must("SET qa:1001", client.Set("qa:1001", "下面是一个 C 语言红黑树实现……"))
	must("SET qa:2088", client.Set("qa:2088", "这里是另一个相关回答……"))
	must("SETEX session:1", client.SetEX("session:1", 60, "temporary-value"))

	value, err := client.Get("qa:1001")
	must("GET qa:1001", err)
	fmt.Printf("GET qa:1001 => %s\n", value)

	exists, err := client.Exist("qa:1001")
	must("EXIST qa:1001", err)
	fmt.Printf("EXIST qa:1001 => %v\n", exists)

	must("MOD qa:2088", client.Mod("qa:2088", "这里是另一个相关回答，已修改。"))
	must("DEL session:1", client.Del("session:1"))

	// Vector 接口样例。生产里 DIM 一般是 768/1024/1536，这里用 2 方便演示。
	// collection 名带时间戳，方便 main 样例重复运行。
	collection := fmt.Sprintf("qa_index_go_demo_%d", time.Now().UnixNano())
	must("CREATEV", client.CreateVector(collection, 2, "COSINE", "FLAT"))
	must("SETV qa:1001", client.SetVectorFloat32(collection, "qa:1001", []float32{1, 0}))
	must("SETV WITH VALUE qa:2088", client.SetVectorFloat32WithValue(collection, "qa:2088", []float32{0.8, 0.2}, "这里是另一个相关回答，来自 SETV VALUE。"))

	results, err := client.SearchVector(collection, []float32{1, 0}, 2)
	must("GETV TOPK", err)
	fmt.Printf("GETV TOPK => %+v\n", results)

	resultsWithValues, err := client.SearchVectorWithValues(collection, []float32{1, 0}, 2)
	must("GETV TOPK WITHVALUES", err)
	fmt.Printf("GETV TOPK WITHVALUES => %+v\n", resultsWithValues)

	info, err := client.VectorInfo(collection)
	must("VINFO", err)
	fmt.Printf("VINFO => %+v\n", info)

	must("DELV qa:1001", client.DeleteVector(collection, "qa:1001"))

	// QA 语义缓存接口样例。要求服务端 [embedding].enabled=true，并且 embedding_server 已能加载模型。
	// 如果服务端用 KVS_EMBEDDING_MOCK=1 启动，也可以用 mock embedding 验证接口流程。
	qaIndex := fmt.Sprintf("qa_go_demo_%d", time.Now().UnixNano())
	member, err := client.SetQAAuto(qaIndex, "1美元多少美分", "1美元等于100美分")
	if err != nil {
		fmt.Printf("SETQA AUTO skipped/failed: %v\n", err)
	} else {
		fmt.Printf("SETQA AUTO => %s\n", member)

		must("SETQA ID", client.SetQAID(qaIndex, "qa:biz1001", "redis setex 是什么", "SETEX 用于设置带过期时间的 key。"))

		qaResults, err := client.GetQA(qaIndex, "one dollar how many cents", 2)
		must("GETQA", err)
		fmt.Printf("GETQA => %+v\n", qaResults)

		must("DELQA", client.DelQA(qaIndex, member))
	}
}
