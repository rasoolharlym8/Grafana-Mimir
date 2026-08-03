package queryrange

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

// Request representa a query PromQL ou fragmento de query
type Request interface {
	Hash() string
}

// QueryRequest implementa a interface Request para testes
type QueryRequest struct {
	QueryString string
}

func (q QueryRequest) Hash() string {
	return q.QueryString // Simplificado para fins de chave
}

// Response representa a resposta com os dados retornados
type Response struct {
	Data       string
	IsPartial  bool
	ShardEpoch int64
}

// Cache interface que define as operações com o Memcached/Redis
type Cache interface {
	Get(ctx context.Context, key string) (*Response, bool)
	Set(ctx context.Context, key string, resp *Response) error
}

// MemoryCache implementa Cache em memória thread-safe para os testes
type MemoryCache struct {
	mu    sync.RWMutex
	store map[string]*Response
}

func NewMemoryCache() *MemoryCache {
	return &MemoryCache{
		store: make(map[string]*Response),
	}
}

func (c *MemoryCache) Get(ctx context.Context, key string) (*Response, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	resp, ok := c.store[key]
	return resp, ok
}

func (c *MemoryCache) Set(ctx context.Context, key string, resp *Response) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.store[key] = resp
	return nil
}

// TenantRoutingTable gerencia de forma thread-safe as topologias/epochs dos shards dos tenants
type TenantRoutingTable struct {
	mu     sync.RWMutex
	epochs map[string]int64
}

func NewTenantRoutingTable() *TenantRoutingTable {
	return &TenantRoutingTable{
		epochs: make(map[string]int64),
	}
}

func (t *TenantRoutingTable) GetEpoch(tenantID string) int64 {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.epochs[tenantID]
}

func (t *TenantRoutingTable) UpdateEpoch(tenantID string) int64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.epochs[tenantID]++
	return t.epochs[tenantID]
}

// QueryFrontend orquestra a lógica de caching e execução das queries
type QueryFrontend struct {
	cache        Cache
	routingTable *TenantRoutingTable
	// Contador de queries executadas com sucesso
	SuccessfulQueries int64
}

func NewQueryFrontend(cache Cache, rt *TenantRoutingTable) *QueryFrontend {
	return &QueryFrontend{
		cache:        cache,
		routingTable: rt,
	}
}

// GenerateCacheKey cria uma chave de cache topology-aware incorporando o epoch do tenant
func (q *QueryFrontend) GenerateCacheKey(tenantID string, epoch int64, req Request) string {
	var sb strings.Builder
	hashStr := req.Hash()
	// Preallocate: len("tenant:") (7) + len(tenantID) + len(":epoch:") (7) + ~10 (epoch) + len(":query:") (7) + len(hashStr)
	sb.Grow(7 + len(tenantID) + 7 + 10 + 7 + len(hashStr))
	sb.WriteString("tenant:")
	sb.WriteString(tenantID)
	sb.WriteString(":epoch:")
	sb.WriteString(strconv.FormatInt(epoch, 10))
	sb.WriteString(":query:")
	sb.WriteString(hashStr)
	return sb.String()
}

// ExecuteQuery com cache topology-aware e invalidação mid-flight thread-safe
func (q *QueryFrontend) ExecuteQuery(ctx context.Context, tenantID string, req Request, executor func() (*Response, error)) (*Response, error) {
	// 1. Obter o epoch da topologia atual no início da query de forma thread-safe
	startEpoch := q.routingTable.GetEpoch(tenantID)

	// 2. Tentar buscar do cache usando a chave topology-aware
	cacheKey := q.GenerateCacheKey(tenantID, startEpoch, req)
	if resp, ok := q.cache.Get(ctx, cacheKey); ok {
		// Validar se o resultado retornado do cache é consistente com a topologia do início
		if resp.ShardEpoch == startEpoch && !resp.IsPartial {
			atomic.AddInt64(&q.SuccessfulQueries, 1)
			return resp, nil
		}
	}

	// 3. Executar o executor real da query (simulando a busca nos queriers downstream)
	resp, err := executor()
	if err != nil {
		return nil, err
	}

	// 4. Capturar o epoch atual após a execução para validar transições de topologia mid-flight
	endEpoch := q.routingTable.GetEpoch(tenantID)

	// Se houve rebalanceamento de shards/mudança de epoch durante a execução,
	// a resposta pode ser parcial ou inconsistente. Devemos abortar a escrita no cache
	// e retornar erro para forçar o client/frontend a fazer bypass ou retry da query.
	if startEpoch != endEpoch {
		return nil, errors.New("query execution aborted: tenant routing topology changed mid-flight")
	}

	if resp.IsPartial {
		return nil, errors.New("query returned partial data due to inconsistent shard state")
	}

	// Injetar o epoch atualizado na resposta
	resp.ShardEpoch = endEpoch

	// 5. Salvar a resposta no cache apenas se a topologia continuar consistente
	if err := q.cache.Set(ctx, cacheKey, resp); err == nil {
		atomic.AddInt64(&q.SuccessfulQueries, 1)
	}

	return resp, nil
}
