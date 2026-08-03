package queryrange

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

// TestTopologyAwareCacheKeys garante que chaves de cache variam quando a topologia muda
func TestTopologyAwareCacheKeys(t *testing.T) {
	cache := NewMemoryCache()
	rt := NewTenantRoutingTable()
	qf := NewQueryFrontend(cache, rt)

	tenantID := "tenant-1"
	req := QueryRequest{QueryString: "sum(up)"}

	epoch1 := rt.GetEpoch(tenantID)
	key1 := qf.GenerateCacheKey(tenantID, epoch1, req)

	// Atualizar topologia (epoch)
	rt.UpdateEpoch(tenantID)

	epoch2 := rt.GetEpoch(tenantID)
	key2 := qf.GenerateCacheKey(tenantID, epoch2, req)

	if key1 == key2 {
		t.Errorf("Chaves de cache devem diferir após a mudança de topologia: %s vs %s", key1, key2)
	}
}

// TestMidFlightShardTransition garante que escritas de cache sejam abortadas se a topologia mudar mid-flight
func TestMidFlightShardTransition(t *testing.T) {
	cache := NewMemoryCache()
	rt := NewTenantRoutingTable()
	qf := NewQueryFrontend(cache, rt)

	tenantID := "tenant-1"
	req := QueryRequest{QueryString: "rate(http_requests_total[5m])"}

	// Executor simula latência e rebalanceamento concorrente mid-flight
	executor := func() (*Response, error) {
		time.Sleep(50 * time.Millisecond) // Simular processamento
		return &Response{Data: "complete-results", IsPartial: false}, nil
	}

	// Iniciar a query em background
	var err error
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		_, err = qf.ExecuteQuery(context.Background(), tenantID, req, executor)
	}()

	// Simular rebalanceamento no meio da execução
	time.Sleep(20 * time.Millisecond)
	rt.UpdateEpoch(tenantID)

	wg.Wait()

	// O query-frontend deve retornar erro e abortar o salvamento de cache/resultado parcial
	if err == nil {
		t.Error("Esperava erro devido à alteração de topologia mid-flight, mas retornou sucesso")
	}

	// Verificar se nada foi salvo no cache para a topologia antiga ou nova
	startKey := qf.GenerateCacheKey(tenantID, 0, req)
	newKey := qf.GenerateCacheKey(tenantID, 1, req)

	if _, ok := cache.Get(context.Background(), startKey); ok {
		t.Error("Não deveria haver cache populado para a chave antiga")
	}
	if _, ok := cache.Get(context.Background(), newKey); ok {
		t.Error("Não deveria haver cache populado para a chave nova")
	}
}

// TestConcurrentReadWriteAndRebalance roda testes de stress concorrente com o race detector ativo
func TestConcurrentReadWriteAndRebalance(t *testing.T) {
	cache := NewMemoryCache()
	rt := NewTenantRoutingTable()
	qf := NewQueryFrontend(cache, rt)

	tenantID := "tenant-1"
	req := QueryRequest{QueryString: "prometheus_engine_query_duration_seconds"}

	var wg sync.WaitGroup
	ctx := context.Background()

	// 1. Iniciar workers que consultam a query continuamente
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				executor := func() (*Response, error) {
					// Simular processamento leve
					return &Response{Data: fmt.Sprintf("val-%d", j), IsPartial: false}, nil
				}
				_, _ = qf.ExecuteQuery(ctx, tenantID, req, executor)
			}
		}(i)
	}

	// 2. Iniciar worker concorrente que altera a topologia (rebalanceamento)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 10; i++ {
			time.Sleep(10 * time.Millisecond)
			rt.UpdateEpoch(tenantID)
		}
	}()

	wg.Wait()
}
