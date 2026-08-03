package main

import (
	"context"
	"fmt"
	"time"

	"github.com/rasoolharlym8/Grafana-Mimir/pkg/queryfrontend/queryrange"
)

func main() {
	fmt.Println("🚀 Iniciando Simulação do Query Frontend (Grafana-Mimir)...")

	cache := queryrange.NewMemoryCache()
	rt := queryrange.NewTenantRoutingTable()
	qf := queryrange.NewQueryFrontend(cache, rt)

	tenantID := "tenant-alpha"
	req := queryrange.QueryRequest{QueryString: "sum(rate(container_cpu_usage_seconds_total[5m]))"}

	ctx := context.Background()

	// 1. Primeira Execução (Cache Miss -> Salva no Cache)
	executor1 := func() (*queryrange.Response, error) {
		return &queryrange.Response{Data: "cpu-usage-data-v1", IsPartial: false}, nil
	}
	resp1, err := qf.ExecuteQuery(ctx, tenantID, req, executor1)
	if err != nil {
		fmt.Printf("❌ Erro 1: %v\n", err)
	} else {
		fmt.Printf("✅ Query 1 executada: %s (Epoch: %d)\n", resp1.Data, resp1.ShardEpoch)
	}

	// 2. Segunda Execução (Cache Hit na mesma topologia)
	executor2 := func() (*queryrange.Response, error) {
		return &queryrange.Response{Data: "cpu-usage-data-v2", IsPartial: false}, nil
	}
	resp2, err := qf.ExecuteQuery(ctx, tenantID, req, executor2)
	if err != nil {
		fmt.Printf("❌ Erro 2: %v\n", err)
	} else {
		fmt.Printf("✅ Query 2 (Cache Hit): %s (Epoch: %d)\n", resp2.Data, resp2.ShardEpoch)
	}

	// 3. Simular Rebalanceamento do Shard (Muda o Epoch de roteamento do tenant)
	fmt.Println("\n🔄 Disparando rebalanceamento de shards (Shuffle-Sharding Rebalance)...")
	rt.UpdateEpoch(tenantID)

	// 4. Terceira Execução (Cache Miss na nova topologia, pois a chave rotacionou!)
	executor3 := func() (*queryrange.Response, error) {
		return &queryrange.Response{Data: "cpu-usage-data-v3-new-shards", IsPartial: false}, nil
	}
	resp3, err := qf.ExecuteQuery(ctx, tenantID, req, executor3)
	if err != nil {
		fmt.Printf("❌ Erro 3: %v\n", err)
	} else {
		fmt.Printf("✅ Query 3 (Nova Topologia): %s (Epoch: %d)\n", resp3.Data, resp3.ShardEpoch)
	}

	// 5. Simular Alteração Mid-Flight (Rebalanceamento ocorre durante a execução da query)
	fmt.Println("\n⚡ Iniciando query com rebalanceamento concorrente mid-flight...")
	req2 := queryrange.QueryRequest{QueryString: "sum(rate(container_cpu_usage_seconds_total[10m]))"}
	executor4 := func() (*queryrange.Response, error) {
		time.Sleep(10 * time.Millisecond) // Simular latência
		return &queryrange.Response{Data: "partial-data", IsPartial: false}, nil
	}

	go func() {
		time.Sleep(5 * time.Millisecond)
		rt.UpdateEpoch(tenantID) // Rebalanceamento mid-flight!
	}()

	resp4, err := qf.ExecuteQuery(ctx, tenantID, req2, executor4)
	if err != nil {
		fmt.Printf("🛡️  Sucesso Guardrail: Execução abortada corretamente: %v\n", err)
	} else {
		fmt.Printf("❌ Falha Guardrail: Query aceitou dados inconsistentes: %s (Epoch: %d)\n", resp4.Data, resp4.ShardEpoch)
	}

	fmt.Printf("\n📊 Total de queries bem-sucedidas registradas no Query Frontend: %d\n", qf.SuccessfulQueries)
}
