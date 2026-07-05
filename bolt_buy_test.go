package main

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	_ "github.com/lib/pq"
	"github.com/redis/go-redis/v9"
)

func setupTestInfra(t *testing.T, ctx context.Context) (*sql.DB, *redis.Client) {
	connStr := "postgres://bolt_user:bolt_password@localhost:6378/bolt_buy?sslmode=disable"
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		t.Fatalf("Failed to connect to DB: %v", err)
	}

	_, _ = db.Exec("DROP TABLE IF EXISTS orders;")
	_, _ = db.Exec("DROP TABLE IF EXISTS products;")
	_, _ = db.Exec(`CREATE TABLE products (id SERIAL PRIMARY KEY, name VARCHAR(255), stock INT);`)
	_, _ = db.Exec(`CREATE TABLE orders (id SERIAL PRIMARY KEY, product_id INT, user_id INT);`)

	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	if err := rdb.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("Failed to flush Redis: %v", err)
	}

	return db, rdb
}

func TestPurchase_Success(t *testing.T) {
	ctx := context.Background()

	db, rdb := setupTestInfra(t, ctx)
	defer db.Close()
	defer rdb.Close()

	_, _ = db.Exec("INSERT INTO products (id, name, stock) VALUES (1, 'Mechanical Keyboard', 10);")
	_ = rdb.Set(ctx, "product:1:stock", 10, 0).Err()

	err := Purchase(ctx, db, rdb, 1, 42, "single-user-happy-path-key")
	if err != nil {
		t.Fatalf("Expected successful purchase, got error: %v", err)
	}

	// Assert Postgres state
	var stock int
	_ = db.QueryRow("SELECT stock FROM products WHERE id = 1").Scan(&stock)
	if stock != 9 {
		t.Errorf("Expected stock to be 9, got %d", stock)
	}

	// Assert Redis
	redisStockStr, _ := rdb.Get(ctx, "product:1:stock").Result()
	redisStock, _ := strconv.Atoi(redisStockStr)
	if redisStock != 9 {
		t.Errorf("Expecpted Redis stock to be 9, got %d", redisStock)
	}
}

func TestPurchase_Concurrent_RedisAndPostgres(t *testing.T) {
	ctx := context.Background()

	db, rdb := setupTestInfra(t, ctx)
	defer db.Close()
	defer rdb.Close()

	const initialStock = 10
	productID := 1

	_, _ = db.Exec("INSERT INTO products (id, name, stock) VALUES ($1, 'Flash Sale Sneakers', $2);", productID, initialStock)
	_ = rdb.Set(ctx, "product:"+strconv.Itoa(productID)+":stock", initialStock, 0).Err()

	const totalUsers = 50
	var wg sync.WaitGroup
	errChan := make(chan error, totalUsers)

	for i := 1; i <= totalUsers; i++ {
		wg.Add(1)
		go func(userID int) {
			defer wg.Done()

			idKey := "concurrent-user-key" + strconv.Itoa(userID)
			err := Purchase(ctx, db, rdb, productID, userID, idKey)
			if err != nil {
				errChan <- err
			}
		}(i)
	}

	wg.Wait()
	close(errChan)

	var finalDBStock int
	_ = db.QueryRow("SELECT stock FROM products WHERE id = $1", productID).Scan(&finalDBStock)

	var totalOrders int
	_ = db.QueryRow("SELECT COUNT(*) FROM orders WHERE product_id = $1", productID).Scan(&totalOrders)

	redisStockStr, _ := rdb.Get(ctx, "product:"+strconv.Itoa(productID)+":stock").Result()
	finalRedisStock, _ := strconv.Atoi(redisStockStr)

	t.Logf("--> Results: DB Stock: %d | Redis Stock: %d | Orders Created: %d", finalDBStock, finalRedisStock, totalOrders)

	if finalDBStock != 0 || finalRedisStock != 0 {
		t.Errorf("Inventory mismatch. DB: %d, Redis: %d", finalDBStock, finalRedisStock)
	}

	if totalOrders != initialStock {
		t.Errorf("Invariant broken! Expected exactly %d orders, but got %d", initialStock, totalOrders)
	}

}

func TestPurchase_CacheDrift_OnFailure(t *testing.T) {
	ctx := context.Background()
	db, rdb := setupTestInfra(t, ctx)
	defer db.Close()
	defer rdb.Close()

	const initialStock = 5
	productID := 1
	userID := 99

	_, _ = db.Exec("INSERT INTO products (id, name, stock) VALUES ($1, 'Rare Vinyl Record', $2);", productID, initialStock)
	_ = rdb.Set(ctx, "product:"+strconv.Itoa(productID)+":stock", initialStock, 0).Err()

	err := Purchase(ctx, db, rdb, productID, userID, "drift-test-key-first-attempt")
	if err != nil {
		t.Fatalf("First purchase failed unexxpectedly: %v", err)
	}

	err = Purchase(ctx, db, rdb, productID, userID, "drift-test-key-second-attempt")
	if err == nil {
		t.Fatalf("Expected second purchase to fail for duplicate user, but it succeed")
	}

	var dbStock int
	_ = db.QueryRow("SELECT stock FROM products WHERE id = $1", productID).Scan(&dbStock)

	redisStockStr, _ := rdb.Get(ctx, "product:"+strconv.Itoa(productID)+":stock").Result()
	redisStock, _ := strconv.Atoi(redisStockStr)

	t.Logf("--> Drif Analysis: DB Stock: %d | Redis Stock: %d", dbStock, redisStock)

	if dbStock != redisStock {
		t.Errorf("CACHE DRIFT DETECTED! DB stock is %d, but Redis stock is %d", dbStock, redisStock)
	}
}

func TestPurchase_IdempotencyKey_ProductionBehavior(t *testing.T) {
	ctx := context.Background()
	db, rdb := setupTestInfra(t, ctx)
	defer db.Close()
	defer rdb.Close()

	const initialStock = 5
	productID := 1
	userID := 100
	idempotencyKey := "client-generated-uuid-v4-12345"
	targetKey := "idempotency:" + idempotencyKey

	_, _ = db.Exec("INSERT INTO products (id, name, stock) VALUES ($1, 'Limited Edition Console', $2);", productID, initialStock)
	_ = rdb.Set(ctx, "product:"+strconv.Itoa(productID)+":stock", initialStock, 0).Err()

	// -----------------------------------------------------
	// Test In-Flight Blocking ("PROCESSING")
	// -----------------------------------------------------

	_ = rdb.Set(ctx, targetKey, "PROCESSING", 10*time.Second).Err()

	err := Purchase(ctx, db, rdb, productID, userID, idempotencyKey)
	if err == nil {
		t.Fatalf("CRITICAL FLAW: Allowed an in-flight request retry to pass through")
	}

	expectedErr := "request conflict: transaction is already in-flight or completed"
	if err.Error() != expectedErr {
		t.Errorf("Unexpected error message. Got: '%v', Expected: '%v'", err.Error(), expectedErr)
	}

	_ = rdb.Del(ctx, targetKey).Err()

	// -----------------------------------------------------
	// Test Succesful Ingestion
	// -----------------------------------------------------
	err = Purchase(ctx, db, rdb, productID, userID, idempotencyKey)
	if err != nil {
		t.Fatalf("Initial genuine purchase failed unexpectedly: %v", err)
	}

	// -----------------------------------------------------
	// Test Completed Retry ("SUCCESS")
	// -----------------------------------------------------
	err = Purchase(ctx, db, rdb, productID, userID, idempotencyKey)
	if err != nil {
		t.Fatalf("Expected retry of completed transaction to return nil (cache success), got error : %v", err)
	}

	// -----------------------------------------------------
	// State Validation
	// -----------------------------------------------------

	var dbStock int
	_ = db.QueryRow("SELECT stock FROM products WHERE id = $1", productID).Scan(&dbStock)

	if dbStock != 4 {
		t.Errorf("INVARIANT VIOLATION: Completed retry mutated data! Expected stock to be 4, found: %d", dbStock)
	}

	var orderCount int
	_ = db.QueryRow("SELECT COUNT(*) FROM orders WHERE product_id = $1 AND user_id = $2", productID, userID).Scan(&orderCount)
	if orderCount != 1 {
		t.Errorf("INVARIANT VIOLATION: Completed retry created a duplicate order! Found count: %d", orderCount)
	}
}

func BenchmarkPurchase_Parallel(b *testing.B) {
	ctx := context.Background()

	db, rdb := setupTestInfra(nil, ctx)
	defer db.Close()
	defer rdb.Close()

	const massiveStock = 1000000
	productID := 999

	_, err := db.Exec("INSERT INTO products (id, name, stock) VALUES ($1, 'Load Test Item', $2);", productID, massiveStock)
	if err != nil {
		b.Fatalf("Failed to seed benchmark DB: %v", err)
	}

	err = rdb.Set(ctx, "product:"+strconv.Itoa(productID)+":stock", massiveStock, 0).Err()
	if err != nil {
		b.Fatalf("Failed to seed benchmark Redis: %v", err)
	}

	b.ResetTimer()

	var dynamicUserSequence int64 = 0

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			currentID := atomic.AddInt64(&dynamicUserSequence, 1)

			userID := int(currentID)
			idempotencyKey := fmt.Sprintf("bench-uuid-%d", currentID)

			_ = Purchase(ctx, db, rdb, productID, userID, idempotencyKey)
		}
	})
}
