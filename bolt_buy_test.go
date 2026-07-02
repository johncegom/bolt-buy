package main

import (
	"context"
	"database/sql"
	"strconv"
	"sync"
	"testing"

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

	err := Purchase(ctx, db, rdb, 1, 42)
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
			err := Purchase(ctx, db, rdb, productID, userID)
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

	err := Purchase(ctx, db, rdb, productID, userID)
	if err != nil {
		t.Fatalf("First purchase failed unexxpectedly: %v", err)
	}

	err = Purchase(ctx, db, rdb, productID, userID)
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
