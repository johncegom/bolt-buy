package main

import (
	"database/sql"
	"sync"
	"testing"

	_ "github.com/lib/pq"
)

func setupTestDB(t *testing.T) *sql.DB {
	connStr := "postgres://bolt_user:bolt_password@localhost:6378/bolt_buy?sslmode=disable"
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		t.Fatalf("Failed to connect to DB: %v", err)
	}

	_, _ = db.Exec("DROP TABLE IF EXISTS orders;")
	_, _ = db.Exec("DROP TABLE IF EXISTS products;")

	_, err = db.Exec(`CREATE TABLE products (id SERIAL PRIMARY KEY, name VARCHAR(255), stock INT);`)
	if err != nil {
		t.Fatalf("Failed to create products table: %v", err)
	}
	_, err = db.Exec(`CREATE TABLE orders (id SERIAL PRIMARY KEY, product_id INT, user_id INT);`)
	if err != nil {
		t.Fatalf("Failed to create orders table: %v", err)
	}

	return db
}

func TestPurchase_Success(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	_, err := db.Exec("INSERT INTO products (id, name, stock) VALUES (1, 'Mechanical Keyboard', 10);")
	if err != nil {
		t.Fatalf("Failed to seed product: %v", err)
	}

	err = Purchase(db, 1, 42)
	if err != nil {
		t.Fatalf("Expected successful purchase, got error: %v", err)
	}

	// Assertion 1: Stock must be decremented by 1
	var stock int
	err = db.QueryRow("SELECT stock FROM products WHERE id = 1").Scan(&stock)

	if err != nil {
		t.Fatalf("Failed to check stock: %v", err)
	}
	if stock != 9 {
		t.Errorf("Expected stock to be 9, got %d", stock)
	}

	// Asserrtion 2: An order record must exist for this user
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM orders WHERE product_id = 1 AND user_id = 42").Scan(&count)
	if err != nil {
		t.Fatalf("Failed to check orders: %v", err)
	}
	if count != 1 {
		t.Errorf("Expected 1 order to be created, got %d", count)
	}
}

func TestPurchase_Concurrent_RaceCondition(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	const initialStock = 10
	_, err := db.Exec("INSERT INTO products (id, name, stock) VALUES (1, 'Flash Sale Sneakers', $1)", initialStock)
	if err != nil {
		t.Fatalf("Failed to seed product: %v", err)
	}

	const totalUsers = 50
	var wg sync.WaitGroup
	errChan := make(chan error, totalUsers)

	for i := 1; i <= totalUsers; i++ {
		wg.Add(1)
		go func(userID int) {
			defer wg.Done()
			err := Purchase(db, 1, userID)
			if err != nil {
				errChan <- err
			}
		}(i)
	}

	wg.Wait()
	close(errChan)

	var failedPurchases int
	for range errChan {
		failedPurchases++
	}

	var finalStock int
	_ = db.QueryRow("SELECT stock FROM products WHERE id = 1").Scan(&finalStock)

	var totalOrders int
	_ = db.QueryRow("SELECT COUNT(*) FROM orders WHERE product_id = 1").Scan(&totalOrders)

	t.Logf("--> Results: Initial Stock: %d | Final Stock: %d | Total Orders Created: %d", initialStock, finalStock, totalOrders)

	if finalStock < 0 {
		t.Errorf("CRITICAL INVARIANT VIOLATION: Negative stock detected! Final stock: %d", finalStock)
	}

	if totalOrders > initialStock {
		t.Errorf("CRITICAL INVARIANT VIOLATION: Oversold! Sold %d items when only %d existed", totalOrders, initialStock)
	}

}
