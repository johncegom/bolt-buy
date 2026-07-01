package main

import (
	"database/sql"
	"testing"
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
