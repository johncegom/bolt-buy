package main

import (
	"database/sql"
	"errors"
)

func Purchase(db *sql.DB, productID int, userID int) error {
	var orderCount int
	var stock int

	err := db.QueryRow("SELECT COUNT(*) FROM orders WHERE product_id = $1 AND user_id = $2", productID, userID).Scan(&orderCount)
	if err != nil {
		return err
	}
	if orderCount > 0 {
		return errors.New("purchase limit exceeded: user has already bought this product")
	}

	err = db.QueryRow("SELECT stock FROM products WHERE id = $1", productID).Scan(&stock)
	if err != nil {
		return err
	}
	if stock <= 0 {
		return errors.New("operational failure: product is out of stock")
	}

	_, err = db.Exec("UPDATE products SET stock = stock - 1 WHERE id = $1", productID)
	if err != nil {
		return err
	}

	_, err = db.Exec("INSERT INTO orders (product_id, user_id) VALUES ($1, $2)", productID, userID)
	if err != nil {
		return err
	}
	return nil
}

func main() {

}
