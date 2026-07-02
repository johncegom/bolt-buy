package main

import (
	"context"
	"database/sql"
	"errors"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

var luaDeductStock = redis.NewScript(`
	local stockKey = KEYS[1]
	local currentStock = redis.call("get", stockKey)

	if not currentStock or tonumber(currentStock) <= 0 then
		return 0
	end

	redis.call("decr", stockKey)
	return 1
`)

func Purchase(ctx context.Context, db *sql.DB, rdb *redis.Client, productID int, userID int, idempotencyKey string) error {
	idKey := "idempotency:" + idempotencyKey

	setSuccess, err := rdb.SetNX(ctx, idKey, "PROCESSING", 10*time.Second).Result()
	if err != nil {
		return err
	}

	if !setSuccess {
		status, err := rdb.Get(ctx, idKey).Result()
		if err != nil {
			return err
		}
		if status == "PROCESSING" {
			return errors.New("request conflict: transaction is already in-flight or completed")
		}
		if status == "SUCCESS" {
			return nil
		}
	}

	removeIdempotencyToken := func() {
		_ = rdb.Del(ctx, idKey).Err()
	}

	stockKey := "product:" + strconv.Itoa(productID) + ":stock"

	result, err := luaDeductStock.Run(ctx, rdb, []string{stockKey}).Result()
	if err != nil {
		removeIdempotencyToken()
		return err
	}

	if result.(int64) == 0 {
		removeIdempotencyToken()
		return errors.New("operational failure: product is out of stock in cache")
	}

	rollbackSystem := func() {
		_ = rdb.Incr(ctx, stockKey).Err()
		removeIdempotencyToken()
	}

	tx, err := db.Begin()
	if err != nil {
		rollbackSystem()
		return err
	}

	defer tx.Rollback()

	var orderCount int
	err = tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM orders WHERE product_id = $1 AND user_id = $2", productID, userID).Scan(&orderCount)
	if err != nil {
		rollbackSystem()
		return err
	}
	if orderCount > 0 {
		rollbackSystem()
		return errors.New("purchase limit exceeded: user has already bought this product")
	}

	_, err = tx.ExecContext(ctx, "UPDATE products SET stock = stock - 1 WHERE id = $1", productID)
	if err != nil {
		rollbackSystem()
		return err
	}

	_, err = tx.ExecContext(ctx, "INSERT INTO orders (product_id, user_id) VALUES ($1, $2)", productID, userID)
	if err != nil {
		rollbackSystem()
		return err
	}

	err = tx.Commit()
	if err != nil {
		rollbackSystem()
		return err
	}

	err = rdb.Set(ctx, idKey, "SUCCESS", 24*time.Hour).Err()
	if err != nil {
		return err
	}

	return nil
}

func main() {

}
