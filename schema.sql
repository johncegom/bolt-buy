CREATE TABLE products (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    stock INT NOT NULL
);

CREATE TABLE orders (
    ID SERIAL PRIMARY KEY,
    product_id INT REFERENCES products(id),
    user_id INT NOT NULL
)