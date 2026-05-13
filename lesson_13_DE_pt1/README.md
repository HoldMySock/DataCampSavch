# ETL Pipeline

Reads four raw CSVs, cleans them, loads them into a SQLite database, and
creates five analytical output tables.

## Data cleaning

### customers.csv

    1. Drop rows where customer_id is missing
    2. Format customer_id to int
    3. Remove duplicate customer_ids
    4. Set invalid or missing emails to NULL
    5. Parse created_at

---

### products.csv

    1. Remove duplicate product_ids
    2. Drop rows with negative price
    3. Fill missing name / category with placeholder

---

### orders.csv

    1. Remove duplicate order_ids
    2. Drop rows with missing customer_id
    3. Normalise status to lowercase
    4. Log unknown statuses
    5. Drop orders referencing a customer_id not in customers table
    6. Parse created_at

---

### order_items.csv

    1. Remove duplicate order_item_ids
    2. Drop rows with negativee quantity
    3. Drop rows referencing orders not in orders table
    4. Drop rows referencing products not in products table


## Output tables

All tables in `output/store.db`, SQLite

### Cleaned tables

`customers`
`products`
`orders`
`order_items`

### Analytical tables

#### `order_summary`
Joins `order_items` + `products` to compute revenue.

`order_id` Order key
`customer_id` Customer foreign key
`order_status` normalised status
`order_date` order timestamp
`line_items` number of distinct product line
`total_units` sum of all quantities
`total_revenue` sum(quantity × unit_price)

---

#### `customer_stats`
Lifetime value and order history.

`customer_id` Customer key
`email`
`country`
`registered_at` account creation date
`total_orders` all orders count
`lifetime_value` sum of revenue across all orders
`first_order_date` / `last_order_date` date range of activity

---

#### `product_performance`
Units sold and revenue excluding canceled orders.

`product_id` Product key
`name` / `category` / `unit_price` product attributes
`orders_containing` how many distinct orders included this product
`total_units_sold` total quantity
`total_revenue` total revenue

---

#### `monthly_revenue`
Revenue by calendar month and order status.

`month` | `YYYY-MM`
`order_status` completed or pending
`orders` count of orders
`revenue` total revenue

---

#### `category_summary`
Revenue roll-up by product category excluding cancelled/returned orders.

`category` product category
`products` distinct product count
`total_units_sold` quantity sold
`total_revenue` total revenue