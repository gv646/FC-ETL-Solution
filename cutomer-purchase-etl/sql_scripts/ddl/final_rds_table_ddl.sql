CREATE TABLE customer_purchase_summary (
    customer_id INT PRIMARY KEY,
    full_name VARCHAR(255),
    email VARCHAR(255),
    total_spent_aud DECIMAL(18, 2),
    first_purchase_date DATE,
    last_purchase_date DATE,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

CREATE TABLE customer_purchase_summary_staging (
    customer_id INT,
    full_name VARCHAR(255),
    email VARCHAR(255),
    total_spent_aud DECIMAL(18, 2),
    first_purchase_date DATE,
    last_purchase_date DATE,
    load_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_total_spent_desc 
ON customer_purchase_summary (total_spent_aud DESC);