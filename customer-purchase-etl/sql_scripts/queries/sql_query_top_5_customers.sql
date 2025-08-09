SELECT customer_id, 
       full_name, 
       email, 
       total_spent_aud
FROM customer_purchase_summary
ORDER BY total_spent_aud DESC
LIMIT 5;