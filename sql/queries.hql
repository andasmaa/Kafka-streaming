SHOW DATABASES;
SELECT COUNT(*) AS bronze_rows FROM bronze.invoices_raw;
SELECT COUNT(*) AS silver_rows FROM silver.invoices_clean;
SELECT * FROM gold.category_sales ORDER BY total_amount DESC;
