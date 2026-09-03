-- =====================================================
-- Issue 1: DUPLICATE CUSTOMER RECORDS
-- =====================================================
-- Multiple signups per customer_id detected.

SELECT
    customer_id,
    COUNT(*) AS occurrences
FROM vw_raw_customers
GROUP BY customer_id
HAVING COUNT(*) > 1;


-- =====================================================
-- Issue 2: MISSING CONTACT INFO
-- =====================================================
-- Customers without email or phone.
-- Impact: Cannot contact customer for support/marketing.
-- Resolution: Fill missing emails with 'unknown@domain.com'.
SELECT
    customer_id,
    full_name,
    email,
    phone
FROM vw_raw_customers
WHERE email IS NULL
   OR TRIM(email) = ''
   OR phone IS NULL
   OR TRIM(phone) = '';


-- =====================================================
-- Issue 3: INVALID ORDER AMOUNTS (SYSTEM ERRORS)
-- =====================================================
-- Orders with negative total_amount.
-- Resolution: Filter total_amount > 0 in pipeline.
SELECT
    order_id,
    customer_id,
    total_amount,
    currency,
    status
FROM vw_raw_orders
WHERE total_amount < 0;


-- =====================================================
-- Issue 4: NEW CUSTOMERS
-- =====================================================
-- Orders with customer_id not found in vw_raw_customers.
-- Impact: Cannot attribute revenue to customer.

SELECT
    o.order_id,
    o.customer_id,
    o.total_amount,
    o.currency
FROM vw_raw_orders o
LEFT JOIN vw_raw_customers c
    ON o.customer_id = c.customer_id
WHERE c.customer_id IS NULL;
