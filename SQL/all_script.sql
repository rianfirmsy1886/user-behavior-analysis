-- user summary
SELECT  
id,
gender,
current_age,
retirement_age, 
birth_month, 
birth_year, 
per_capita_income, 
yearly_income,
total_debt,
credit_score,
num_credit_cards
FROM read_csv_auto('/Users/mac/Downloads/Rian/users_data.csv')
;

--segmentatis income & credit score
SELECT  
id,
  yearly_income,
  credit_score,
  CASE 
    WHEN (REGEXP_REPLACE(yearly_income, '[$,]', '', 'g'))::numeric >= 100000 THEN 'High Income'
    WHEN (REGEXP_REPLACE(yearly_income, '[$,]', '', 'g'))::numeric >= 50000 THEN 'Mid Income'
    ELSE 'Low Income'
  END AS income_segment,
  CASE 
    WHEN credit_score >= 750 THEN 'Excellent'
    WHEN credit_score >= 650 THEN 'Good'
    WHEN credit_score >= 550 THEN 'Fair'
    ELSE 'Poor'
  END AS credit_segment
FROM read_csv_auto('/Users/mac/Downloads/Rian/users_data.csv');


--segmenation based on adress high income

select
address, latitude, longitude,
  COUNT(*) AS user_count,
  AVG((REGEXP_REPLACE(yearly_income, '[$,]', '', 'g'))::numeric) AS avg_income FROM read_csv_auto('/Users/mac/Downloads/Rian/users_data.csv')
WHERE (REGEXP_REPLACE(yearly_income, '[$,]', '', 'g'))::numeric >= 100000
GROUP BY address, latitude, longitude
ORDER BY avg_income DESC
  ;

--Total transaksi per user dan average amount

SELECT 
  ud.id AS client_id,
  COUNT(td.id) AS total_txn,
  CAST(
    SUM(
      CAST(REPLACE(REPLACE(td.amount, '$', ''), ',', '') AS DOUBLE)
    ) AS BIGINT
  ) AS total_spent,
  CAST(
    ROUND(
      AVG(
        CAST(REPLACE(REPLACE(td.amount, '$', ''), ',', '') AS DOUBLE)
      )
    ) AS BIGINT
  ) AS avg_txn_amount,
  MAX(td.date) AS last_txn_date
FROM read_csv(
     '/Users/mac/Downloads/transactions_data.csv',
     delim=',',
     header=TRUE,
     quote='"',
     null_padding=TRUE,
     ignore_errors=TRUE
) AS td
JOIN read_csv(
     '/Users/mac/Downloads/Rian/users_data.csv',
     delim=',',
     header=TRUE,
     quote='"'
) AS ud
  ON td.client_id = ud.id
GROUP BY ud.id;

-- trx chip non chip

SELECT 
  use_chip,
  COUNT(*) AS txn_count,
  CAST(
    ROUND(
      AVG(
        CAST(REPLACE(REPLACE(amount, '$', ''), ',', '') AS DOUBLE)
      )
    ) AS BIGINT
  ) AS avg_txn_amount,
  CAST(
    SUM(
      CAST(REPLACE(REPLACE(amount, '$', ''), ',', '') AS DOUBLE)
    ) AS BIGINT
  ) AS total_spent
FROM read_csv(
     '/Users/mac/Downloads/transactions_data.csv',
     delim=',',
     header=TRUE,
     quote='"',
     null_padding=TRUE,
     ignore_errors=TRUE
)
GROUP BY use_chip;

--txn by city
SELECT 
  merchant_city,
  merchant_id,
  CAST(
    ROUND(
      COUNT(
        CAST(REPLACE(REPLACE(amount, '$', ''), ',', '') AS DOUBLE)
      )
    ) AS BIGINT
  ) AS txn_count,
  CAST(
    SUM(
      CAST(REPLACE(REPLACE(amount, '$', ''), ',', '') AS DOUBLE)
    ) AS BIGINT
  ) AS total_amount
FROM read_csv(
     '/Users/mac/Downloads/transactions_data.csv',
     delim=',',
     header=TRUE,
     quote='"',
     null_padding=TRUE
     )
GROUP BY merchant_city, merchant_id
ORDER BY total_amount DESC;

-- declining trx freq
WITH user_txn AS (
  SELECT 
    client_id,
    DATE_TRUNC('month', date) AS month,
    COUNT(*) AS txns
  FROM read_csv(
     '/Users/mac/Downloads/transactions_data.csv',
     delim=',',
     header=TRUE,
     quote='"',
     null_padding=TRUE
  )
  GROUP BY client_id, DATE_TRUNC('month', date)
),
ranked AS (
  SELECT 
    client_id,
    month,
    txns,
    ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY month DESC) AS rn
  FROM user_txn
),
latest AS (
  SELECT client_id, txns AS latest_txns
  FROM ranked
  WHERE rn = 1
),
previous AS (
  SELECT client_id, txns AS prev_txns
  FROM ranked
  WHERE rn = 2
)
SELECT 
  l.client_id
FROM latest l
JOIN previous p ON l.client_id = p.client_id
WHERE l.latest_txns < p.prev_txns;


--ratio utang terhadap limit kartu

WITH user_card_spend AS (
  SELECT 
    cd.id as card_id,
    cd.client_id,
    CAST(REPLACE(REPLACE(cd.credit_limit, '$', ''), ',', '') AS DOUBLE) AS credit_limit,
    ROUND(
      SUM(
        CAST(REPLACE(REPLACE(td.amount, '$', ''), ',', '') AS DOUBLE)
      ), 0
    ) AS total_spent
  FROM read_csv_auto('/Users/mac/Downloads/cards_data.csv') AS cd
  LEFT JOIN read_csv(
     '/Users/mac/Downloads/transactions_data.csv',
     delim=',',
     header=TRUE,
     quote='"',
     null_padding=TRUE
  )
  AS td 
    ON cd.id = td.card_id
  GROUP BY cd.id, cd.client_id, cd.credit_limit
)
SELECT 
  card_id,
  client_id,
  credit_limit,
  total_spent,
  CASE 
    WHEN credit_limit > 0 THEN total_spent / credit_limit
    ELSE NULL
  END AS utilization_ratio
FROM user_card_spend;

-- low credit score but most card issued
SELECT 
  ud.id AS client_id,
  ud.credit_score,
  CAST(
    ROUND(
      AVG(
        CAST(REPLACE(REPLACE(cd.credit_limit, '$', ''), ',', '') AS DOUBLE)
      )
    ) AS BIGINT
  ) AS avg_credit_limit
FROM read_csv_auto(
     '/Users/mac/Downloads/Rian/users_data.csv',
     delim=',',
     header=TRUE,
     quote='"'
) AS ud
JOIN read_csv_auto('/Users/mac/Downloads/cards_data.csv') AS cd
  ON ud.id = cd.client_id
GROUP BY ud.id, ud.credit_score
HAVING COUNT(cd.id) >= 3 
   AND ud.credit_score < 600;

WITH txn_count AS (
  SELECT client_id, COUNT(*) AS num_txn
  FROM trx_data
  GROUP BY client_id
)
SELECT 
  u.client_id,
  u.yearly_income,
  COALESCE(t.num_txn, 0) AS txn_count
FROM user_data u
LEFT JOIN txn_count t ON u.client_id = t.client_id
WHERE u.yearly_income >= 100000 AND COALESCE(t.num_txn, 0) <= 2;

-- high income but low trx

WITH txn_count AS (
  SELECT 
    td.client_id, 
    COUNT(*) AS num_txn
  FROM read_csv(
     '/Users/mac/Downloads/transactions_data.csv',
     delim=',',
     header=TRUE,
     quote='"',
     null_padding=TRUE
  )AS td
  GROUP BY td.client_id
)
SELECT 
  ud.id AS client_id,
  CAST(REPLACE(REPLACE(ud.yearly_income, '$', ''), ',', '') AS DOUBLE) AS yearly_income,
  COALESCE(t.num_txn, 0) AS txn_count
FROM read_csv_auto('/Users/mac/Downloads/Rian/users_data.csv') AS ud
LEFT JOIN txn_count t 
  ON ud.id = t.client_id
WHERE 
  CAST(REPLACE(REPLACE(ud.yearly_income, '$', ''), ',', '') AS DOUBLE) >= 100000
  AND COALESCE(t.num_txn, 0) <= 2;