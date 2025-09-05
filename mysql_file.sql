create database customers_transactions;
update customer set Gender = null where Gender ='';
update customer set Age = null where Age ='';
alter table customer modify Age int null;

select * from customer;

create table transactions (
    date_new     DATE,
    Id_check     INT,
    ID_client    INT,
    Count_products DECIMAL(10,3),
    Sum_payment  DECIMAL(10,2)
);

load data local infile "C:\Users\b.zhuma\Downloads\transactions_info.csv"
into table transactions
fields terminated by ','
lines terminated by '\n'
ignore 1 rows;

SHOW VARIABLES LIKE 'secure_file_priv';
SHOW VARIABLES LIKE 'local_infile';
SET GLOBAL local_infile = 1;


select * from transactions;

#1
#список клиентов с непрерывной историей за год
#средний чек за период с 01.06.2015 по 01.06.2016
#средняя сумма покупок за месяц
#количество всех операций по клиенту за период

SELECT 
    t.ID_client,
    AVG(t.Sum_payment) AS avg_check,              
    SUM(t.Sum_payment) / 12 AS avg_monthly_spend, 
    COUNT(*) AS ops,                              
    SUM(t.Sum_payment) AS total_sum               
FROM transactions t
WHERE t.date_new >= '2015-06-01' 
  AND t.date_new < '2016-06-01'
  AND t.ID_client IN (
      SELECT ID_client
      FROM transactions
      WHERE date_new >= '2015-06-01' 
        AND date_new < '2016-06-01'
      GROUP BY ID_client
      HAVING COUNT(DISTINCT CONCAT(YEAR(date_new), '-', MONTH(date_new))) = 12
  )
GROUP BY t.ID_client
ORDER BY total_sum DESC;

#2
#информацию в разрезе месяцев:
#средняя сумма чека в месяц;
#среднее количество операций в месяц;
#среднее количество клиентов, которые совершали операции в месяц;
#долю от общего количества операций за год и долю в месяц от общей суммы операций в месяц;

WITH tx AS (SELECT ID_client,
        DATE(date_new) AS tx_dt,
        MONTH(date_new) AS mth,
        YEAR(date_new) AS yr,
        Sum_payment
    FROM transactions
    WHERE date_new >= '2015-06-01' AND date_new < '2016-06-01'
),
monthly AS (SELECT yr, mth,
        SUM(Sum_payment) AS monthly_sum,
        COUNT(*) AS monthly_ops,
        COUNT(DISTINCT ID_client) AS active_customers
    FROM tx
    GROUP BY yr, mth
),
totals AS (SELECT SUM(monthly_sum) AS total_sum,
           SUM(monthly_ops) AS total_ops
FROM monthly
)
SELECT 
    yr, mth,
    monthly_sum,
    monthly_ops,
    active_customers,
    monthly_sum / monthly_ops AS avg_check,
    monthly_ops / active_customers AS avg_ops_per_customer,
    ROUND(monthly_ops / total_ops * 100, 2) AS share_ops_pct,
    ROUND(monthly_sum / total_sum * 100, 2) AS share_sum_pct
FROM monthly, totals
ORDER BY yr, mth;

#вывести % соотношение M/F/NA в каждом месяце с их долей затрат в месяц;

WITH tx AS (
    SELECT 
        t.ID_client,
        DATE(t.date_new) AS tx_dt,
        MONTH(t.date_new) AS mth,
        YEAR(t.date_new) AS yr,
        t.Sum_payment,
        COALESCE(NULLIF(TRIM(UPPER(c.Gender)), ''), 'NA') AS gender
    FROM transactions t
    LEFT JOIN customer c ON t.ID_client = c.Id_client
    WHERE t.date_new >= '2015-06-01' AND t.date_new < '2016-06-01'
),
gender_month AS (
    SELECT 
        yr, mth, gender,
        SUM(Sum_payment) AS sum_amount,
        COUNT(*) AS ops,
        COUNT(DISTINCT ID_client) AS customers
    FROM tx
    GROUP BY yr, mth, gender
),
gender_month_total AS (
    SELECT 
        yr, mth,
        SUM(sum_amount) AS m_sum,
        SUM(ops) AS m_ops,
        SUM(customers) AS m_customers
    FROM gender_month
    GROUP BY yr, mth
)
SELECT 
    g.yr, g.mth, g.gender,
    g.customers,
    ROUND(g.customers / t.m_customers * 100, 2) AS gender_share_customers_pct,
    ROUND(g.sum_amount / t.m_sum * 100, 2) AS gender_share_sum_pct,
    ROUND(g.ops / t.m_ops * 100, 2) AS gender_share_ops_pct
FROM gender_month g
JOIN gender_month_total t ON g.yr = t.yr AND g.mth = t.mth
ORDER BY yr, mth, gender;

#3
#возрастные группы клиентов с шагом 10 лет и отдельно клиентов, у которых нет данной информации, 
#сумма и количество операций за весь период

WITH tx AS (SELECT t.ID_client, t.Sum_payment, c.Age
    FROM transactions t
    LEFT JOIN customer c ON t.ID_client = c.Id_client
    WHERE t.date_new >= '2015-06-01' AND t.date_new < '2016-06-01'
),
age_grouped AS (SELECT
        CASE 
            WHEN Age IS NULL THEN 'NA'
            WHEN Age BETWEEN 0 AND 9 THEN '00-09'
            WHEN Age BETWEEN 10 AND 19 THEN '10-19'
            WHEN Age BETWEEN 20 AND 29 THEN '20-29'
            WHEN Age BETWEEN 30 AND 39 THEN '30-39'
            WHEN Age BETWEEN 40 AND 49 THEN '40-49'
            WHEN Age BETWEEN 50 AND 59 THEN '50-59'
            WHEN Age BETWEEN 60 AND 69 THEN '60-69'
            WHEN Age BETWEEN 70 AND 79 THEN '70-79'
            WHEN Age >= 80 THEN '80+'
        END AS age_group,
        Sum_payment
    FROM tx
)
SELECT 
    age_group,
    SUM(Sum_payment) AS total_sum,
    COUNT(*) AS total_ops
FROM age_grouped
GROUP BY age_group
ORDER BY age_group;

#поквартально - средние показатели и %

WITH tx AS (SELECT t.ID_client, t.Sum_payment, c.Age,
        QUARTER(t.date_new) AS qtr,
        YEAR(t.date_new) AS yr
    FROM transactions t
    LEFT JOIN customer c ON t.ID_client = c.Id_client
    WHERE t.date_new >= '2015-06-01' AND t.date_new < '2016-06-01'
),
age_grouped AS (SELECT CASE 
            WHEN Age IS NULL THEN 'NA'
            WHEN Age BETWEEN 0 AND 9 THEN '00-09'
            WHEN Age BETWEEN 10 AND 19 THEN '10-19'
            WHEN Age BETWEEN 20 AND 29 THEN '20-29'
            WHEN Age BETWEEN 30 AND 39 THEN '30-39'
            WHEN Age BETWEEN 40 AND 49 THEN '40-49'
            WHEN Age BETWEEN 50 AND 59 THEN '50-59'
            WHEN Age BETWEEN 60 AND 69 THEN '60-69'
            WHEN Age BETWEEN 70 AND 79 THEN '70-79'
            WHEN Age >= 80 THEN '80+'
        END AS age_group,
        Sum_payment,
        qtr, yr
    FROM tx
),
agg AS (SELECT 
        yr, qtr, age_group,
        SUM(Sum_payment) AS sum_amount,
        COUNT(*) AS ops
    FROM age_grouped
    GROUP BY yr, qtr, age_group
),
totals AS (SELECT 
        yr, qtr,
        SUM(sum_amount) AS q_sum,
        COUNT(*) AS q_ops
    FROM agg
    GROUP BY yr, qtr
)
SELECT a.yr, a.qtr, a.age_group, a.sum_amount, a.ops,
    ROUND(a.sum_amount / a.ops, 2) AS avg_check,
    ROUND(a.sum_amount / t.q_sum * 100, 2) AS share_sum_pct,
    ROUND(a.ops / t.q_ops * 100, 2) AS share_ops_pct
FROM agg a
JOIN totals t ON a.yr = t.yr AND a.qtr = t.qtr
ORDER BY yr, qtr, age_group;
