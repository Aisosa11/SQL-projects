--By running all blocks of CTES together You are able to get Social influence and customer lifetime Value 
With Avg_value AS (
    SELECT t.customer_id, c.segment, AVG(t.amount) OVER (PARTITION BY c.segment) AS Avg_value_Seg
    FROM cus_trxns t
    JOIN Cus_biodatas c 
	ON c.id = t.customer_id
    WHERE t.deb_cre_ind = 'debit'
),

---------------------------
Trxn_count AS(
Select t.customer_id,Segment, COUNT(amount) AS Cnt_trxn, SUM(amount) AS Sum_trxn, 
AVG(COUNT(amount))OVER(PARTITION BY Segment) AS Avg_Segment_cnt, COUNT(DISTINCT t.product_id) AS Cnt_Product,c.segment,
AVG(COUNT(DISTINCT t.product_id )) Over(PARTITION BY segment) AS AVG_count
FROM cus_trxns t
JOIN Cus_biodatas c ON c.id = t.customer_id
WHERE t.deb_cre_ind = 'debit'
GROUP BY t.customer_id,c.segment
)

, Social AS (
    SELECT c.id, COUNT(DISTINCT t.customer_id) AS referred_count
    FROM Cus_biodatas c
    JOIN cus_trxns t ON c.id = t.customer_id
    WHERE t.deb_cre_ind = 'Credit' 
    GROUP BY c.id
),
debit_transactions AS (
    SELECT t.customer_id AS sender, t.deb_cre_ind, t.reference_no, c.segment
    FROM cus_trxns t
    JOIN Cus_biodatas c ON t.customer_id = c.id
    WHERE t.deb_cre_ind = 'debit'
),
credit_transactions AS (
    SELECT t.customer_id AS receiver, t.deb_cre_ind, t.reference_no, c.segment
    FROM cus_trxns t
    JOIN Cus_biodatas c ON t.customer_id = c.id   
    WHERE t.deb_cre_ind = 'credit'
),
AVG_SOCIALS AS (
SELECT d.sender, d.segment, COUNT(DISTINCT c.receiver) AS distinct_receivers, 
Avg(count (distinct receiver)) over (partition by d.segment) as avg_social
FROM debit_transactions d
JOIN credit_transactions c ON d.reference_no = c.reference_no
GROUP BY d.sender, d.segment
ORDER BY d.sender
),
--SELECT * FROM AVG_SOCIALS
--select sender, distinct_receivers/avg_social from AVG_SOCIALS

max_trxn_date as
(
Select customer_id, max(date_trxn) as last_trxn
from cus_trxns
group by customer_id
),

duration_cte as (
	select customer_id, last_trxn, cb.date_open,cb.segment as seg,
	(last_trxn::date - cb.date_open::date) as customer_duration,
    avg(last_trxn::date - cb.date_open::date) over (partition by cb.segment) as avg_life_span
	from max_trxn_date md
	join cus_biodatas cb
	on md.customer_id = cb.id
),

trxn_timeframe as (
	select customer_id, date_trxn,
	lag(date_trxn, 1) over (partition by customer_id
						   order by date_trxn asc) as previous_date
	from cus_trxns
),

trxn_gaps as(
--Find the number of days gap
	select customer_id, date_trxn,
	previous_date, 
	(date_trxn::date - previous_date::date) as time_difference
	from trxn_timeframe
),

time_diff_cte as (
	select customer_id, cb.segment, avg(time_difference) as time_diff_avg,
	avg(avg(time_difference)) over (partition by cb.segment) as avg_val_seg
	from trxn_gaps tg
    join cus_biodatas cb on tg.customer_id = cb.id
	group by tg.customer_id, cb.segment
)

SELECT DISTINCT TC.customer_id AS customer_id,av.segment, TC.Sum_trxn / AV.Avg_value_Seg AS ratio,
TC.Cnt_trxn/TC.AVG_count AS Count_Ratio, TC.Cnt_Product/TC.AVG_count AS ratio_productID,
AVS.distinct_receivers/AVS.avg_social AS Ratio_SocialInfluence,
DC.customer_duration, DC.avg_life_span, 
DC.customer_duration/DC.avg_life_span as duration_ratio, time_diff_avg/avg_val_seg AS Avg_timediff
FROM Trxn_count TC
JOIN Avg_value AV ON TC.customer_id = AV.customer_id
JOIN AVG_SOCIALS AVS ON TC.customer_id =AVS.sender
JOIN duration_cte DC on DC.customer_id = TC.customer_id
JOIN time_diff_cte TD on TD.customer_id=TC.customer_id
WHERE TC.customer_id=386;

