DROP VIEW IF EXISTS join_3;
CREATE VIEW join_3 AS 

SELECT DISTINCT(cus.client_id), amtch.Total_Amt_Chng_Q4_Q1, amt.Total_Trans_Amt, ct.Total_Trans_Ct, ctch.total_ct_chng_q4_q1
FROM credit_card_db.clients AS cus
INNER JOIN credit_card_db.total_amt_chng_q4_q1 AS amtch
ON cus.client_id = amtch.client_id
INNER JOIN view_total_trans_amt AS amt
ON cus.client_id = amt.client_id 
INNER JOIN credit_card_db.view_total_trans_ct AS ct
ON cus.client_id = ct.client_id
INNER JOIN credit_card_db.total_ct_chng_q4_q1 AS ctch
ON cus.client_id = ctch.client_id