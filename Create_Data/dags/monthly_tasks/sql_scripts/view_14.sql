DROP VIEW IF EXISTS join_2;
CREATE VIEW join_2 AS 
SELECT  DISTINCT(cus.client_id), rel.total_relationship_count, inac.month_inactive, cred.credit_limit, bal.balance
FROM credit_card_db.clients as cus
INNER JOIN credit_card_db.Total_Relationship_Count AS rel
ON cus.client_id = rel.client_id
INNER JOIN credit_card_db.month_inactive AS inac
ON cus.client_id = inac.client_id
INNER JOIN credit_card_db.credit_limit AS cred
ON cus.client_id = cred.client_id
INNER JOIN credit_card_db.balance_ultimos_12_meses_por_cliente AS bal
ON cus.client_id = bal.client_id