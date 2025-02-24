DROP VIEW IF EXISTS credit_limit;
CREATE VIEW credit_limit AS
SELECT 
	client_id, SUM(credit_limit) AS credit_limit
FROM
	credit_card_db.cards
GROUP BY 
	client_id