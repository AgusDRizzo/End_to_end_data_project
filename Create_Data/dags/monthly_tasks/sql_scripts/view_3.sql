DROP VIEW IF EXISTS max_card_category_client;
CREATE VIEW max_card_category_client AS
WITH 
	card_categories AS (
		SELECT 
			 DISTINCT(category_num) AS num, card_category
		FROM
			credit_card_db.cards
		), 
	max_card_num AS (
		SELECT 
			client_id, MAX(category_num) AS max_num
		FROM 
			cards 
		GROUP BY 
			client_id )
    
SELECT 
	car.client_id, car.max_num, cat.card_category
FROM 
	max_card_num AS car
LEFT JOIN 
	card_categories AS cat
ON
	car.max_num = cat.num
