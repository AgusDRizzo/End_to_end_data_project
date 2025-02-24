DROP VIEW IF EXISTS dependant_count;
CREATE VIEW dependant_count AS

	SELECT 
		DISTINCT(car.client_id), COALESCE(sub.Dependent_count, 0) AS Dependant_count
    FROM 
		credit_card_db.cards AS car
    LEFT JOIN (
		SELECT 
			client_id, COUNT(dependant_id) AS Dependent_count
		FROM 
			credit_card_db.dependants 
		GROUP BY 
			client_id ) AS sub
	ON car.client_id = sub.client_id