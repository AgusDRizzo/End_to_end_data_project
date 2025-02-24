DROP VIEW IF EXISTS balance_ultimos_12_meses_por_cliente;
CREATE VIEW balance_ultimos_12_meses_por_cliente AS
		SELECT DISTINCT(car.client_id), COALESCE(sub.balance, 0) AS balance
        FROM credit_card_db.cards AS car
        LEFT JOIN
        (
	    SELECT 
		    SUM(b.balance) AS balance, c.client_id
	    FROM 
		    credit_card_db.cards AS c 
	    RIGHT JOIN 
		    credit_card_db.balance_ultimos_12_meses_por_tarjeta AS b
	    ON 
		    b.card_id = c.card_id
	    GROUP BY (c.client_id) ) AS sub
	ON car.client_id = sub.client_id