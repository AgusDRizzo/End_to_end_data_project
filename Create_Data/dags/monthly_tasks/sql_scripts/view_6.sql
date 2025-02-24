DROP VIEW IF EXISTS month_inactive;
CREATE VIEW month_inactive AS
		SELECT 
			car.client_id, COALESCE(sub.month_inactive, 12) AS month_inactive 
				FROM credit_card_db.cards AS car
        LEFT JOIN ( 
				SELECT 
					car.client_id, 12- COUNT(DISTINCT(MONTH(con.consumption_date))) AS month_inactive
				FROM 
					credit_card_db.consumptions AS con
				LEFT JOIN 
					credit_card_db.cards AS car
				ON 
					car.card_id = con.card_id
				WHERE 
					consumption_date > DATE_FORMAT( DATE('{{ ds }}') - INTERVAL 12 MONTH, '%Y/%m/01') 
					AND consumption_date < DATE_FORMAT( DATE('{{ ds }}'), '%Y/%m/01' )
				GROUP BY 
					car.client_id ) AS sub
		ON car.client_id = sub.client_id
