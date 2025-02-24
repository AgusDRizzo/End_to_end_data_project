DROP VIEW IF EXISTS view_Total_Trans_Amt;
CREATE VIEW view_Total_Trans_Amt AS 
	SELECT 
		cli.client_id, COALESCE(sub.Total_Trans_Amt, 0)  AS Total_Trans_Amt
	FROM 
		credit_card_db.clients AS cli
	LEFT JOIN 
		(
		SELECT 
			car.client_id, SUM(con.consumption_amount) AS Total_Trans_Amt
		FROM 
			credit_card_db.consumptions AS con
		LEFT JOIN 
			credit_card_db.cards AS car
		ON 
			con.card_id = car.card_id
		WHERE 
				consumption_date >= DATE_FORMAT( DATE('{{ ds }}') - INTERVAL 12 MONTH, '%Y/%m/01')
			AND
				consumption_date < DATE_FORMAT( DATE('{{ ds }}'), '%Y/%m/01' )
		GROUP BY 
			car.client_id
		ORDER BY
			car.client_id ) AS sub
	ON 
		cli.client_id = sub.client_id