DROP VIEW IF EXISTS Total_Ct_Chng_Q4_Q1;
CREATE VIEW Total_Ct_Chng_Q4_Q1 AS 
WITH consumption_Q4 AS (
	SELECT 
		car.client_id, COUNT(con.consumption_id) AS Q4
	FROM 
		credit_card_db.consumptions AS con
	LEFT JOIN
		credit_card_db.cards AS car
	ON 
		con.card_id = car.card_id
	WHERE 
		consumption_date BETWEEN DATE_FORMAT( DATE('{{ ds }}') - INTERVAL 4 MONTH, '%Y/%m/01') AND DATE('{{ ds }}')
	GROUP BY 
		car.client_id), 
        
	consumption_Q1 AS ( 
		SELECT 
		car.client_id, COUNT(con.consumption_id) AS Q1
	FROM 
		credit_card_db.consumptions AS con
	LEFT JOIN
		credit_card_db.cards AS car
	ON 
		con.card_id = car.card_id
	WHERE 
		con.consumption_date BETWEEN DATE_FORMAT( DATE('{{ ds }}') - INTERVAL 12 MONTH, '%Y/%m/01') AND DATE_FORMAT( DATE('{{ ds }}') - INTERVAL 8 MONTH, '%Y/%m/01')
	GROUP BY 
		car.client_id)
SELECT 
	cli.client_id, change_ct.cociente AS Total_Ct_Chng_Q4_Q1
FROM
	credit_card_db.clients AS cli
LEFT JOIN
	(

	SELECT
		q1.client_id, ROUND(q4.Q4/q1.Q1, 2) AS cociente
	FROM 
		consumption_Q1 AS q1
	LEFT JOIN 
		consumption_Q4 AS q4
	ON 
		q1.client_id = q4.client_id ) AS change_ct
	ON 
		cli.client_id = change_ct.client_id
        
