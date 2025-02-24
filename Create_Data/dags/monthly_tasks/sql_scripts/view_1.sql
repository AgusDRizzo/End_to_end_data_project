DROP VIEW IF EXISTS balance_ultimos_12_meses_por_tarjeta;
CREATE VIEW balance_ultimos_12_meses_por_tarjeta AS
WITH conusmo_ultimos_12_meses AS (
		SELECT 
			card_id, SUM(consumption_amount) AS consumo_mes
		FROM 
			credit_card_db.consumptions 
		WHERE 
			consumption_date >= DATE_FORMAT( DATE('{{ ds }}') - INTERVAL 12 MONTH, '%Y/%m/01')
		AND
			consumption_date < DATE_FORMAT( DATE('{{ ds }}'), '%Y/%m/01' )
		
		GROUP BY card_id), 
	pago_ultimos_12_meses AS (

		SELECT 
			SUM(payment_amount) AS pago_mes, card_id 
		FROM 
			credit_card_db.payments 
		WHERE 
			payment_date >= DATE_FORMAT( DATE('{{ ds }}') - INTERVAL 12 MONTH, '%Y/%m/01')
		AND
			payment_date < DATE_FORMAT( DATE('{{ ds }}'), '%Y/%m/01' )
        
		GROUP BY card_id)
        
	SELECT 
		(c.consumo_mes-p.pago_mes) AS balance, p.card_id 
	FROM
		pago_ultimos_12_meses AS p
	RIGHT JOIN
		conusmo_ultimos_12_meses AS c
	ON p.card_id = c.card_id
		
		
		