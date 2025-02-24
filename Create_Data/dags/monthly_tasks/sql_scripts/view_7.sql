DROP VIEW IF EXISTS Months_on_book;
CREATE VIEW Months_on_book AS
	SELECT 
		client_id, timestampdiff(MONTH, date_on, DATE('{{ ds }}')) AS Months_on_book 
	FROM 
		credit_card_db.clients


