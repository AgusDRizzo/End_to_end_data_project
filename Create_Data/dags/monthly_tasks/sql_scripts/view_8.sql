DROP VIEW IF EXISTS Total_Relationship_Count;
CREATE VIEW Total_Relationship_Count AS
		
        SELECT 
			client_id, COUNT(card_id) AS Total_Relationship_Count 
		FROM 
			credit_card_db.cards 
		GROUP BY 
			client_id