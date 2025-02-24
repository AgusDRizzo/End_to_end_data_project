DROP VIEW IF EXISTS join_1;
CREATE VIEW join_1 AS 
SELECT  DISTINCT(car.client_id), cus.age, cus.gender, dep.dependant_count, cus.educational_level, cus.marital_status, cus.income_category, car.card_category, mon.months_on_book 
FROM credit_card_db.clients AS cus
INNER JOIN credit_card_db.dependant_count AS dep
ON cus.client_id = dep.client_id
INNER JOIN credit_card_db.max_card_category_client AS car
ON cus.client_id = car.client_id
INNER JOIN credit_card_db.Months_on_book AS mon
ON cus.client_id = mon.client_id
