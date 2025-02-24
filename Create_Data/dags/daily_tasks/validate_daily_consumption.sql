USE credit_card_db;

DROP PROCEDURE IF EXISTS iterate_daily_balance;

CREATE PROCEDURE iterate_daily_balance()
    READS SQL DATA
BEGIN
    DECLARE done INT DEFAULT FALSE;
    DECLARE _consumption_id INT;
    DECLARE iterate_consumptions CURSOR FOR 
        SELECT consumptions.consumption_id 
        FROM consumptions 
        WHERE consumptions.consumption_date = CURRENT_DATE();
    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;
    
    OPEN iterate_consumptions;
    iteration: LOOP
        FETCH iterate_consumptions INTO _consumption_id;
        IF done THEN 
            LEAVE iteration;
        END IF;
        CALL credit_card_db.balance_between_dates(_consumption_id);
    END LOOP iteration;
    CLOSE iterate_consumptions;
END;

CALL iterate_daily_balance;