

DROP PROCEDURE IF EXISTS iterate_average_open_credit;

CREATE PROCEDURE iterate_average_open_credit()
                    
                    READS SQL DATA
                    
                    BEGIN
					
                        DECLARE done INT DEFAULT FALSE;
                        DECLARE _card_id INT;
                        DECLARE iterate_card_id CURSOR FOR SELECT DISTINCT(card_id) FROM credit_card_db.cards;
                        DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;
                        
                        OPEN iterate_card_id;
                        iteration: LOOP
                            FETCH iterate_card_id INTO _card_id;
                            IF done = TRUE THEN 
                                LEAVE iteration;
                            END IF;
							CALL average_open_credit(_card_id);
                          
                            
                            
                        END LOOP iteration;
                        CLOSE iterate_card_id;
                    END; 
CALL iterate_average_open_credit;
                       
                    
                            
                    
