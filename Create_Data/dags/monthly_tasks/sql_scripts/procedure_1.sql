
DROP TABLE IF EXISTS open_credit_per_month;
CREATE TABLE open_credit_per_month (
card_id INT,
month INT, 
open_credit FLOAT);

DROP PROCEDURE IF EXISTS average_open_credit;

CREATE PROCEDURE average_open_credit(_card_id INT)
                    
                    READS SQL DATA
                    
                    BEGIN
					
                        DECLARE done INT DEFAULT FALSE;
                        DECLARE _month_ INT;
                        DECLARE iterate_month CURSOR FOR SELECT DISTINCT(MONTH(date_on)) as mes FROM credit_card_db.clients  WHERE date_on >= DATE_FORMAT( CURRENT_DATE - INTERVAL 12 MONTH, '%Y/%m/01') AND date_on < DATE_FORMAT( CURRENT_DATE, '%Y/%m/01' ) ORDER by mes ASC;
                        DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;
                        
                        OPEN iterate_month;
                        iteration: LOOP
                            FETCH iterate_month INTO _month_;
                            IF done = TRUE THEN 
                                LEAVE iteration;
                            END IF;
                            SET @limit = ( SELECT 
												credit_limit
											FROM 
												credit_card_db.cards
											WHERE 
												card_id = _card_id);
							
                            SET @consumption = COALESCE(( SELECT 
													SUM(consumption_amount)
												FROM 
													credit_card_db.consumptions
												WHERE
													card_id = _card_id
												AND 
													consumption_date >= DATE_FORMAT( CURRENT_DATE - INTERVAL 12 MONTH, '%Y/%m/01')
												AND
													consumption_date < DATE_FORMAT( CURRENT_DATE - INTERVAL 12 MONTH + INTERVAL _month_ MONTH, '%Y/%m/01' )
												 ), 0)
												
                            
                            ;
                            
                            SET @payment = COALESCE(( SELECT 
													SUM(payment_amount)
												FROM 
													credit_card_db.payments
												WHERE
													card_id = _card_id
												AND 
													payment_date >= DATE_FORMAT( CURRENT_DATE - INTERVAL 12 MONTH, '%Y/%m/01')
												AND
													payment_date < DATE_FORMAT( CURRENT_DATE - INTERVAL 12 MONTH + INTERVAL _month_ MONTH, '%Y/%m/01' )
												
													), 0);
							INSERT INTO open_credit_per_month
                            VALUES (_card_id, _month_, ((@limit-(@consumption-@payment))))
                            ;
                            
                        END LOOP iteration;
                        CLOSE iterate_month;
                    END; 
                        
                       
