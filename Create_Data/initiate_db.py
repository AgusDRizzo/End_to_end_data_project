# -------------------- 1. Create DataBase --------------------
import pandas as pd
import mysql.connector  
from mysql.connector import errorcode
from mysql.connector import Error
from mysql.connector.constants import ClientFlag
from credentials import cred_db, cred_host, cred_password, cred_user, cred_port

DB_NAME = cred_db
TABLES = {}
TABLES['cards'] = (
    "CREATE TABLE `cards` ("
    "  `card_id` int NOT NULL AUTO_INCREMENT,"
    "  `client_id` int NOT NULL,"
    "  `card_number` int NOT NULL,"
    "   `card_type` enum('VISA','MASTERCARD') NOT NULL,"
    "  `card_category` enum('Blue','Silver','Gold','Platinum') NOT NULL,"
    "  `date_emission` date NOT NULL,"
    "   `date_expiry` date NOT NULL,"
    "   `credit_limit` int unsigned NOT NULL,"
    "   PRIMARY KEY (`card_id`)"
    ") ENGINE=InnoDB")

TABLES['clients'] = (
 " CREATE TABLE `clients` ("
 " `client_id` int NOT NULL AUTO_INCREMENT,"
 " `first_name` varchar(200) NOT NULL,"
 " `last_name` varchar(200) NOT NULL,"
 " `email` varchar(200) NOT NULL,"
 " `gender` enum('Male','Female') DEFAULT NULL,"
 " `educational_level` enum('Unknown','Uneducated','High-School','College','Graduate','Post-Graduate','Doctorate') DEFAULT NULL,"
 " `income_category` enum('Unknown','Less than $40K','$40K - $60K','$60K - $80K','$80K - $120K','$120K +') DEFAULT NULL,"
 " `phone_number` varchar(200) DEFAULT NULL,"
 " `date_on` date NOT NULL,"
 " `age` int DEFAULT NULL,"
 " `marital_status` enum('Single','Married','Divorced','Unknown') NOT NULL,"
 " PRIMARY KEY (`client_id`)"
 "   ) ENGINE=InnoDB")

TABLES['dependants'] = (
"    CREATE TABLE `dependants` ("
"  `dependant_id` int NOT NULL AUTO_INCREMENT,"
"  `client_id` int NOT NULL,"
"  `dependant_name` varchar(200) NOT NULL,"
"  `dependant_last_name` varchar(200) NOT NULL,"
"  `dependant_age` int DEFAULT NULL,"
"  PRIMARY KEY (`dependant_id`)"
"    ) ENGINE=InnoDB" )

TABLES['consumptions'] = (
"    CREATE TABLE `consumptions` ("
"  `consumption_id` int NOT NULL AUTO_INCREMENT,"
"  `card_id` int NOT NULL,"
"  `consumption_amount` int unsigned NOT NULL,"
"  `consumption_date` date NOT NULL,"
"  `business_name` varchar(200) NOT NULL,"
"  `consumption_status` ENUM('ACCEPTED', 'REJECTED') DEFAULT 'ACCEPTED',"
"  PRIMARY KEY (`consumption_id`)"
"    ) ENGINE=InnoDB" )

TABLES['payments'] = (
"    CREATE TABLE `payments` ("
"  `payment_id` int NOT NULL AUTO_INCREMENT,"
"  `card_id` int NOT NULL,"
"  `payment_amount` int NOT NULL,"
"  `payment_date` date NOT NULL,"
"  PRIMARY KEY (`payment_id`)"
"    ) ENGINE=InnoDB" )

TABLES['churn_clients'] = (
"    CREATE TABLE `churn_clients` ("
"  `churn_id` int NOT NULL AUTO_INCREMENT,"
"  `client_id` int NOT NULL,"
"  `date` date NOT NULL,"
"  PRIMARY KEY (`churn_id`)"
"    ) ENGINE=InnoDB" )


cnx = mysql.connector.connect(user=cred_user, 
                              password=cred_password,
                              host=cred_host,
                              port = cred_port)
cursor = cnx.cursor()
cursor.execute("DROP DATABASE IF EXISTS credit_card_db")

def create_database(cursor):
    
    try:
        cursor.execute(
            "CREATE DATABASE {} DEFAULT CHARACTER SET 'utf8'".format(DB_NAME))
    except mysql.connector.Error as err:
        print("Failed creating database: {}".format(err))
        exit(1)

try:
    cursor.execute("USE {}".format(DB_NAME))
except mysql.connector.Error as err:
    print("Database {} does not exists.".format(DB_NAME))
    if err.errno == errorcode.ER_BAD_DB_ERROR:
        create_database(cursor)
        print("Database {} created successfully.".format(DB_NAME))
        cnx.database = DB_NAME
    else:
        print(err)
        exit(1)

for table_name in TABLES:
    table_description = TABLES[table_name]
    try:
        print("Creating table {}: ".format(table_name), end='')
        cursor.execute(table_description)
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
            print("already exists.")
        else:
            print(err.msg)
    else:
        print("OK")
    
cursor.close()
cnx.close()

# -------------------- 2. Create Synthetic Data --------------------

from Create_Data import menu_generate_consumptions_and_payments

menu_generate_consumptions_and_payments()

# -------------------- 3. Load Data --------------------

cnx = mysql.connector.connect(user=cred_user, 
                              password=cred_password,
                              host=cred_host,
                              db = cred_db,
                              port = cred_port, 
                              client_flags=[ClientFlag.LOCAL_FILES], 
                              allow_local_infile=True)
cursor = cnx.cursor()
cursor.execute("USE credit_card_db")
cursor.execute("SET GLOBAL local_infile = 'ON'")

query_1 = """
        LOAD DATA LOCAL INFILE 'CSV_Files/MOCK_DATA_CLIENTS.csv'
        INTO TABLE credit_card_db.clients
        FIELDS TERMINATED BY ','
        LINES TERMINATED BY '\n'
        IGNORE 1 LINES
        (first_name, last_name, email, gender, educational_level, 
         income_category, phone_number, date_on, age, marital_status)
        """
query_2 = """
        LOAD DATA LOCAL INFILE
        "CSV_Files/MOCK_DATA_CARDS.csv"
        INTO TABLE credit_card_db.cards
        FIELDS TERMINATED BY ','
        LINES TERMINATED BY '\n'
        IGNORE 1 LINES
        (client_id, card_number, card_type, card_category, date_emission, date_expiry, credit_limit)
        """
query_3 = """
        LOAD DATA LOCAL INFILE
        "CSV_Files/MOCK_DATA_DEPENDANTS.csv"
        INTO TABLE credit_card_db.dependants
        FIELDS TERMINATED BY ','
        LINES TERMINATED BY '\n'
        IGNORE 1 LINES
        (client_id, dependant_name, dependant_last_name, dependant_age)
        """
query_4 = """
        LOAD DATA LOCAL INFILE
        "CSV_Files/MOCK_DATA_CONSUMPTIONS.csv"
        INTO TABLE credit_card_db.consumptions
        FIELDS TERMINATED BY ','
        LINES TERMINATED BY '\n'
        IGNORE 1 LINES
        (card_id, consumption_amount, consumption_date, business_name)
        """
query_5 = """
        LOAD DATA LOCAL INFILE
        "CSV_Files/MOCK_DATA_PAYMENTS.csv"
        INTO TABLE credit_card_db.payments
        FIELDS TERMINATED BY ','
        LINES TERMINATED BY '\n'
        IGNORE 1 LINES
        (card_id, payment_amount, payment_date)
        """



cursor.execute(query_1)
cursor.execute(query_2)
cursor.execute(query_3)
cursor.execute(query_4)
cursor.execute(query_5)

cnx.commit()
cursor.close()
cnx.close()

# -------------------- 4. Create Relational Logic --------------------

cnx = mysql.connector.connect(user=cred_user, 
                              password=cred_password,
                              host=cred_host,
                              db = cred_db,
                              port = cred_port)
                              
cursor = cnx.cursor()
cursor.execute("USE credit_card_db")

alter_table_1 = """
                ALTER TABLE cards
	            ADD CONSTRAINT fk_clients_cards
                FOREIGN KEY (client_id) REFERENCES clients(client_id)
                """

alter_table_2 = """
                ALTER TABLE dependants
	            ADD CONSTRAINT fk_clients_dependants
                FOREIGN KEY (client_id) REFERENCES clients(client_id)
                """



alter_table_3 = """
                ALTER TABLE consumptions
	            ADD CONSTRAINT fk_cards_consumptions
                FOREIGN KEY (card_id) REFERENCES cards(card_id)
                """

alter_table_4 = """
                ALTER TABLE payments
	            ADD CONSTRAINT fk_cards_payments
                FOREIGN KEY (card_id) REFERENCES cards(card_id)
                """

alter_table_5 = """
                ALTER TABLE credit_card_db.cards
                ADD category_num INT; 

                UPDATE credit_card_db.cards
                SET category_num = CASE
                    WHEN card_category = "Platinum" THEN 3
                    WHEN card_category = "Gold" THEN 2
                    WHEN card_category = "Silver" THEN 1
                    WHEN card_category = "Blue" THEN 0
                    ELSE 0
                    END
                """

alter_table_6 = """
                ALTER TABLE churn_clients
	            ADD CONSTRAINT fk_clients_churn
                FOREIGN KEY (client_id) REFERENCES clients(client_id)
                """

cursor.execute(alter_table_1)
cursor.execute(alter_table_2)
cursor.execute(alter_table_3)
cursor.execute(alter_table_4)
cursor.execute(alter_table_6)
cursor.execute(alter_table_5)

cursor.close()
cnx.close()


# -------------------- 5. Clean DataBase --------------------

cnx = mysql.connector.connect(user=cred_user, 
                              password=cred_password,
                              host=cred_host,
                              db = cred_db,
                              port = cred_port)
                              
cursor = cnx.cursor()
cursor.execute("USE credit_card_db")

cursor.execute("DROP PROCEDURE IF EXISTS sum_consumption_between_dates")
procedure_1 = """
                CREATE PROCEDURE sum_consumption_between_dates(_consumption INT)
                    
                    READS SQL DATA
                    
                    BEGIN
                        SET @date_on  = 
                        (SELECT 
                            consumption_date 
                        FROM 
                            credit_card_db.consumptions 
                        ORDER BY 
                            consumption_date ASC 
                        LIMIT 1);

                        SET @date_end = (
                                SELECT 
                                    consumption_date 
                                FROM 
                                    credit_card_db.consumptions 
                                WHERE 
                                    consumption_id = _consumption
                                LIMIT 1);
                        
                        SET @card_id_verify = (
                                SELECT 
                                    card_id 
                                FROM 
                                    credit_card_db.consumptions 
                                WHERE consumption_id = _consumption
                                LIMIT 1);
                        SET @consumo = ( 
                                SELECT 
                                    consumption_amount 
                                FROM 
                                    credit_card_db.consumptions 
                                WHERE consumption_id = _consumption
                                LIMIT 1);
                        DROP TABLE IF EXISTS consumo_acumulado;
                        CREATE TEMPORARY TABLE consumo_acumulado AS
                        SELECT 
                            SUM(c.consumption_amount)-@consumo AS suma_consumos, c.card_id, ca.credit_limit
                        FROM 
                            credit_card_db.consumptions AS c
                        LEFT JOIN 
                            credit_card_db.cards AS ca
                        ON
                            c.card_id = ca.card_id
                    
                        WHERE 
                            c.card_id = @card_id_verify AND c.consumption_status="ACCEPTED" AND consumption_date BETWEEN @date_on AND @date_end
                        GROUP BY 
                            c.card_id;
                    
                    
                    END;
                """
procedure_2 = """
                CREATE PROCEDURE sum_payments_between_dates(_consumption INT)
                    
                    READS SQL DATA
                    
                    BEGIN
                        SET @date_on  = 
                        (SELECT 
                            consumption_date 
                        FROM 
                            credit_card_db.consumptions 
                        ORDER BY 
                            consumption_date ASC 
                        LIMIT 1);

                        SET @date_end = (
                                SELECT 
                                    consumption_date 
                                FROM 
                                    credit_card_db.consumptions 
                                WHERE 
                                    consumption_id = _consumption
                                LIMIT 1);
                        
                        SET @card_id_verify = (
                                SELECT 
                                    card_id 
                                FROM 
                                    credit_card_db.consumptions 
                                WHERE consumption_id = _consumption
                                LIMIT 1);
                        DROP TABLE IF EXISTS pago_acumulado;
                        CREATE TEMPORARY TABLE pago_acumulado AS
                        SELECT 
                            SUM(p.payment_amount) AS suma_pagos, p.card_id
                        FROM 
                            credit_card_db.payments AS p
                    
                        WHERE 
                            p.card_id = @card_id_verify AND payment_date BETWEEN @date_on AND @date_end
                        GROUP BY 
                            p.card_id;
                        
                    
                    END;
                """
procedure_3 = """
                CREATE PROCEDURE balance_between_dates(_consumption INT)
                    
                    READS SQL DATA
                    
                    BEGIN
                        CALL sum_consumption_between_dates(_consumption);
                        CALL  sum_payments_between_dates(_consumption);
                        
                        SET @limit = ( SELECT 
                                    credit_limit
                                FROM 
                                    credit_card_db.consumo_acumulado
                                LIMIT 1);
                        
                        SET @consumo = ( 
                                SELECT 
                                    consumption_amount 
                                FROM 
                                    credit_card_db.consumptions 
                                WHERE consumption_id = _consumption
                                LIMIT 1);
                        
                        SET @flag = (SELECT EXISTS(SELECT 1 FROM pago_acumulado));
                                
                        SET @balance = CASE 
                                WHEN @flag = 1 THEN
                                    (SELECT 
                                        @limit - (c.suma_consumos-(p.suma_pagos)) AS open_credit
                                    FROM
                                        consumo_acumulado as c
                                    LEFT JOIN
                                        pago_acumulado as p
                                    ON
                                        c.card_id = p.card_id)
                                ELSE @limit
                                END;
                                
                        
                        
                        UPDATE credit_card_db.consumptions SET consumption_status = CASE
                        WHEN @balance > @consumo  THEN "ACCEPTED"
                        WHEN @balance = @consumo THEN "ACCEPTED"
                        ELSE "REJECTED"
                        END
                        WHERE consumption_id = _consumption;
                    
                            
                    END;
                 """
procedure_4 = """
                CREATE PROCEDURE iterate_balance()
                    
                    READS SQL DATA
                    
                    BEGIN
                        DECLARE done INT DEFAULT FALSE;
                        DECLARE _consumption_id INT;
                        DECLARE iterate_consumptions CURSOR FOR SELECT consumptions.consumption_id FROM consumptions;
                        DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;
                        
                        OPEN iterate_consumptions;
                        iteration: LOOP
                            FETCH iterate_consumptions INTO _consumption_id;
                            IF done = TRUE THEN 
                                LEAVE iteration;
                            END IF;
                            CALL credit_card_db.balance_between_dates(_consumption_id);
                        END LOOP iteration;
                        CLOSE iterate_consumptions;
                    END;
                """
procedure_5 = """
                CREATE PROCEDURE iterate_cleaning()
                    
                    READS SQL DATA
                    
                    BEGIN
                        DECLARE done INT DEFAULT FALSE;
                        DECLARE _cleaning_id INT;
                        DECLARE iterate_cleaning CURSOR FOR SELECT DISTINCT(consumptions.consumption_id) FROM consumptions WHERE consumption_status = "REJECTED";
                        DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;
                        
                        OPEN iterate_cleaning;
                        iteration: LOOP
                            FETCH iterate_cleaning INTO _cleaning_id;
                            IF done = TRUE THEN 
                                LEAVE iteration;
                            END IF;
                            SET @id_rejected = (
										SELECT card_id 
                                        FROM credit_card_db.consumptions
										WHERE consumption_id = _cleaning_id);
							SET @month_rejected = ( 
										SELECT MONTH(consumption_date) 
                                        FROM credit_card_db.consumptions
										WHERE consumption_id = _cleaning_id);	
                            
                            DELETE FROM 
								credit_card_db.payments 
                            WHERE 
								card_id = @id_rejected AND MONTH(payment_date)= @month_rejected ;
                            DELETE FROM 
								credit_card_db.consumptions
							WHERE 
								consumption_id = _cleaning_id;
                            
                        END LOOP iteration;
                        CLOSE iterate_cleaning;
                    END;
                """
cursor.execute("DROP PROCEDURE IF EXISTS sum_consumption_between_dates")
cursor.execute("DROP PROCEDURE IF EXISTS sum_payments_between_dates")
cursor.execute("DROP PROCEDURE IF EXISTS balance_between_dates")
cursor.execute("DROP PROCEDURE IF EXISTS iterate_balance")
cursor.execute("DROP PROCEDURE IF EXISTS iterate_cleaning")
cursor.execute(procedure_1)
cursor.execute(procedure_2)
cursor.execute(procedure_3)
cursor.execute(procedure_4)
cursor.execute(procedure_5)

cursor.close()
cnx.close()
