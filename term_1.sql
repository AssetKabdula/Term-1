-- This is Term Project 1 for the Data Engineering 1: SQL and Different Shapes of Data course 
-- Author: Asset Kabdula
--
-- ============================================================================================================
-- SECTION 1: Operational Layer
-- This section creates and imports the necessary tables 

-- SECTION 2: Analytical Layer
-- This section creates analytical table combining columns from operational layer

-- SECTION 3: ETL Pipeline
-- This section creates stored procedures and a trigger 

-- SECTION 4: DATA MARTS
-- This section inserts the data to the analytical table and creates views as data marts

-- SECTION 5: Testing
-- This section tests the trigger
-- ============================================================================================================


-- ####################################################################################################
-- ####################################################################################################
-- ####################################################################################################

-- ============================================================================================================
-- SECTION 1: Operational Layer
-- This section creates the necessary tables and loads the data into them  
-- ============================================================================================================

-- Creating a schema named `financial_loan`
DROP SCHEMA IF EXISTS `financial_loan`;
CREATE SCHEMA  `financial_loan`;
USE `financial_loan`;

SHOW VARIABLES LIKE 'secure_file_priv';
SHOW VARIABLES LIKE "local_infile"; 
SHOW VARIABLES LIKE 'datadir'; -- 8 csv files are located in this directory in my computer
--
-- Table structure for table `district`
DROP TABLE IF EXISTS district;
CREATE TABLE district (
    district_id INT NOT NULL PRIMARY KEY,
    A2 VARCHAR(100),
    A3 VARCHAR(100),
    A4 INT,
    A5 INT,
    A6 INT,
    A7 INT,
    A8 INT,
    A9 INT,
    A10 DECIMAL(6,1),
    A11 INT,
    A12 DECIMAL(6,1),
    A13 DECIMAL(6,2),
    A14 INT,
    A15 INT,
    A16 INT
);
--
-- Table structure for table `account`
DROP TABLE IF EXISTS account;
CREATE TABLE account (
    account_id INT NOT NULL PRIMARY KEY,
    district_id INT,
    frequency VARCHAR(255),
    date DATE,
    FOREIGN KEY (district_id) REFERENCES district(district_id) -- Links to district table
);
--
-- Table structure for table `client`
DROP TABLE IF EXISTS client;
CREATE TABLE client (
    client_id INT NOT NULL PRIMARY KEY,
    gender VARCHAR(10),
    birth_date DATE,
    district_id INT,
    FOREIGN KEY (district_id) REFERENCES district(district_id) -- Links to district table
);
--
-- Table structure for table `disposition`
DROP TABLE IF EXISTS disposition;
CREATE TABLE disposition (
    disp_id INT NOT NULL PRIMARY KEY,
    client_id INT,
    account_id INT,
    type VARCHAR(20),
    FOREIGN KEY (client_id) REFERENCES client(client_id), -- Links to client table
    FOREIGN KEY (account_id) REFERENCES account(account_id) -- Links to account table
);
--
-- Table structure for table `credit`
DROP TABLE IF EXISTS credit;
CREATE TABLE credit (
    card_id INT NOT NULL PRIMARY KEY,
    disp_id INT,
    type VARCHAR(20),
    issued DATE,
    FOREIGN KEY (disp_id) REFERENCES disposition(disp_id) -- Links to disponent table
);
--
-- Table structure for table `loan`
DROP TABLE IF EXISTS loan;
CREATE TABLE loan (
    loan_id INT NOT NULL PRIMARY KEY,
    account_id INT,
    date DATE,
    amount INT,
    duration INT,
    payments DECIMAL(10,1),
    status VARCHAR(1),
    FOREIGN KEY (account_id) REFERENCES account(account_id) -- Links to account table
);
--
-- Table structure for table `order_`
DROP TABLE IF EXISTS order_;
CREATE TABLE order_ (
    order_id INT NOT NULL PRIMARY KEY,
    account_id INT,
    bank_to VARCHAR(10),
    account_to INT,
    amount DECIMAL(10,1),
    k_symbol VARCHAR(20),
    FOREIGN KEY (account_id) REFERENCES account(account_id) -- Links to account table
);
--
-- Table structure for table `transaction`
DROP TABLE IF EXISTS transaction;
CREATE TABLE transaction (
    trans_id INT NOT NULL PRIMARY KEY,
    account_id INT,
    date DATE,
    type VARCHAR(20),
    operation VARCHAR(20),
    amount INT,
    balance INT,
    k_symbol VARCHAR(100),
    bank VARCHAR(10),
    account INT,
    FOREIGN KEY (account_id) REFERENCES account(account_id) -- Links to account table
);
--
-- Loading the data to 'district' table
TRUNCATE TABLE district;

LOAD DATA INFILE '/usr/local/mysql/data/district.csv'
INTO TABLE district
FIELDS TERMINATED BY ';' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;  
--
-- 'district' table 
SELECT * FROM district LIMIT 10;
DESCRIBE district;
--
-- Loading the data to 'client' table
TRUNCATE TABLE client;

LOAD DATA INFILE '/usr/local/mysql/data/client.csv'
INTO TABLE client
FIELDS TERMINATED BY ';' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;  
--
-- 'client' table 
SELECT * FROM client LIMIT 10;
DESCRIBE client;
--
-- Loading the data to 'account' table
TRUNCATE TABLE account;

LOAD DATA INFILE '/usr/local/mysql/data/account.csv'
INTO TABLE account
FIELDS TERMINATED BY ';' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;  
--
-- 'account' table 
SELECT * FROM account LIMIT 10;
DESCRIBE account;
--
-- Loading the data to 'disposition' table
TRUNCATE TABLE disposition;

LOAD DATA INFILE '/usr/local/mysql/data/disp.csv'
INTO TABLE disposition
FIELDS TERMINATED BY ';' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;  
--
-- 'disposition' table 
SELECT * FROM disposition LIMIT 10;
DESCRIBE disposition;
--
-- Loading the data to 'credit' table
TRUNCATE TABLE credit;

LOAD DATA INFILE '/usr/local/mysql/data/credit.csv'
INTO TABLE credit
FIELDS TERMINATED BY ';' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;  
--
-- 'credit' table 
SELECT * FROM credit LIMIT 10;
DESCRIBE credit;
--
-- Loading the data to 'loan' table
TRUNCATE TABLE loan;

LOAD DATA INFILE '/usr/local/mysql/data/loan.csv'
INTO TABLE loan
FIELDS TERMINATED BY ';' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;  
--
-- 'loan' table 
SELECT * FROM loan LIMIT 10;
DESCRIBE loan;
--
-- Loading the data to 'order_' table
TRUNCATE TABLE order_;

LOAD DATA INFILE '/usr/local/mysql/data/order.csv'
INTO TABLE order_
FIELDS TERMINATED BY ';' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;  
--
-- 'order' table 
SELECT * FROM order_ LIMIT 10;
DESCRIBE order_;
--
-- Loading the data to 'transaction' table
TRUNCATE TABLE transaction;

LOAD DATA INFILE '/usr/local/mysql/data/trans.csv'
INTO TABLE transaction
FIELDS TERMINATED BY ';' 
ENCLOSED BY '"'          
LINES TERMINATED BY '\n' 
IGNORE 1 ROWS;
--
-- 'transaction' table 
SELECT * FROM transaction LIMIT 20;
DESCRIBE transaction;

-- ============================================================================================================
-- SECTION 2: Analytical layer
-- This section creates analytical table combining columns from operational layer
-- ============================================================================================================

-- Table structure for table `analytical_data`
DROP TABLE IF EXISTS analytical_data;
CREATE TABLE analytical_data (
    -- Loan details 
    loan_id INT NOT NULL PRIMARY KEY,
    loan_amount INT,
    loan_duration INT,
    loan_payments DECIMAL(10, 1),
    loan_status VARCHAR(1),
    loan_date DATE, 
    -- Transaction details    
    most_frequent_type VARCHAR(20),
    total_transaction_amount INT,
    avg_transaction_balance INT,
    transaction_count INT, 
    most_frequent_k_symbol VARCHAR(100),
    -- Credit card details    
    credit_type VARCHAR(20),
    -- Client details    
    gender VARCHAR(10),
    -- District details    
    district_name VARCHAR(100),
    district_average_salary DECIMAL(6, 1)
);

--
-- 'analytical_data' table 
		
DESCRIBE analytical_data;

-- ============================================================================================================
-- SECTION 3: ETL Pipeline 
-- This section creates stored procedures and a trigger 
-- ============================================================================================================

-- STORED PROCEDURES

-- Stored Procedure to transform district table
DROP PROCEDURE IF EXISTS transform_district;
DELIMITER // 
CREATE PROCEDURE transform_district()
BEGIN
    UPDATE district
    SET A2 = REPLACE(A2, ' ', '_');
END //

-- Stored Procedure to transform transaction table
DROP PROCEDURE IF EXISTS transform_transaction;
DELIMITER // 
CREATE PROCEDURE transform_transaction()
BEGIN
    UPDATE transaction
    SET type = CASE 
                   WHEN type = 'PRIJEM' THEN 'Credit'
                   WHEN type = 'VYDAJ' OR type IS NULL THEN 'Withdrawal'
               END,
        k_symbol = CASE
                      WHEN k_symbol = 'POJISTNE' THEN 'Insurance Payment (IP)'
                      WHEN k_symbol = 'SLUZBY' THEN 'Payment on Statement (PS)'
                      WHEN k_symbol = 'UROK' THEN 'Interest Credited (ICR)'
                      WHEN k_symbol = 'SANKC. UROK' THEN 'Sanction Interest (SI)'
                      WHEN k_symbol = 'SIPO' THEN 'Household (H)'
                      WHEN k_symbol = 'DUCHOD' THEN 'Old-age Pension (OP)'
                      WHEN k_symbol = 'UVER' THEN 'Loan Payment (LP)'
                      WHEN k_symbol = '' THEN 'Other'  -- If k_symbol is empty, set to 'Other'
                   END;
END //

--
-- TRIGGER
-- Trigger for loan Table. This trigger will call each of above stored procedures to perform the ETL process every time there’s an insert on loan
DROP TRIGGER IF EXISTS after_loan_insert;
DELIMITER //
CREATE TRIGGER after_loan_insert
AFTER INSERT ON loan
FOR EACH ROW
BEGIN
    -- Check if the loan_id already exists in the analytical_data table
    IF NOT EXISTS (
        SELECT 1 
        FROM analytical_data 
        WHERE loan_id = NEW.loan_id
    ) THEN
        -- Load step - insert into analytical_data table if not already present
        INSERT INTO analytical_data (
            credit_type, gender, district_name, district_average_salary,
            total_transaction_amount, avg_transaction_balance,
            transaction_count, most_frequent_k_symbol, most_frequent_type, loan_id, 
            loan_amount, loan_duration,
            loan_payments, loan_status, loan_date
        )
        SELECT 
            IFNULL(c.type, 'Not holder') AS credit_type,  -- If credit_type is NULL, replace with 'Not holder'
            cl.gender,
            d.A2 AS district_name,
            d.A11 AS district_average_salary,
            SUM(t.amount) AS total_transaction_amount,  -- Sum of transaction amounts
            AVG(t.balance) AS avg_transaction_balance,  -- Average transaction balance
            COUNT(t.trans_id) AS transaction_count,     -- Count of transactions
            (SELECT t.k_symbol FROM transaction t WHERE t.account_id = a.account_id 
             GROUP BY t.k_symbol ORDER BY COUNT(*) DESC LIMIT 1) AS most_frequent_k_symbol, -- Most frequent k_symbol
            (SELECT t.type FROM transaction t WHERE t.account_id = a.account_id 
             GROUP BY t.type ORDER BY COUNT(*) DESC LIMIT 1) AS most_frequent_type, -- Most frequent type
            l.loan_id,
            l.amount AS loan_amount,
            l.duration AS loan_duration,
            l.payments AS loan_payments,
            l.status AS loan_status,
            l.date AS loan_date
        FROM 
            loan l
        JOIN account a ON l.account_id = a.account_id
        LEFT JOIN disposition disp ON a.account_id = disp.account_id AND disp.type = 'Owner'  -- Filter for 'Owner' role
        LEFT JOIN client cl ON disp.client_id = cl.client_id
        LEFT JOIN credit c ON disp.disp_id = c.disp_id
        LEFT JOIN transaction t ON a.account_id = t.account_id
        LEFT JOIN district d ON a.district_id = d.district_id
        WHERE 
            l.loan_id = NEW.loan_id -- Load based on the newly inserted loan
        GROUP BY 
            l.loan_id, c.type, cl.gender, d.A2, d.A11, l.amount, l.duration, l.payments, l.status, l.date;
    END IF;
END //

-- Reset delimiter to default
DELIMITER ;

-- ============================================================================================================
-- SECTION 4: DATA MARTS
-- This section creates views as data marts
-- ============================================================================================================

SET SQL_SAFE_UPDATES = 0;
CALL transform_district(); -- Stored procedure 
CALL transform_transaction(); -- Stored procedure 

TRUNCATE TABLE analytical_data;
-- Inserting data into 'analytical_data' table 
INSERT INTO analytical_data (
    credit_type, gender, district_name, district_average_salary,
    total_transaction_amount, avg_transaction_balance,
    transaction_count, most_frequent_k_symbol, most_frequent_type, loan_id, 
    loan_amount, loan_duration,
    loan_payments, loan_status, loan_date
)
SELECT 
	IFNULL(c.type, 'Not holder') AS credit_type,  -- If credit_type is NULL, replace with 'Not holder'
    cl.gender,
    d.A2 AS district_name,
    d.A11 AS district_average_salary,
    SUM(t.amount) AS total_transaction_amount,  -- Sum of transaction amounts
    AVG(t.balance) AS avg_transaction_balance,  -- Average transaction balance
    COUNT(t.trans_id) AS transaction_count,     -- Count of transactions
    (SELECT t.k_symbol FROM transaction t WHERE t.account_id = a.account_id 
     GROUP BY t.k_symbol ORDER BY COUNT(*) DESC LIMIT 1) AS most_frequent_k_symbol, -- Most frequent k_symbol
	(SELECT t.type FROM transaction t WHERE t.account_id = a.account_id 
     GROUP BY t.type ORDER BY COUNT(*) DESC LIMIT 1) AS most_frequent_type, -- Most frequent type
    l.loan_id,
    l.amount AS loan_amount,
    l.duration AS loan_duration,
    l.payments AS loan_payments,
    l.status AS loan_status,
    l.date AS loan_date
FROM 
    loan l
JOIN account a ON l.account_id = a.account_id
LEFT JOIN disposition disp ON a.account_id = disp.account_id AND disp.type = 'Owner'  -- Filter for 'Owner' role
LEFT JOIN client cl ON disp.client_id = cl.client_id
LEFT JOIN credit c ON disp.disp_id = c.disp_id
LEFT JOIN transaction t ON a.account_id = t.account_id
LEFT JOIN district d ON a.district_id = d.district_id
GROUP BY 
    l.loan_id, c.type, cl.gender, d.A2, d.A11, l.amount, l.duration, l.payments, l.status, l.date;

-- 'analytical_data' table
SELECT * FROM analytical_data LIMIT 10;

-- VIEWS
-- 1. View for Client Financial Profiles
DROP VIEW IF EXISTS client_financial_profiles;
CREATE VIEW client_financial_profiles AS
SELECT 
    loan_id,
    gender,
    credit_type,
    loan_amount,
    loan_status,
    loan_duration,
    avg_transaction_by_credit_type,
    loan_percentile_in_district,
    district_default_count,
    CASE 
        WHEN loan_percentile_in_district > 0.8 THEN 'Top 20% Loan Amount'
        ELSE 'Below 80% Loan Amount'
    END AS loan_amount_segment
FROM (
    SELECT 
        loan_id,
        gender,
        credit_type,
        loan_amount,
        loan_status,
        loan_duration,
        AVG(total_transaction_amount) OVER (PARTITION BY credit_type) AS avg_transaction_by_credit_type,
        PERCENT_RANK() OVER (PARTITION BY district_name ORDER BY loan_amount) AS loan_percentile_in_district,
        SUM(CASE WHEN loan_status = 'D' THEN 1 ELSE 0 END) OVER (PARTITION BY district_name) AS district_default_count
    FROM 
        analytical_data
) AS subquery;



-- 2. Gender Relationship with Status of Loans
DROP VIEW IF EXISTS gender_loan_status;
CREATE VIEW gender_loan_status AS
SELECT 
    gender,
    loan_status,
    COUNT(loan_id) AS loan_count,
    AVG(loan_amount) AS avg_loan_amount
FROM analytical_data
GROUP BY gender, loan_status;


-- 3. Gender Relationship with Average Loan Amount and Average Duration of Loans
DROP VIEW IF EXISTS gender_avg_loan_amount;
CREATE VIEW gender_avg_loan_amount AS
SELECT 
    gender,
    AVG(loan_amount) AS avg_loan_amount,
	AVG(loan_duration) AS avg_loan_duration
FROM analytical_data
GROUP BY gender;

-- 4. In Which Months People Usually Take Loans
DROP VIEW IF EXISTS monthwise_loan_activity;
CREATE VIEW monthwise_loan_activity AS
SELECT 
    EXTRACT(MONTH FROM loan_date) AS loan_month,
    COUNT(loan_id) AS loan_count,
    AVG(loan_amount) AS avg_loan_amount
FROM analytical_data
GROUP BY loan_month
ORDER BY loan_count DESC;

-- 5. Gender Relationship with Average Old-Age Pension Payment Amount
DROP VIEW IF EXISTS gender_average_pension_payment;
CREATE VIEW gender_average_pension_payment AS
SELECT 
    cl.gender,
    AVG(ad.total_transaction_amount) AS average_pension_payment
FROM 
    analytical_data ad
JOIN 
    client cl ON ad.loan_id = cl.client_id  -- This assumes that loan_id is related to client_id
WHERE 
    ad.most_frequent_k_symbol = 'Old-age Pension (OP)'  -- Filtering for old-age pension transactions
GROUP BY 
    cl.gender;

-- 6. Amount of Loan Relationship with Districts
DROP VIEW IF EXISTS district_loan_summary;
CREATE VIEW district_loan_summary AS
SELECT 
    ad.district_name,
    AVG(ad.loan_amount) AS average_loan_amount,
    COUNT(ad.loan_id) AS number_of_loans
FROM 
    analytical_data ad
GROUP BY 
    ad.district_name;
    
-- ============================================================================================================
-- SECTION 5: Testing
-- This section tests the trigger
-- ============================================================================================================
INSERT INTO loan (loan_id, account_id, date, amount, duration, payments, status) VALUES -- Inserting new data
(2, 2, '2024-10-01', 80952, 24, 3373.0, 'A');

SELECT * FROM loan LIMIT 10;
SELECT * FROM analytical_data where loan_id = 2; -- New data is indeed inserted in the analytical_data table







