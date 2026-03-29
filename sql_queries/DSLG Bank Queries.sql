/*
Project: Customer Support Workforce Analytics & Hiring Optimisation Framework

Business Context:
DSLG Bank’s support team has reported increased workloads and overtime due to
a rise in customer support queries. Executive leadership requires a data-backed
assessment to determine whether additional support staff should be hired.

The objective is to build a structured analytical environment to evaluate:

• Customer support demand volume
• Support representative workload
• Regional concentration of support issues
• Employee overtime risk

The analysis provides leadership with a "quantitative hiring decision
framework" for scaling the support workforce.
*/


/* ======================================================================
SECTION 01 — DATABASE INITIALIZATION & ENVIRONMENT SETUP
Objective: Create analytics environment for support operations analysis
====================================================================== */

DROP DATABASE IF EXISTS BANKING;
CREATE DATABASE BANKING;
USE banking;

/* =============================================================================
SECTION 02 — RAW DATA TABLE (SUPPORT OPERATIONS MASTER TABLE)
Objective: Store consolidated operational dataset
================================================================== */

DROP TABLE IF EXISTS bank_t;

CREATE TABLE bank_t (
	CUSTOMER_ID INTEGER,
	FIRSTNAME VARCHAR(30),
    LASTNAME VARCHAR(30),
	EMAIL_ADDRESS VARCHAR(50),
    CONTACT VARCHAR(25),
    BRANCH_ID INTEGER,
    BRANCH_LATITUDE DECIMAL(14,8),
	BRANCH_LONGITUDE DECIMAL(14,8),
    ACCOUNT_ID INTEGER,
    BRANCH_CITY VARCHAR(25),
    BRANCH_STATE VARCHAR(30),
    SUPPORT_QUERY_ID INTEGER,
    SUPPORT_QUERY_TYPE VARCHAR(30),
    SUPPORT_QUERY_CATEGORY INTEGER,
    SUPPORT_REPRESENTATIVE_ID INTEGER,
    CALL_DURATION INTEGER,
    EMPLOYEE_ID INTEGER,
    EMPLOYEE_NAME VARCHAR(50),
    WEEK_NUMBER INTEGER,
    SUPPORT_QUERY_TIME_IN_DAYS INTEGER,
	PRIMARY KEY (CUSTOMER_ID, SUPPORT_QUERY_ID, EMPLOYEE_ID, BRANCH_ID)
);

/* =====================================================
SECTION 03 — NORMALIZED ANALYTICAL DATA MODEL
Objective: Create structured dimension and fact tables
===================================================== */

-- Branch Table
DROP TABLE IF EXISTS bank_branch_t;

CREATE TABLE bank_branch_t (
	BRANCH_ID INTEGER PRIMARY KEY,
    BRANCH_CITY VARCHAR(25),
    BRANCH_STATE VARCHAR(30),
    BRANCH_LATITUDE DECIMAL(14,8),
	BRANCH_LONGITUDE DECIMAL(14,8)
);

-- Employee Table
DROP TABLE IF EXISTS bank_employees_t;

CREATE TABLE bank_employees_t (
	EMPLOYEE_ID INTEGER PRIMARY KEY,
    EMPLOYEE_NAME VARCHAR(50)
);

-- Customer Table
DROP TABLE IF EXISTS bank_customer_t;

CREATE TABLE bank_customer_t (
	CUSTOMER_ID INTEGER PRIMARY KEY,
    BRANCH_ID INTEGER,
	FIRSTNAME VARCHAR(30),
    LASTNAME VARCHAR(30),
	EMAIL_ADDRESS VARCHAR(50),
    CONTACT VARCHAR(25),
    ACCOUNT_ID INTEGER
);

-- Support Query Table
DROP TABLE IF EXISTS bank_support_query_t;

CREATE TABLE bank_support_query_t (
    SUPPORT_QUERY_ID INTEGER PRIMARY KEY,
    SUPPORT_QUERY_TYPE VARCHAR(30),
    SUPPORT_QUERY_CATEGORY INTEGER,
    SUPPORT_REPRESENTATIVE_ID INTEGER,
    CALL_DURATION INTEGER,
	WEEK_NUMBER INTEGER,
    CUSTOMER_ID INTEGER,
	BRANCH_ID INTEGER,
	ACCOUNT_ID INTEGER
);

/* =========================================================================
SECTION 04 — ETL STORED PROCEDURES (DATA PIPELINE LOGIC)
Objective: Transform raw operational data into structured analytical tables
========================================================================= */

-- Branch Population Procedure
DROP PROCEDURE IF EXISTS bank_branch_p;

DELIMITER $$
CREATE PROCEDURE bank_branch_p()
BEGIN
	INSERT INTO bank_branch_t (
		BRANCH_ID,
		BRANCH_CITY,
		BRANCH_STATE,
		BRANCH_LATITUDE,
		BRANCH_LONGITUDE
    )
    SELECT DISTINCT 
		BRANCH_ID,
		BRANCH_CITY,
		BRANCH_STATE,
		BRANCH_LATITUDE,
		BRANCH_LONGITUDE
	FROM bank_t; 
END;

-- Employee Population Procedure
DROP PROCEDURE IF EXISTS bank_employees_p;

DELIMITER $$
CREATE PROCEDURE bank_employees_p()
BEGIN
	INSERT INTO bank_employees_t (
		EMPLOYEE_ID,  
		EMPLOYEE_NAME
    )
    SELECT DISTINCT
		EMPLOYEE_ID,  
		EMPLOYEE_NAME
	FROM bank_t; 
END;

-- Customer Population Procedure
DROP PROCEDURE IF EXISTS bank_cust_p;

DELIMITER $$
CREATE PROCEDURE bank_cust_p()
BEGIN
	INSERT INTO bank_customer_t (
        CUSTOMER_ID,
		BRANCH_ID,	  
		FIRSTNAME,
		LASTNAME, 
		EMAIL_ADDRESS,
		CONTACT,
		ACCOUNT_ID
    )
    SELECT DISTINCT 
		CUSTOMER_ID, 
		BRANCH_ID,
		FIRSTNAME,
		LASTNAME, 
		EMAIL_ADDRESS,
		CONTACT,
		ACCOUNT_ID
	FROM banking.bank_t;
END;

-- Support Query Population Procedure
DROP PROCEDURE IF EXISTS bank_support_query_p;

DELIMITER $$
CREATE PROCEDURE bank_support_query_p()
BEGIN
	INSERT INTO bank_support_query_t (
		SUPPORT_QUERY_ID,
        SUPPORT_QUERY_TYPE,
		SUPPORT_QUERY_CATEGORY,  
		SUPPORT_REPRESENTATIVE_ID,  
		CALL_DURATION,
		WEEK_NUMBER,
		CUSTOMER_ID, 
		BRANCH_ID,
		ACCOUNT_ID
    )
    SELECT  
		SUPPORT_QUERY_ID,
        SUPPORT_QUERY_TYPE,
		SUPPORT_QUERY_CATEGORY,  
		SUPPORT_REPRESENTATIVE_ID,  
		CALL_DURATION,
		WEEK_NUMBER,
		CUSTOMER_ID, 
		BRANCH_ID,
		ACCOUNT_ID
	FROM bank_t; 
END;

/* ============================================================
SECTION 05 — DATA INGESTION PROCESS
Objective: Load raw operational data into staging environment
============================================================ */

-- Ingestion code --

TRUNCATE BANK_T;

LOAD DATA LOCAL INFILE 'D:/Downloads/PGDSDMS/2 -- SQL and Databases/Week Two Materials/MLS/Fresh/bank.csv'
INTO TABLE BANK_T
FIELDS TERMINATED by ','
OPTIONALLY ENCLOSED BY '"' 
LINES TERMINATED BY '\n'
IGNORE 1 LINES;

-- Calling the procedures after ingestion --  

CALL bank_branch_p();
CALL bank_employees_p();
CALL bank_cust_p();
CALL bank_support_query_p();

/* ===========================================================
SECTION 06 — ANALYTICAL VIEWS (BUSINESS REPORTING LAYER)
Objective: Create reusable datasets for management reporting
=========================================================== */

-- Support Queries by Branch
DROP VIEW IF EXISTS banking_support_query_branch_v;

CREATE VIEW banking_support_query_branch_v AS
    SELECT 
		BR.BRANCH_ID,
        BR.BRANCH_CITY,
        BR.BRANCH_STATE,
		SUPP.SUPPORT_QUERY_ID,
        SUPP.SUPPORT_QUERY_TYPE,
        SUPP.SUPPORT_REPRESENTATIVE_ID,
        SUPP.CALL_DURATION
    FROM BANK_BRANCH_T BR 
        INNER JOIN BANK_SUPPORT_QUERY_T SUPP 
            ON BR.BRANCH_ID = SUPP.BRANCH_ID;

-- Support Queries by Customer
DROP VIEW IF EXISTS banking_support_query_customer_v;

CREATE VIEW banking_support_query_customer_v AS
    SELECT 
		CUST.CUSTOMER_ID,
		SUPP.SUPPORT_QUERY_ID,
        SUPP.SUPPORT_QUERY_TYPE,
        SUPP.SUPPORT_REPRESENTATIVE_ID,
        SUPP.CALL_DURATION
    FROM BANK_SUPPORT_QUERY_T SUPP 
        INNER JOIN BANK_CUSTOMER_T CUST 
            ON SUPP.CUSTOMER_ID = CUST.CUSTOMER_ID;
            
-- Support Queries by Employee
DROP VIEW IF EXISTS banking_support_query_employee_v;

CREATE VIEW banking_support_query_employee_v AS
    SELECT 
		SUPP.SUPPORT_QUERY_ID,
        SUPP.SUPPORT_QUERY_TYPE,
        SUPP.SUPPORT_REPRESENTATIVE_ID,
        SUPP.CALL_DURATION,
        EMP.EMPLOYEE_NAME
    FROM BANK_SUPPORT_QUERY_T SUPP 
        INNER JOIN BANK_EMPLOYEES_T EMP 
            ON SUPP.SUPPORT_REPRESENTATIVE_ID = EMP.EMPLOYEE_ID;

/* ===================================================================
SECTION 07 — WORKLOAD CLASSIFICATION FUNCTION
Objective: Categorize employee work hours relative to standard shift
=================================================================== */

DROP FUNCTION IF EXISTS work_hours_f;

DELIMITER $$
CREATE FUNCTION work_hours_f (WORK_HOURS DECIMAL(5,2)) 
RETURNS VARCHAR(25)
DETERMINISTIC
BEGIN
	DECLARE CATEGORY VARCHAR(25);
    IF WORK_HOURS > 8 THEN
		SET CATEGORY = 'OVER TIME';
	ELSEIF WORK_HOURS = 8 THEN
		SET CATEGORY = 'FIXED TIME';
	ELSEIF WORK_HOURS < 8 THEN
		SET CATEGORY = 'LESS THAN SHIFT HOURS';
	END IF;
RETURN CATEGORY;
END;

/*------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------
                                               Queries
--------------------------------------------------------------------------------------------------                                               
------------------------------------------------------------------------------------------------*/

-- DISPLAYING DATA FROM TABLES
select *
from bank_branch_t
limit 10;

select *
from bank_customer_t
limit 10;

select *
from bank_employees_t
limit 10;

select *
from bank_support_query_t
limit 10;

select *
from bank_t
limit 10;

-- DISPLAYING DATA FROM VIEWS
select *
from banking_support_query_branch_v
limit 10;

select *
from banking_support_query_customer_v
limit 10;

select *
from banking_support_query_employee_v
limit 10;


/* =========================================================
SECTION 08 — SUPPORT DEMAND & CUSTOMER ANALYSIS
Objective: Evaluate query demand and customer distribution
========================================================= */

-- [1] Total Customers
select
	count(distinct CUSTOMER_ID) as NO_OF_CUSTOMERS
from bank_customer_t;

-- [2] Customer Distribution by State
select
	BRANCH_STATE,
	count(CUSTOMER_ID) as NO_OF_CUSTOMERS
from bank_t
group by 1
order by 2 desc;

-- [3] Customer Distribution by City
select
	BRANCH_CITY,
	count(CUSTOMER_ID) as NO_OF_CUSTOMERS
from bank_t
group by 1
order by 2 desc;

-- [4] Total Support Queries
select
	count(distinct SUPPORT_QUERY_ID) as NO_OF_QUERIES
from banking_support_query_employee_v;

-- [5a] Total Support Query Types
select
	distinct (SUPPORT_QUERY_TYPE)
from banking_support_query_employee_v;

-- [5b] Support Query Distribution by Query Type
select
	SUPPORT_QUERY_TYPE,
    count(SUPPORT_QUERY_ID) as NO_OF_QUERIES
from bank_support_query_t
group by 1
order by 2 desc;

    
/* ============================================================
SECTION 09 — SUPPORT WORKLOAD & EMPLOYEE PERFORMANCE ANALYSIS
Objective: Identify workload imbalance across representatives
============================================================ */

-- [6] Queries Handled by Each Representative
select
	 SUPPORT_REPRESENTATIVE_ID,
     EMPLOYEE_NAME as REPRESENTANTIVE_NAME,
     count(SUPPORT_QUERY_ID) as NO_OF_QUERIES
from banking_support_query_employee_v
group by 1,2
order by 3 desc;
     
-- [7] High Workload Representatives (>250 queries)
select
	 SUPPORT_REPRESENTATIVE_ID as EMPLOYEE_ID,
     EMPLOYEE_NAME,
     count(SUPPORT_QUERY_ID) as NO_OF_QUERIES
from banking_support_query_employee_v
group by 1,2
having NO_OF_QUERIES > 250
order by 3 desc;


/* ============================================================
SECTION 10 — WORK HOURS & OVERTIME ANALYSIS
Objective: Identify representatives within and outside normal workload
============================================================ */

-- [8] Weekly and Daily Representative Call Duration(hours)
select
	SUPPORT_REPRESENTATIVE_ID,
    EMPLOYEE_NAME as REPRESENTATIVE_NAME,
    sum(CALL_DURATION)/60 as 'CALL_DURATION_PER_WEEK(hours)',
    sum(CALL_DURATION)/60/5 as 'CALL_DURATION_PER_DAY(hours)'
from banking_support_query_employee_v
group by 1,2
order by 3 desc;

-- [9] Representatives Outside Normal Work Hours
select
	SUPPORT_REPRESENTATIVE_ID,
    EMPLOYEE_NAME as REPRESENTATIVE_NAME,
    WORK_HRS_F(sum(CALL_DURATION)/60/5) as WORK_HOUR_CATEGORY
from banking_support_query_employee_v
group by 1,2;


/* ================================================================================
SECTION 11 — REGIONAL AND BRANCH SUPPORT DEMAND ANALYSIS
Objective: Identify branches with highest support demand (including unique demand) 
================================================================================ */

-- [10] Region with Most Queries
select
	BRANCH_CITY,
    BRANCH_STATE,
    count(SUPPORT_QUERY_ID) as NO_OF_QUERIES
from banking_support_query_branch_v
group by 1,2
order by 3 desc
limit 1;

-- [11] Support Query Types by Region
select
	BRANCH_CITY,
    BRANCH_STATE,
    count(SUPPORT_QUERY_TYPE) as NO_OF_QUERY_TYPES
from banking_support_query_branch_v
group by 1,2
order by 3 desc;

-- [12] Branch with Most Home Loan Queries
select
	BRANCH_ID,
    SUPPORT_QUERY_TYPE,
    count(SUPPORT_QUERY_TYPE) as NO_OF_QUERIES
from banking_support_query_branch_v
where SUPPORT_QUERY_TYPE = 'Home Loan'
group by 1
order by 3 desc;

-- [13] Customer and Branch Details (Queries with call duration >8mins)
select
	SUPPORT_QUERY_ID,
    CALL_DURATION,
    CUSTOMER_ID,
    FIRSTNAME,
    LASTNAME,
    EMAIL_ADDRESS,
    CONTACT,
    BRANCH_ID,
    BRANCH_LATITUDE,
    BRANCH_LONGITUDE,
    ACCOUNT_ID,
    BRANCH_CITY,
    BRANCH_STATE
from bank_t
where CALL_DURATION > 8;


/* =================================================================
SECTION 12 — EXECUTIVE SUMMARY OUTPUT (HIRING DECISION METRICS)
Objective: Provide leadership-level metrics for workforce planning
================================================================= */

-- [14] Total Support Queries
select
	count(distinct SUPPORT_QUERY_ID) as TOTAL_SUPPORT_QUERIES
from banking_support_query_employee_v;
    
-- [15] Total Support Representatives
select
	count(distinct SUPPORT_REPRESENTATIVE_ID) as TOTAL_REPRESENTATIVES
from bank_support_query_t;

-- [16] Average Queries per Representative
select
    count(*) / count(distinct SUPPORT_REPRESENTATIVE_ID) as AVG_QUERIES_PER_REP
from bank_support_query_t;

-- [17] Representatives Handling Excessive Query Volume (>250)
select
	count(*) as OVERLOADED_REPS
from (
	select
		SUPPORT_REPRESENTATIVE_ID
	from banking_support_query_employee_v
	group by 1
	having count(SUPPORT_QUERY_ID) > 250
) tab1;

-- [18] Average Queries per Customer
select
    count(*) / count(distinct CUSTOMER_ID) as AVG_QUERIES_PER_CUSTOMER
from bank_support_query_t;

-- [19] Customers with over Average Queries (> or = 3)
select
	count(CUSTOMER_ID) as NO_OF_CUSTOMERS
from (
	select
		CUSTOMER_ID,
		count(distinct SUPPORT_QUERY_ID) as QUERIES_PER_CUSTOMER
	from banking_support_query_customer_v
	group by 1
	having QUERIES_PER_CUSTOMER >= 3
) tab2;

-- [20] Branch with Highest Support Demand
select
	BRANCH_CITY,
    BRANCH_STATE,
    count(SUPPORT_QUERY_ID) as NO_OF_QUERIES
from banking_support_query_branch_v
group by 1,2
order by 3 desc
limit 1;