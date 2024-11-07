# Term-1
Term Project 1 for Data Engineering 1: SQL and Different Shapes of Data course
# Table of Contents
- [Project Overview](#ProjectOverview)
- [Data Source Details](#DataSourceDetails)
- [Data Structure - ER Diagram](#DataStructure-ERDiagram)
- [Operational Data Layer](#OperationalDataLayer)
- [Analytics Plan](#AnalyticsPlan)
- [Analytical Data Layer](#AnalyticalDataLayer)
- [ETL Pipeline](#ETLPipelineDesign)
- [Views](#Views)
- [Conclusion](#ConclusionandFutureWork)
- [Contact](#ConclusionandFutureWork)

## Usage Instructions
1. Clone the repository: 
```git clone https://github.com/AssetKabdula/Term-1```
## Repository Structure
The repository is organized as follows:
- Term-1/: Main project folder
  - data/: Contains CSV files with raw data
  - SQL outputs: Contains ER diagram and Analytical Table snapshot
  - term_1.sql: The SQL code for the project
  - README.md: Description of the project

## Project Overview
The goal of this project is to analyze loan profiles, gender relationship with status of loans, average loan amount, average duration of loans, seasonal trends in taking loans among customers of Czech Bank. 

The project consists of the following components:

- Operational layer: creating table structures and importing the dataset
- Data analytics plan: analytics to be executed on the data
- Analytical layer: analytical data structure with necessary columns from operational layer
- ETL pipeline: extracting raw data from CSV files, transforming it through cleaning and merging relevant columns, and loading the cleaned data into analytical tables. Triggers and stored procedures are used.
- Data mart: views as data marts

## Data Source Details
The Berka dataset is a collection of financial information from a Czech bank. The dataset deals with over 5,300 bank clients with approximately 1,000,000 transactions. Additionally, the bank represented in the dataset has extended close to 700 loans and issued nearly 900 credit cards, all of which are represented in the data.
- Table Descriptions:
  - `account.csv`: each record describes static characteristics of an account
  - `client.csv`: each record describes characteristics of a client
  - `credit.csv`: Each record describes a credit card issued to an account
  - `disp.csv`: Each record relates together a client with an account i.e. this relation describes the rights of clients to operate accounts
  - `district.csv`: Each record describes demographic characteristics of a district
  - `loan.csv`: Each record describes a loan granted for a given account
  - `order.csv`: Each record describes characteristics of a payment order
  - `trans.csv`: Each record describes one transaction on an account
- Entity-Relationship Description:
  - Each account has both static characteristics (e.g. date of creation, address of the branch) given in relation "account" and dynamic characteristics (e.g. payments debited or credited, balances) given in relations "permanent order" and "transaction".
  - Relation "client" describes characteristics of persons who can manipulate with the accounts.
  - One client can have more accounts, more clients can manipulate with single account; clients and accounts are related together in relation "disposition".
  - Relations "loan" and "credit card" describe some services which the bank offers to its clients.
  - More than one credit card can be issued to an account.
  - At most one loan can be granted for an account.
  - Relation "demographic data" gives some publicly available information about the districts (e.g. average salary); additional information about the clients can be deduced from this.

## Operational Data Layer
- Data Structure - ER Diagram
![ER_Diagram](ER_Diagram.png)
This includes 8 relational tables, 1 analytical table, 6 views, 2 stored procedures and 1 trigger.

## Analytics Plan
This analysis aims to derive insights from the Berka dataset, with a focus on understanding the financial behavior of clients, the gender characteristics of their loans, and trends in loan repayment.

1. Client Financial Profiles
   
This section explores how various client attributes and their financial behaviors are associated with their loans.

Distribution of loan amounts across client demographics: Understand how loan amounts vary by gender, loan type, and region.
Loan amount categorization: Classify clients into segments (e.g., Top 20% loan amount vs. Below 80% loan amount) based on their loan amounts and financial behavior.
Loan default patterns by region: Identify if there is a regional correlation to loan defaults and examine how different regions contribute to the overall default rate.
Average transaction amounts by loan type: Calculate the average transaction amount per loan type (e.g., personal loans, home loans), and observe trends within each loan type.

2. Loan Status by Demographics
   
This section will analyze the relationship between demographic factors (like gender) and loan status, providing insights into loan performance across different client groups.

Loan status and gender relationship: Explore how loan status (e.g., default, repaid) varies across different genders. Are males or females more likely to default?
Loan amounts by gender: Compare the average loan amounts for male and female clients and assess any gender-based disparities.

3. Loan Duration and Gender

This analysis looks at how the loan duration correlates with gender, exploring whether certain genders take loans for longer or shorter periods on average.

Average loan duration by gender: Assess the average duration of loans for different genders and determine whether one group tends to take longer-term loans than the other.
Loan amount versus loan duration: Investigate whether the loan amount is a significant factor in determining the loan duration for different genders.

4. Monthly Loan Trends

Analyzing loan patterns over time will provide insights into when clients are more likely to take out loans, and whether there are seasonal variations.

Month-wise loan activity: Identify which months experience the highest loan issuance. Are loans concentrated during specific times of the year?
Monthly loan amount trends: Analyze the average loan amount per month to identify if there are seasonal spikes or downturns in loan amounts.

5. Loan Default and Pension Payments by Gender

Understanding how gender relates to loan defaults and pension payments will provide deeper insights into client behavior.

Gender-based pension payments: Analyze pension payments by gender to see if there are any differences in the average pension amounts received.
Loan defaults and gender: Investigate whether there are gender-specific trends in loan defaults and if these clients are taking larger or smaller loans.

6. Loan Amounts and Regional Variations

Regional differences play a significant role in understanding loan behavior, and analyzing these trends will help identify areas with higher loan demand or higher default rates.

Loan amounts across districts: Compare the average loan amount by district to determine which districts issue larger loans.
Loan counts by districts: Analyze how many loans are being issued in each district and which districts contribute most to the overall loan volume.

## Analytical Data Layer : `analytical_data`
Denormalized Data Structure:
Columns:
- Loan Information:
  - `loan_id`: Primary identifier for each loan
  - `loan_amount`: Amount of the loan
  - `loan_duration`: Duration of the loan
  - `loan_payments`: Monthly Payments on Loan
  - `loan_status`: Categorical status of the loan (A' stands for contract finished, no problems; 'B' stands for contract finished, loan not payed;
'C' stands for running contract, OK thus-far; 'D' stands for running contract, client in debt)
  - `loan_date`: Date the loan was issued

- Transaction Information:
  - `most_frequent_type`: Most frequent transaction type (debit/credit).
  - `total_transaction_amount`: Total sum of all transactions 
  - `avg_transaction_balance`: Average balance of transactions
  - `transaction_count`: Total number of transactions
  - `most_frequent_k_symbol`: The most frequent transaction characteristic

- Credit Card Information:
  - `credit_type`: Type of credit associated with the client, such as 'Junior', 'Classic', and 'Gold'

- Client Information:
  - `gender`: Gender of the client

- District Information:
  - `district_name`: Name of the district where the client resides
  - `district_average_salary`: Average salary of the district's population

## ETL Pipeline
Information on how to get in touch...

## Views
Guidelines for contributing...

## Conclusion
Details about the project’s license...

## Contact
Information on how to get in touch...
