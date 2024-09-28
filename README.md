# Data Handling Project

## Table of Contents

- [Project Description](#project-description)
- [What I Learned](#what-i-learned)
- [Installation Instructions](#installation-instructions)
- [Usage Instructions](#usage-instructions)
- [File Structure](#file-structure)

## Project Description

This project involves extracting, transforming, and loading data from multiple sources into a PostgreSQL database named `sales_data`. The data comes from various sources such as AWS RDS databases, APIs, S3 buckets, and PDFs. The goal is to create a star-based schema to consolidate the company's sales data for analysis.

## What I Learned

- **Data Extraction**: Learned how to extract data from databases, APIs, S3 buckets, and PDF files using Python libraries like `requests`, `boto3`, and `tabula-py`.
- **Data Cleaning**: Gained experience in cleaning data using Pandas, handling NULL values, correcting data types, and standardizing formats.
- **Database Operations**: Used SQLAlchemy for database connections and performed SQL queries to alter tables, add primary and foreign keys, and enforce data integrity.
- **Schema Design**: Designed a star schema by setting up dimension tables and a fact table, establishing primary and foreign key relationships.
- **Version Control**: Improved skills in using Git for version control, including staging changes, committing, and pushing to GitHub.
- **Documentation**: Learned the importance of maintaining clear and comprehensive project documentation.

## Installation Instructions

1. **Clone the Repository**

   ```bash
   git clone https://github.com/nishypenguiny/Multinational-Retail-Data-Centralisation.git

2. **Navigate to Project Directory**

    ```bash
    cd Multinational-Retail-Data-Centralisation

3. **Install Required Packages**

    ```bash
    pip install -r requirments.txt

4. **Set Up the PostgreSQL Database**

    Install PostgreSQL and pgAdmin4 if you haven't already.
    Create a new database called sales_data in pgAdmin4.

5. **Configure AWS**

    Ensure that you have the AWS CLI installed and configured with your credentials.

5. **Create a db_creds.yaml File**

    ```yaml
    RDS_HOST: your_host
    RDS_PASSWORD: your_password
    RDS_USER: your_username
    RDS_DATABASE: your_database
    RDS_PORT: 5432
    ```

    **Note**: Make sure to add `db_creds.yaml` to your `.gitignore` file to prevent uploading sensitive information to GitHub.

## Usage Instructions

1. **Data Extraction**

    Run the `data_extraction.py` script to extract data from various sources:

    ```bash
    python data_extraction.py
    ```

    This script will:

    - Connect to the AWS RDS database and extract user data.
    - Retrieve PDF data from an S3 bucket.
    - Fetch store data using the provided API endpoints.
    - Extract product data from an S3 bucket.
    - Read orders data from the RDS database.
    - Load date details from a JSON file.

2. **Data Cleaning**

    Run the `data_cleaning.py` script to clean the extracted data:

    ```bash
    python data_cleaning.py
    ```

    This script will:

    - Clean user data (handle NULL values, fix dates, etc.).
    - Clean card details data (remove erroneous values).
    - Clean store data (correct data types, merge latitude columns).
    - Clean product data (standardize weight units, remove currency symbols).
    - Clean orders data (remove unnecessary columns, correct data types).
    - Clean date details data.

3. **Upload Data to Database**

    The `DatabaseConnector` class in `database_utils.py` is used to upload the cleaned data to your `sales_data` database. Ensure that the upload methods are correctly called in your scripts.

4. **Run SQL Queries**

    Execute the SQL queries saved in `queries.sql` to:

    - Alter data types of columns.
    - Add primary and foreign keys.
    - Perform data analysis as per business requirements.

    You can run these queries using the `psql` command-line tool or through pgAdmin4.

## File Structure

- **data_extraction.py**: Contains the `DataExtractor` class with methods to extract data from different sources.
- **data_cleaning.py**: Contains the `DataCleaning` class with methods to clean each dataset.
- **database_utils.py**: Contains the `DatabaseConnector` class to connect to the database and upload data.
- **main.py**: Contains the final logic to run whatever code needs to be run 
- **queries.sql**: A file containing all the SQL queries used for table alterations and data analysis.
- **requirements.txt**: A list of Python packages required to run the project.
- **README.md**: Project documentation.
- **.gitignore**: Specifies files and directories for Git to ignore.
- **db_creds.yaml**: Contains database credentials (should be ignored by Git).

