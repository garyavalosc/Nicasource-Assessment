# Nicasource-Assessment

## Introduction

This is a Python script that performs the following tasks:
- Reads CSV files from the Data folder
- Extracts data from the CSV files
- Transforms the extracted data
- Loads the transformed data into a MySQL database

## Prerequisites
- Python3 installed on your system
- mysql.connector module installed in your python environment
- pandas module installed in your python environment

## Access to a MySQL database with the following details:
- Host
- User
- Password
- Database name

## The following environment variables should be set on your system:
- DB_HOST
- DB_USER
- DB_PASSWORD
- DB_NAME

## Usage
- Clone the repository to your local system
- Create a virtual environment and activate it
- Install the required modules (mysql.connector, pandas)
- Set the environment variables with the database details
- Place the CSV files to be processed in the Data folder
- Run the script by executing the following command in your terminal:
  python car_sales_pipeline.py
  
## Expected Output
The script should extract the data from the CSV files, transform the data, and load it into the MySQL database. The script will log the progress of the process and any error messages.

### Summary

This pipeline is for processing car sales data stored in CSV files and storing it in a database. The pipeline is developed using several libraries including glob, pandas, os, mysql.connector, and datetime. The pipeline has the following steps: (1) Connect to database, (2) Extract data from CSV files, (3) Transform the extracted data, (4) Load data into the database. In the Extract step, the pipeline selects all CSV files in a folder and reads the header to check if the header is in the expected format. If it is not, an exception is raised. The Transform step removes rows with missing values, validates the date and price format, converts date format to a standard format, and converts car model values to numerical codes. The Load step loads the transformed data into the database.
