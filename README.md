# Nicasource-Assessment

Introduction

This is a Python script that performs the following tasks:
-Reads CSV files from the Data folder
-Extracts data from the CSV files
-Transforms the extracted data
-Loads the transformed data into a MySQL database

Prerequisites
-Python3 installed on your system
-mysql.connector module installed in your python environment
-pandas module installed in your python environment

Access to a MySQL database with the following details:
-Host
-User
-Password
-Database name
-Environment Variables

The following environment variables should be set on your system:
-DB_HOST
-DB_USER
-DB_PASSWORD
-DB_NAME

Usage
-Clone the repository to your local system
-Create a virtual environment and activate it
-Install the required modules (mysql.connector, pandas)
-Set the environment variables with the database details
-Place the CSV files to be processed in the Data folder
-Run the script by executing the following command in your terminal:
  python car_sales_pipeline.py
  
Expected Output
The script should extract the data from the CSV files, transform the data, and load it into the MySQL database. The script will log the progress of the process and any error messages.
