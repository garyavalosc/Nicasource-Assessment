import glob                         # this module helps in selecting files 
import pandas as pd                 # this module helps in processing CSV files
import os                           # this module helps in accessing the env. variables
import mysql.connector              # this module helps in connecting to the DB  

# Folder Path
path = os.getcwd()
print(path)
 
# DB Connection
def connect_to_database():
    try:
        # Retrieve the values of the environment variables
        host = os.environ['DB_HOST']
        user = os.environ['DB_USER']
        password = os.environ['DB_PASSWORD']
        database = os.environ['DB_NAME']
        
        connection = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )
        
        print('Successfully connected to the database.')
        return connection
    except KeyError as e:
        print(f'Error: Missing environment variable {e}')
        return None
    except mysql.connector.Error as e:
        print(f'Error connecting to the database: {e}')
        return None


# Extract 
def extract_from_csv(file_to_process):
    dataframe = pd.read_csv(file_to_process)
    return dataframe

# CSV Extract Function
def extract():
    # Create an empty data frame to hold extracted data
    extracted_data = pd.DataFrame(columns=['Brand','Car Model','Date of Sale','Car Price'])
    
    # Process all csv files
    for csv_file in glob.glob(path + '\\Data\\*.csv'):
        extracted_data = extracted_data.append(extract_from_csv(csv_file), ignore_index = True)
    return extracted_data

# Transform
# Loading
# Logging
# Running ETL Process
