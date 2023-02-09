import glob                         # this module helps in selecting files 
import pandas as pd                 # this module helps in processing CSV files
import os                           # this module helps in accessing the env. variables
import mysql.connector              # this module helps in connecting to the DB  
from datetime import datetime       # this module helps in working with date formats

# Folder Path
path = os.getcwd()
    
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
def transform(data):
    # Removing rows with missing values
    data.dropna(inplace = True)
    # Column date to standart format
    data['Date of Sale'] = pd.to_datetime(data['Date of Sale'], format = '%Y-%m-%d')
    # New column to store year as integer
    data['Year of Sale'] = data['Date of Sale'].dt.year
    
    # Categorical car model values to numerical
    # Replace categorical values fo Car Model with numerical values
    model_map = pd.read_csv('car_model_mapping.csv').set_index('Car Model').to_dict()['Code']
    data['Car Model'] = data['Car Model'].map(model_map)
    # In case a new car model appears we should add it to our model map in order to keep consistency
    new_models = set(data['Car Model'].unique()) - set(model_map.keys())
    # Run only if a new car model appears
    if new_models:
        for model in new_models:
            model_map[model] = max(model_map.values()) + 1
        
        # Saving car model updated file
        pd.DataFrame.from_dict(model_map, orient='index').reset_index().to_csv('car_model_mapping.csv', index=False, header=False)
    
    return data
# Loading
def load(data_to_load):
    
    # from DF to list of tuples
    list_tuple = [tuple(x) for x in data_to_load.numpy()]
    
    # inserting into the DB
    columns = ','.join(data_to_load.columns)
    # creating place holders to prevent SQL injections attacks
    values = ','.join(['%s' for i in range(len(data_to_load))])
    query = f'INSER INTO car_sales ({columns}) VALUES({values})'
    
    # calling the connection function
    conn = connect_to_database()
    if conn:
        
        log('Successfully connected to the database')
        cursor = conn.cursor()
        
        # executing query
        cursor.executemany(query, list_tuple)
        conn.commit
        
        # closing connection
        cursor.close()
        conn.close()
    
# Logging
def log(message):
    timestamp_format = '%Y-%h-%d-%H:%M:%S'
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open('logfile.txt','a') as f:
        f.write(timestamp + ',' + message + '\n')
        
        
# Running ETL Process
log('ETL job started')
log('Extract job started')
extracted_data = extract()
log('Extract phase ended')
log('Transform phase started')
transformed_data = transform(extracted_data)
log('Transform phase ended')
log('Load phase started')
load(transformed_data)
log('Load phase ended')





