import glob                         # this module helps in selecting files 
import pandas as pd                 # this module helps in processing CSV files
import os                           # this module helps in accessing the env. variables
import mysql.connector              # this module helps in connecting to the DB  
from datetime import datetime       # this module helps in working with date formats
import csv
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
        

        return connection
    except KeyError as e:
        log(f'Error: Missing environment variable {e}')
        return None
    except mysql.connector.Error as e:
        log(f'Error connecting to the database: {e}')
        return None


# Extract 
def extract_from_csv(file_to_process):
    dataframe = pd.read_csv(file_to_process)
    return dataframe

# CSV Extract Function
def extract():
    # Create an empty data frame to hold extracted data
    
    extracted_data = pd.DataFrame(columns=['brand','car_model','date_of_sale','car_price'])
    
    expected_columns = ['brand','car_model','date_of_sale','car_price']
    # Process all csv files
    for csv_file in glob.glob(path + '\\Data\\*.csv'):
        with open(csv_file, 'r') as csvfile:
            reader = csv.reader(csvfile)
            header = next(reader)

            if header != expected_columns:
                log(f'Unexpected columns in file. Expected: {expected_columns}. Found: {header}')
                raise Exception(f'Unexpected columns in file. Expected: {expected_columns}. Found: {header}')
            else:
        
                extracted_data = extracted_data.append(extract_from_csv(csv_file), ignore_index=True)
    return extracted_data

# Transform
def transform(data):
    # Removing rows with missing values
    data.dropna(inplace=True)
    
    # validating data types
    data['date_of_sale_valid'] = data['date_of_sale'].apply(lambda x: is_valid_date(x))
    data['car_price_valid'] = data['car_price'].apply(lambda x: is_valid_price(x))
    invalid_rows = data.index[(data['date_of_sale_valid'] == False) | (data['car_price_valid'] == False)].tolist()
    if invalid_rows:
        log(f'Invalid formats in rows: {invalid_rows} rows will be skipped')
    
    data = data[data['date_of_sale'].apply(lambda x: is_valid_date(x))]
    data = data[data['car_price'].apply(lambda x: is_valid_price(x))]
    
        
        
    # Column date to standart format
    data['date_of_sale'] = pd.to_datetime(data['date_of_sale'], format='%d/%m/%Y')
    # New column to store year as integer
    data['year_of_sale'] = data['date_of_sale'].dt.year
    
    # Categorical car model values to numerical
    # Replace categorical values fo Car Model with numerical values
    model_map = pd.read_csv('car_model_mapping.csv').set_index('Car Model').to_dict()['Code']
    new_models = set(data['car_model'].unique()) - set(model_map.keys())
    
    # Run only if a new car model appears
    # In case a new car model appears we should add it to our model map in order to keep consistency
    if new_models:
        for model in new_models:
            model_map[model] = max(model_map.values()) + 1
        
        # Saving car model updated file
        pd.DataFrame.from_dict(model_map, orient='index', columns=['Code']).reset_index().rename(columns={'index': 'Car Model'}).to_csv('car_model_mapping.csv', index=False)
        
        
    data['car_model'] = data['car_model'].map(model_map)
    
    data_transformed = data[['brand','car_model','date_of_sale','car_price','year_of_sale']]
    
    return data_transformed


# Loading
def load(data_to_load):
    
    # from DF to list of tuples
    list_tuple = [tuple(row) for index, row in transformed_data.iterrows()]
    # Format the dates in the list of tuples as strings
    formatted_car_sales = [(brand, model, date.strftime("%Y-%m-%d"), price, year) for brand, model, date, price, year in list_tuple]
    
    # inserting into the DB
    query = 'INSERT INTO car_sales (Brand, car_model, date_of_sale, car_price, year_of_sale) VALUES (?, ?, ?, ?, ?)'
    
    # calling the connection function
    conn = connect_to_database()
    if conn:
        
        log('Successfully connected to the database')
        cursor = conn.cursor()
        
        # executing query
        cursor.executemany(query, formatted_car_sales)
        conn.commit
        log(f'Number of rows inserted: {cursor.rowcount}')
        
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
        
        
# data type validations functions
def is_valid_date(date_string):
    try:
        datetime.strptime(date_string, '%d/%m/%Y')
        return True
    except ValueError:
        return False

def is_valid_price(price):
    try:
        float(price)
        return True
    except ValueError:
        return False

        
# Running ETL Process
log('ETL job started')


log('Extract job started')
extracted_data = extract()
log('Extract phase ended')
log('Transform phase started')
transformed_data = transform(extracted_data)
log('Transform phase ended')
log('Load phase started')

try:
    load(transformed_data)
    
except Exception as e:
    raise Exception(f'something went wrong: {e}')
log('Load phase ended')







