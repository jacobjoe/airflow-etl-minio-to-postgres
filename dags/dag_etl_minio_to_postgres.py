import hashlib
import io
import logging
from datetime import datetime, timedelta
import numpy as np
import pandas as pd
import psycopg2
from pandas.api.types import is_object_dtype, is_string_dtype, is_integer_dtype
from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

config = {
            'host': 'host.docker.internal',
            'database': 'etl_cleaned',
            'user': 'airflow',
            'password': 'airflow'
        }

default_args = {
    'owner': 'airflow',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}

id_mapping = {}


# to read files from minio
def read_files_minio(connection_id, file_name_with_ext, source_bucket = 'raw-data', ip_folder = 'raw-files'):
    s3_hook = S3Hook(aws_conn_id = connection_id)
    
    file_name = file_name_with_ext.split('.')[0]
    file_type = file_name_with_ext.split('.')[1]
    
    key = f"{ip_folder}/{file_name_with_ext}"
    file_obj = s3_hook.get_key(key = key, bucket_name = source_bucket)
    file_content = file_obj.get()['Body'].read()
    
    if file_type in ['xlsx', 'xls']:
        df = pd.read_excel(io.BytesIO(file_content))
    elif file_type == 'csv':
        df = pd.read_csv(io.BytesIO(file_content))
    else:
        raise ValueError(f"Unsupported file type: {file_type}")
    
    return {"dataframe": df, "file_name": file_name}


# to write files to minio
def write_files_minio(connection_id, df, file_name, target_bucket = 'bronze', op_folder = 'bronze-files'):
    s3_hook = S3Hook(aws_conn_id = connection_id)

    if not s3_hook.check_for_bucket(target_bucket):
        s3_hook.create_bucket(target_bucket)

    csv_bytes = df.to_csv(index = False).encode('utf-8')
    file_name = file_name.lower().replace(' ', '_')
    processed_key = f"{op_folder}/{file_name}.csv"

    s3_hook.load_bytes(bytes_data = csv_bytes,
                       key = processed_key,
                       bucket_name = target_bucket,
                       replace = True)

    return f"{target_bucket}/{processed_key}"


# to standardize strings
def standardize(word):
    result = [word[0]]

    for i in range(1, len(word)):
        if word[i].isupper():
            if not (word[i - 1].isupper() or word[i - 1] == '_'):
                result.append('_')

        result.append(word[i])

    final_str = ''.join(result).lower().strip().replace(' ', '')

    return final_str


# to trim, convert to lowercase, and replace empty string with nan 
def trim_strings(df):
    str_cols = df.select_dtypes(include = ['object']).columns

    df[str_cols] = df[str_cols].apply(lambda col: col.str.strip().str.lower())
    df.replace('', np.nan, inplace = True)

    return df


# to replace null values with -1, and unknown
def fill_nulls(df):
    for col in df.columns:
        if is_integer_dtype(df[col]):
            df[col] = df[col].fillna(-1)

        elif is_object_dtype(df[col]) or is_string_dtype(df[col]):
            df[col] = df[col].fillna('unknown')

    return df


# to create postgres table
def postgres_table_creation(config, schema_name, table_query):
    conn = psycopg2.connect(**config)
    try:
        with conn:
            with conn.cursor() as cursor:
                schema_query = f"CREATE SCHEMA IF NOT EXISTS {schema_name}"
                cursor.execute(schema_query)
                cursor.execute(table_query)
    finally:
        conn.close()

    return "Table created"


# to push data to postgres
def push_to_postgres(config, df, query, key_name=None):
    global id_mapping

    with psycopg2.connect(**config) as conn:
        with conn.cursor() as cursor:
            for _, row in df.iterrows():
                vals = tuple(row[col].item() if hasattr(row[col], "item") else row[col] for col in df.columns)
                cursor.execute(query, vals)

                if key_name:
                    generated_id = cursor.fetchone()[0]
                    id_mapping[row[key_name]] = generated_id

            conn.commit()


# to read from postgres
def read_postgres(config, table_name):
    query = f"SELECT * FROM {table_name}"
    
    with psycopg2.connect(**config) as conn:
        df = pd.read_sql_query(query, conn)
        
    return df


# to get unique target values
def get_unique_targets(df, col_name = 'target_table'):
    unique_values = list(set(x.lower().strip() for x in df[col_name].unique()))

    return unique_values


# to get a dictionary of columns
def get_dict_of_columns(target_cols, df, main_table = 'property', df_keys = None, include_keys = False):
    dict_cols = dict()

    if include_keys:
        for target_col in target_cols:
            if target_col == main_table:
                selected_cols = list(df[df['target_table'].str.lower() == target_col]['column_name'])
                initial_col = ['natural_key', 'property_key']
                added_cols = [x + '_key' for x in df_keys]
                initial_col.extend(added_cols)
                initial_col.extend(selected_cols)
                dict_cols[target_col] = initial_col

            elif target_col in df_keys:
                selected_cols = list(df[df['target_table'].str.lower() == target_col]['column_name'])
                initial_col = []
                initial_col.append(df_keys[df_keys.index(target_col)] + '_key')
                initial_col.extend(selected_cols)
                dict_cols[target_col] = initial_col

            else:
                initial_col = ['property_key']
                selected_cols = list(df[df['target_table'].str.lower() == target_col]['column_name'])
                initial_col.extend(selected_cols)
                dict_cols[target_col] = initial_col

    else:
        for target_col in target_cols:
            dict_cols[target_col] = list(df[df['target_table'].str.lower() == target_col]['column_name'])
    
    return dict_cols


# to get dictionary of dataframes
def get_individual_dfs_in_dict(df, grouped_cols):
    grouped_df_dict = dict()

    for target in grouped_cols:
        grouped_df_dict[target] = df[grouped_cols[target]].copy()

    return grouped_df_dict


# to generate natural key
def generate_natural_key(df, cols, delimiter = '|'):

    return df[cols].astype(str).agg(lambda row: delimiter.join(row.values), axis = 1)


# to generate hask key
def generate_hash_columns(df, cols, hash_len = 16):
    key_str = ''.join(str(df[col]) for col in cols)

    return hashlib.sha256(key_str.encode()).hexdigest()[:hash_len]


# to remove duplicates from dataframe
def duplicates_removal(df):
    df = df.drop_duplicates()

    return df


# to replace key column with id column
def replace_key_with_id(df, id_col, key_col):
    df[id_col] = df[key_col].map(id_mapping)

    df.drop([key_col], axis = 1, inplace = True)

    return df


# to re-order the columns of dataframe
def re_order_df(df):
    cols = df.columns
    last_col = cols[-1:]
    remaining_cols = cols[:-1]

    new_order = list(last_col) + list(remaining_cols)
    df = df[new_order]

    return df



@dag(dag_id = 'dag_etl_minio_to_postgres_v01',
     description = 'This is a etl from Minio to Postgres',
     default_args = default_args,
     start_date = datetime(2025, 9, 29),
     schedule = '@daily',
     catchup = False,
     tags = ['etl', 'postgres', 'minio', 'taskflow-api'])
def etl_minio_to_postgres():
    
    @task()
    def bronze():
        read_fake_data = read_files_minio('minio_conn', 'fake_data.csv')
        df = read_fake_data['dataframe']
        file_name = read_fake_data['file_name']
        fake_data_write_path = write_files_minio('minio_conn', df, file_name)

        read_fake_data = read_files_minio('minio_conn', 'Field Config.xlsx')
        df = read_fake_data['dataframe']
        file_name = read_fake_data['file_name']
        field_config_write_path = write_files_minio('minio_conn', df, file_name)

        return [fake_data_write_path, field_config_write_path]
    

    @task()
    def silver(paths):
        fake_data_path = paths[0]
        field_config_path = paths[1]

        file_bucket = fake_data_path.split('/')[0]
        file_folder = fake_data_path.split('/')[1]
        fake_file_name_xt = fake_data_path.split('/')[2]

        read_fake_data = read_files_minio('minio_conn', fake_file_name_xt, source_bucket = file_bucket, ip_folder = file_folder)
        fake_df = read_fake_data['dataframe']
        fake_file_name = read_fake_data['file_name']

        fake_df.columns = [standardize(x) for x in fake_df.columns]
        fake_df = trim_strings(fake_df)
        fake_df = fill_nulls(fake_df)

        field_file_name_xt = field_config_path.split('/')[2]

        read_field_data = read_files_minio('minio_conn', field_file_name_xt, source_bucket = file_bucket, ip_folder = file_folder)
        field_df = read_field_data['dataframe']
        field_file_name = read_field_data['file_name']

        field_df.columns = [standardize(x) for x in field_df.columns]
        field_df['column_name'] = [standardize(x) for x in field_df['column_name']]
        field_df = trim_strings(field_df)


        fake_table_creation_query = """
                                        CREATE TABLE IF NOT EXISTS silver.property(
                                            property_title TEXT, address TEXT, reviewed_status TEXT, most_recent_status TEXT,
                                            source TEXT, market TEXT, occupancy VARCHAR(20), flood TEXT, street_address TEXT,
                                            city TEXT, state CHAR(2), zip VARCHAR(10), property_type TEXT, highway TEXT,
                                            train TEXT, tax_rate NUMERIC(5,2), sqft_basement INTEGER, htw TEXT, pool VARCHAR(20),
                                            commercial VARCHAR(20), water TEXT, sewage TEXT, year_built INTEGER, sqft_mu INTEGER,
                                            sqft_total INTEGER, parking TEXT, bed INTEGER, bath INTEGER, basement_yes_no VARCHAR(20),
                                            layout TEXT, net_yield NUMERIC(5,2), irr NUMERIC(5,2), rent_restricted VARCHAR(20),
                                            neighborhood_rating INTEGER, previous_rent INTEGER, list_price INTEGER, zestimate INTEGER,
                                            arv INTEGER, expected_rent INTEGER, rent_zestimate INTEGER, low_fmr INTEGER, high_fmr INTEGER,
                                            hoa INTEGER, underwriting_rehab INTEGER, rehab_calculation INTEGER, paint TEXT, 
                                            flooring_flag VARCHAR(20), foundation_flag VARCHAR(20), roof_flag VARCHAR(20), hvac_flag VARCHAR(20),
                                            kitchen_flag VARCHAR(20), bathroom_flag VARCHAR(20), appliances_flag VARCHAR(20), windows_flag VARCHAR(20),
                                            landscaping_flag VARCHAR(20), trashout_flag VARCHAR(20), latitude NUMERIC(10,6), 
                                            longitude NUMERIC(10,6), subdivision TEXT, taxes INTEGER, redfin_value INTEGER,
                                            selling_reason TEXT, seller_retained_broker VARCHAR(20), hoa_flag VARCHAR(20), final_reviewer TEXT,
                                            school_average NUMERIC(4,2)
                                            )
                                    """
        
        field_table_creation_query = """
                                        CREATE TABLE IF NOT EXISTS silver.field (
                                            column_name VARCHAR(30),
                                            target_table VARCHAR(30)
                                            )
                                    """
        
        postgres_table_creation(config, 'silver', fake_table_creation_query)
        postgres_table_creation(config, 'silver', field_table_creation_query)

        fake_data_insert_query = """
                                    INSERT INTO silver.property (
                                        property_title, address, reviewed_status, most_recent_status, source, market, occupancy, flood, street_address,
                                        city, state, zip, property_type, highway, train, tax_rate, sqft_basement, htw, pool, commercial, water, sewage,
                                        year_built, sqft_mu, sqft_total, parking, bed, bath, basement_yes_no, layout, net_yield, irr, rent_restricted,
                                        neighborhood_rating, previous_rent, list_price, zestimate, arv, expected_rent, rent_zestimate, low_fmr, high_fmr,
                                        hoa, underwriting_rehab, rehab_calculation, paint, flooring_flag, foundation_flag, roof_flag, hvac_flag,
                                        kitchen_flag, bathroom_flag, appliances_flag, windows_flag, landscaping_flag, trashout_flag, latitude,
                                        longitude, subdivision, taxes, redfin_value, selling_reason, seller_retained_broker, hoa_flag, final_reviewer,
                                        school_average
                                    ) VALUES (
                                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                                        %s, %s, %s, %s, %s, %s
                                    )
                                """
        
        field_data_insert_query = """
                                    INSERT INTO silver.field (
                                        column_name, target_table
                                    ) VALUES (
                                        %s, %s
                                    )
                                """
        
        logging.info('Pushing fake data to postgres...')
        push_to_postgres(config, fake_df, fake_data_insert_query)

        logging.info('Pushing field config to postgres...')
        push_to_postgres(config, field_df, field_data_insert_query)

        return [fake_data_insert_query.split()[2], field_data_insert_query.split()[2]]
    

    @task()
    def gold(path):
        property_path = path[0]
        field_path = path[1]

        data_df = read_postgres(config, property_path)
        field_df = read_postgres(config, field_path)

        unique_targets = get_unique_targets(field_df)

        grouped_cols = get_dict_of_columns(unique_targets, field_df)

        grouped_dict = get_individual_dfs_in_dict(data_df, grouped_cols)

        valuation_df = grouped_dict['valuation']
        rehab_df = grouped_dict['rehab']
        hoa_df = grouped_dict['hoa']
        taxes_df = grouped_dict['taxes']
        property_df = grouped_dict['property']
        leads_df = grouped_dict['leads']

        cols = ['property_title', 'zip']
        data_df['natural_key'] = generate_natural_key(data_df, cols)

        data_df['hoa_key'] = data_df.apply(lambda row: generate_hash_columns(row, ['hoa', 'hoa_flag']), axis = 1)
        data_df['taxes_key'] = data_df.apply(lambda row: generate_hash_columns(row, ['taxes']), axis = 1)
        data_df['property_key'] = data_df.apply(lambda row: generate_hash_columns(row, ['property_title', 'zip']), axis = 1)

        updated_grouped_cols = get_dict_of_columns(unique_targets, field_df, main_table = 'property', df_keys = ['hoa', 'taxes'], include_keys = True)
        updated_grouped_dict = get_individual_dfs_in_dict(data_df, updated_grouped_cols)

        valuation_df = updated_grouped_dict['valuation']
        rehab_df = updated_grouped_dict['rehab']
        hoa_df = updated_grouped_dict['hoa']
        taxes_df = updated_grouped_dict['taxes']
        property_df = updated_grouped_dict['property']
        leads_df = updated_grouped_dict['leads']

        valuation_df.columns = [standardize(x) for x in valuation_df.columns]
        rehab_df.columns = [standardize(x) for x in rehab_df.columns]
        hoa_df.columns = [standardize(x) for x in hoa_df.columns]
        taxes_df.columns = [standardize(x) for x in taxes_df.columns]
        property_df.columns = [standardize(x) for x in property_df.columns]
        leads_df.columns = [standardize(x) for x in leads_df.columns]

        hoa_df = duplicates_removal(hoa_df)
        taxes_df = duplicates_removal(taxes_df)

        hoa_table_creation_query = """
                                        CREATE TABLE gold.hoa(
                                            id SERIAL PRIMARY KEY,
                                            hoa_key VARCHAR(50) UNIQUE,
                                            hoa INTEGER,
                                            hoa_flag VARCHAR(10)
                                        )
                                    """
        
        taxes_table_creation_query = """
                                        CREATE TABLE gold.taxes(
                                            id SERIAL PRIMARY KEY,
                                            taxes_key VARCHAR(50) UNIQUE,
                                            taxes INTEGER
                                        )
                                    """

        property_table_creation_query = """
                                        CREATE TABLE gold.property (
                                            id SERIAL PRIMARY KEY, natural_key VARCHAR(255), property_key VARCHAR(50), hoa_key VARCHAR(50),
                                            taxes_key VARCHAR(50), property_title VARCHAR(255), address VARCHAR(255), market VARCHAR(20),
                                            flood VARCHAR(20), street_address VARCHAR(255), city VARCHAR(30), state CHAR(2), zip VARCHAR(10),
                                            property_type VARCHAR(30), highway VARCHAR(10), train VARCHAR(10), tax_rate DECIMAL(5, 2),
                                            sqft_basement INT, htw VARCHAR(10), pool VARCHAR(10), commercial VARCHAR(10), water VARCHAR(10),
                                            sewage VARCHAR(10), year_built INT, sqft_mu INT, sqft_total INT, parking VARCHAR(20), bed INT,
                                            bath INT, basement_yes_no VARCHAR(10), layout VARCHAR(20), rent_restricted VARCHAR(10),
                                            neighborhood_rating INT, latitude DECIMAL(10, 6), longitude DECIMAL(10, 6), subdivision VARCHAR(20),
                                            school_average DECIMAL(4, 2), FOREIGN KEY (hoa_key) REFERENCES gold.hoa(hoa_key),
                                            FOREIGN KEY (taxes_key) REFERENCES gold.taxes(taxes_key)
                                        )
                                    """

        postgres_table_creation(config, 'gold', hoa_table_creation_query)
        postgres_table_creation(config, 'gold', taxes_table_creation_query)
        postgres_table_creation(config, 'gold', property_table_creation_query)

        hoa_data_insert_query = """
                                    INSERT INTO gold.hoa (
                                       hoa_key, hoa, hoa_flag
                                    ) VALUES (
                                        %s, %s, %s
                                    )
                                """

        taxes_data_insert_query = """
                                    INSERT INTO gold.taxes (
                                       taxes_key, taxes
                                    ) VALUES (
                                        %s, %s
                                    )
                                """

        property_data_insert_query = """
                                    INSERT INTO gold.property (
                                        natural_key, property_key, hoa_key, taxes_key, property_title, address, market, flood, street_address, city, state, 
                                        zip, property_type, highway, train, tax_rate, sqft_basement, htw, pool, commercial, water, sewage, year_built, 
                                        sqft_mu, sqft_total, parking, bed, bath, basement_yes_no, layout, rent_restricted, neighborhood_rating, latitude, 
                                        longitude, subdivision, school_average
                                    ) 
                                    VALUES(
                                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                                        %s, %s, %s, %s, %s, %s
                                    )
                                    RETURNING id;
                                """

        logging.info('Pushing hoa to postgres...')
        push_to_postgres(config, hoa_df, hoa_data_insert_query)

        logging.info('Pushing taxes to postgres...')
        push_to_postgres(config, taxes_df, taxes_data_insert_query)

        logging.info('Pushing property to postgres...')
        push_to_postgres(config, property_df, property_data_insert_query, 'property_key')

        leads_df = replace_key_with_id(leads_df, 'property_id', 'property_key')
        rehab_df = replace_key_with_id(rehab_df, 'property_id', 'property_key')
        valuation_df = replace_key_with_id(valuation_df, 'property_id', 'property_key')

        leads_df = re_order_df(leads_df)
        rehab_df = re_order_df(rehab_df)
        valuation_df = re_order_df(valuation_df)

        leads_table_creation_query = """
                                        CREATE TABLE gold.leads (
                                            id SERIAL PRIMARY KEY,
                                            property_id INTEGER,
                                            reviewed_status VARCHAR(20),
                                            most_recent_status VARCHAR(20),
                                            lead_source VARCHAR(20),
                                            occupancy VARCHAR(10),
                                            net_yield DECIMAL(5, 2),
                                            irr DECIMAL(5, 2),
                                            selling_reason VARCHAR(20),
                                            seller_retained_broker VARCHAR(10),
                                            final_reviewer VARCHAR(50),
                                            FOREIGN KEY (property_id) REFERENCES gold.property(id)
                                        )
                                    """
        
        rehab_table_creation_query = """
                                        CREATE TABLE gold.rehab (
                                            id SERIAL PRIMARY KEY,
                                            property_id INTEGER,
                                            underwriting_rehab INT,
                                            rehab_calculation INT,
                                            paint VARCHAR(10),
                                            flooring_flag VARCHAR(10),
                                            foundation_flag VARCHAR(10),
                                            roof_flag VARCHAR(10),
                                            hvac_flag VARCHAR(10),
                                            kitchen_flag VARCHAR(10),
                                            bathroom_flag VARCHAR(10),
                                            appliances_flag VARCHAR(10),
                                            windows_flag VARCHAR(10),
                                            landscaping_flag VARCHAR(10),
                                            trashout_flag VARCHAR(10),
                                            FOREIGN KEY (property_id) REFERENCES gold.property(id)
                                        )
                                    """
        
        val_table_creation_query = """
                                        CREATE TABLE gold.valuation (
                                            id SERIAL PRIMARY KEY,
                                            property_id INTEGER,
                                            previous_rent INT,
                                            list_price DECIMAL(10, 2),
                                            zestimate DECIMAL(10, 2),
                                            arv DECIMAL(10, 2),
                                            expected_rent DECIMAL(10, 2),
                                            rent_zestimate DECIMAL(10, 2),
                                            low_fmr DECIMAL(10, 2),
                                            high_fmr DECIMAL(10, 2),
                                            redfin_value DECIMAL(10, 2),
                                            FOREIGN KEY (property_id) REFERENCES gold.property(id)
                                        )
                                    """

        postgres_table_creation(config, 'gold', leads_table_creation_query)
        postgres_table_creation(config, 'gold', rehab_table_creation_query)
        postgres_table_creation(config, 'gold', val_table_creation_query)

        leads_data_insert_query = """
                                    INSERT INTO gold.leads (
                                        property_id, reviewed_status, most_recent_status, lead_source, occupancy, net_yield, irr, selling_reason, 
                                        seller_retained_broker, final_reviewer
                                    )
                                    VALUES(
                                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                                    )
                                """
        
        rehab_data_insert_query = """
                                    INSERT INTO gold.rehab (
                                        property_id, underwriting_rehab, rehab_calculation, paint, flooring_flag, foundation_flag, roof_flag, hvac_flag,
                                        kitchen_flag, bathroom_flag, appliances_flag, windows_flag, landscaping_flag, trashout_flag
                                    )
                                    VALUES(
                                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                                        %s, %s, %s, %s
                                    )
                                """
        
        vals_data_insert_query = """
                                    INSERT INTO gold.valuation (
                                        property_id, previous_rent, list_price, zestimate, arv, expected_rent, rent_zestimate, low_fmr, high_fmr, 
                                        redfin_value
                                    )
                                    VALUES(
                                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                                    )
                                """

        logging.info('Pushing leads to postgres...')
        push_to_postgres(config, leads_df, leads_data_insert_query)

        logging.info('Pushing rehab to postgres...')
        push_to_postgres(config, rehab_df, rehab_data_insert_query)

        logging.info('Pushing valuation to postgres...')
        push_to_postgres(config, valuation_df, vals_data_insert_query)

        return 'All tables inserted successfully in Gold!!!'
    

    bronze_path = bronze()
    silver_path = silver(bronze_path)
    gold(silver_path)


dag_instance = etl_minio_to_postgres()
