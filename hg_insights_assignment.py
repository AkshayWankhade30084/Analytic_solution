
def data_transform(raw_df):
    """Transform raw data into clean format"""
    # Data Transformation
    default_values = {
        'Age': 0,
        'Tenure': 0,
        'MonthlyCharges': np.nanmedian(raw_df['MonthlyCharges']),
        'TotalCharges': np.nanmedian(raw_df['TotalCharges']),
        'Gender': 'Unknown',
        'ContractType': 'Unknown',
        'InternetService': 'Unknown',
        'TechSupport': 'Unknown',
        'Churn': 'No'
    }

    # Missing value handling
    for col in ['Age', 'Tenure', 'MonthlyCharges', 'TotalCharges', 'Gender',
                'ContractType', 'InternetService', 'TechSupport', 'Churn']:
        if col in raw_df.columns:
            raw_df[col] = raw_df[col].fillna(default_values[col])

    # Standardizing the values
    raw_df['Gender'] = raw_df['Gender'].str.upper()
    raw_df['Churn'] = raw_df['Churn'].replace({'Yes': 1, 'No': 0, True: 1, False: 0})

    # New feature calculation
    raw_df['LifetimeValue'] = raw_df['MonthlyCharges'] * raw_df['Tenure']

    # Add current timestamp to DataFrame
    utc_now = datetime.now(pytz.utc)
    ist_time = utc_now.astimezone(pytz.timezone('Asia/Kolkata'))
    raw_df['load_timestamp_ist'] = ist_time.strftime('%Y-%m-%d %H:%M:%S')

    return raw_df


def create_dimension_tables():
    """Create all dimension tables"""
    cursor = conn.cursor()

    # Customer dimension
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS dim_customer (
        customer_key INTEGER PRIMARY KEY AUTOINCREMENT,
        customer_id INTEGER UNIQUE ,
        age INTEGER,
        age_group TEXT,
        gender TEXT,
        valid_from DATETIME DEFAULT CURRENT_TIMESTAMP,
        valid_to DATETIME DEFAULT '9999-12-31',
        is_current BOOLEAN DEFAULT 1
    )
    """)

    # Contract dimension
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS dim_contract (
        contract_key INTEGER PRIMARY KEY AUTOINCREMENT,
        contract_type TEXT UNIQUE,
        duration_months INTEGER,
        is_month_to_month BOOLEAN
    )
    """)

    # Service dimension
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS dim_service (
        service_key INTEGER PRIMARY KEY AUTOINCREMENT,
        internet_service TEXT UNIQUE,
        service_category TEXT,
        has_service BOOLEAN
    )
    """)

    # Tech Support dimension
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS dim_tech_support (
        support_key INTEGER PRIMARY KEY AUTOINCREMENT,
        tech_support TEXT UNIQUE,
        has_support BOOLEAN
    )
    """)

    # Time dimension (based on tenure)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS dim_tenure (
        tenure_key INTEGER PRIMARY KEY,
        tenure_months INTEGER UNIQUE,
        tenure_years INTEGER,
        tenure_category TEXT
    )
    """)

    conn.commit()


def create_fact_table():
    """Create the fact table"""
    cursor = conn.cursor()
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS fact_churn (
        fact_key INTEGER PRIMARY KEY AUTOINCREMENT,
        customer_key INTEGER,
        contract_key INTEGER,
        service_key INTEGER,
        support_key INTEGER,
        tenure_key INTEGER,
        monthly_charges REAL,
        total_charges REAL,
        lifetime_value REAL,
        churn_status BOOLEAN,
        load_timestamp DATETIME,
        FOREIGN KEY (customer_key) REFERENCES dim_customer(customer_key),
        FOREIGN KEY (contract_key) REFERENCES dim_contract(contract_key),
        FOREIGN KEY (service_key) REFERENCES dim_service(service_key),
        FOREIGN KEY (support_key) REFERENCES dim_tech_support(support_key),
        FOREIGN KEY (tenure_key) REFERENCES dim_tenure(tenure_key)
    )
    """)
    conn.commit()


def load_dimension_data(transformed_df):
    """Load data into dimension tables"""

    # Load dim_customer
#transformed_df=data_clean
    customer_data = transformed_df[['customer_id', 'age', 'gender']].drop_duplicates()
    customer_data['age_group'] = pd.cut(customer_data['age'],
                                      bins=[0, 18, 30, 45, 60, 100],
                                      labels=['<18', '18-30', '31-45', '46-60', '60+'])
  #   cursor.execute("""
  #  delete from  dim_customer
  #   """)
    cursor.executemany("""
    INSERT INTO dim_customer (customer_id, age, gender, age_group)
    VALUES (?, ?, ?, ?)
    ON CONFLICT(customer_id) DO UPDATE SET
        age = excluded.age,
        gender = excluded.gender,
        age_group = excluded.age_group
""", customer_data[['customer_id', 'age', 'gender', 'age_group']].values.tolist())

    # Load dim_contract
    # Load dim_contract
    contract_data = pd.DataFrame({
        'contract_type': ['Month-to-Month','One-Year','Two-Year'],
        'duration_months': [1, 12, 24],
        'is_month_to_month': [True, False, False]
    })
  #   cursor.execute("""
  #  delete from  dim_contract
  #   """)
    cursor.executemany("""
    INSERT INTO dim_contract (contract_type, duration_months, is_month_to_month)
    VALUES (?, ?, ?)
    ON CONFLICT(contract_type) DO UPDATE SET
        duration_months = excluded.duration_months,
        is_month_to_month = excluded.is_month_to_month
""", contract_data[['contract_type', 'duration_months', 'is_month_to_month']].values.tolist())


     # Load dim_service
    service_data = pd.DataFrame({
         'internet_service': ['Fiber Optic', 'DSL', 'Unknown'],
         'service_category': ['Premium', 'Standard', 'None'],
         'has_service': [True, True, False]
     })

    # Then use UPSERT for insertion/updates
    cursor.executemany("""
    INSERT INTO dim_service (internet_service, service_category, has_service)
    VALUES (?, ?, ?)
    ON CONFLICT(internet_service) DO UPDATE SET
        service_category = excluded.service_category,
        has_service = excluded.has_service
""", service_data[['internet_service', 'service_category', 'has_service']].values.tolist())

     #Load dim_tech_support
    support_data = pd.DataFrame({
         'tech_support': ['Yes', 'No', 'Unknown'],
         'has_support': [True, False, False]
     })

    # Then use UPSERT for smart inserts/updates
    cursor.executemany("""
    INSERT INTO dim_tech_support (tech_support, has_support)
    VALUES (?, ?)
    ON CONFLICT(tech_support) DO UPDATE SET
        has_support = excluded.has_support
""", support_data[['tech_support', 'has_support']].values.tolist())

    # Load dim_tenure
    max_tenure = transformed_df['tenure_months'].max()
    tenure_data = pd.DataFrame({'tenure_months': range(0, int(max_tenure)+1)})
    tenure_data['tenure_key'] = tenure_data['tenure_months'] + 1
    tenure_data['tenure_years'] = tenure_data['tenure_months'] // 12
    tenure_data['tenure_category'] = pd.cut(tenure_data['tenure_months'],
                                           bins=[-1, 0, 6, 12, 24, 60, float('inf')],
                                           labels=['New', '0-6m', '6-12m', '1-2y', '2-5y', '5y+'])

    cursor.executemany("""
    INSERT INTO dim_tenure (tenure_key, tenure_months, tenure_years, tenure_category)
    VALUES (?, ?, ?, ?)
    ON CONFLICT(tenure_months) DO UPDATE SET
        tenure_years = excluded.tenure_years,
        tenure_category = excluded.tenure_category
""", tenure_data[['tenure_key', 'tenure_months', 'tenure_years', 'tenure_category']].values.tolist())

    # def drop_table_diemension():

    #   ## note: this method to just avoid duplicate. In real time needto implement change data capture here as done for fact table
    #   cursor.execute("""DROP TABLE IF EXISTS dim_customer""")
    #   cursor.execute("""DROP TABLE IF EXISTS dim_contract""")
    #   cursor.execute("""DROP TABLE IF EXISTS dim_service""")
    #   cursor.execute("""DROP TABLE IF EXISTS dim_tech_support""")
    #   cursor.execute("""DROP TABLE IF EXISTS dim_tenure""")

def incremental_load_fact_data(transformed_df):
    """Load data into fact table incrementally using MERGE-like approach"""
    cursor = conn.cursor()

    try:
        # Create temporary staging table with all required columns
        cursor.execute("""
        CREATE  TABLE IF NOT EXISTS  temp_fact_churn (
            customer_id INTEGER,
            contract_type TEXT,
            internet_service TEXT,
            tech_support TEXT,
            tenure_months INTEGER,
            monthly_charges REAL,
            total_charges REAL,
            lifetime_value REAL,
            churn_status BOOLEAN,
            load_timestamp DATETIME
        )
        """)

        # # Prepare and load data into temp table
        # staging_data = transformed_df.rename(columns={
        #     'CustomerID': 'customer_id',
        #     'ContractType': 'contract_type',
        #     'InternetService': 'internet_service',
        #     'TechSupport': 'tech_support',
        #     'Tenure': 'tenure_months',
        #     'MonthlyCharges': 'monthly_charges',
        #     'TotalCharges': 'total_charges',
        #     'LifetimeValue': 'lifetime_value',
        #     'Churn': 'churn_status',
        #     'load_timestamp_ist': 'load_timestamp'
        # })

        # Select columns in correct order matching temp table definition
        staging_data = transformed_df[[
            'customer_id', 'contract_type', 'internet_service', 'tech_support',
            'tenure_months', 'monthly_charges', 'total_charges', 'lifetime_value',
            'churn_status', 'load_timestamp'
        ]]

        # Load to temp table
        staging_data.to_sql('temp_fact_churn', conn, if_exists='replace', index=False)

        # Begin transaction
        cursor.execute("BEGIN TRANSACTION")


        cursor.execute("""
    UPDATE fact_churn
    SET
        monthly_charges = tf.monthly_charges,
        total_charges = tf.total_charges,
        lifetime_value = tf.lifetime_value,
        churn_status = tf.churn_status,
        load_timestamp = tf.load_timestamp
    FROM fact_churn fc
    LEFT JOIN dim_customer c ON fc.customer_key = c.customer_key
    LEFT JOIN dim_contract ct ON fc.contract_key = ct.contract_key
    LEFT JOIN dim_service s ON fc.service_key = s.service_key
    LEFT JOIN dim_tech_support ts ON fc.support_key = ts.support_key
    LEFT JOIN dim_tenure t ON fc.tenure_key = t.tenure_key
    INNER JOIN temp_fact_churn tf ON tf.customer_id = c.customer_id
                AND tf.contract_type = ct.contract_type
                AND tf.internet_service = s.internet_service
                AND tf.tech_support = ts.tech_support
                AND tf.tenure_months = t.tenure_months
    WHERE fc.monthly_charges <> tf.monthly_charges OR
          fc.total_charges <> tf.total_charges OR
          fc.lifetime_value <> tf.lifetime_value OR
          fc.churn_status <> tf.churn_status
""")
        updated_count = cursor.rowcount

        # # STEP 2: INSERT new records
        cursor.execute("""
        INSERT INTO fact_churn (
            customer_key, contract_key, service_key, support_key,
            tenure_key, monthly_charges, total_charges, lifetime_value,
            churn_status, load_timestamp
        )
        SELECT
            c.customer_key,
            ct.contract_key,
            s.service_key,
            ts.support_key,
            t.tenure_key,
            tf.monthly_charges,
            tf.total_charges,
            tf.lifetime_value,
            tf.churn_status,
            tf.load_timestamp
        FROM temp_fact_churn tf
        JOIN dim_customer c ON tf.customer_id = c.customer_id
        JOIN dim_contract ct ON tf.contract_type = ct.contract_type
        JOIN dim_service s ON tf.internet_service = s.internet_service
        JOIN dim_tech_support ts ON tf.tech_support = ts.tech_support
        JOIN dim_tenure t ON tf.tenure_months = t.tenure_months
        LEFT JOIN fact_churn fc ON
            fc.customer_key = c.customer_key AND
            fc.contract_key = ct.contract_key AND
            fc.service_key = s.service_key AND
            fc.support_key = ts.support_key AND
            fc.tenure_key = t.tenure_key
        WHERE fc.fact_key IS NULL  -- Only insert if record doesn't exist
        """)
        inserted_count = cursor.rowcount

        # Commit transaction
        conn.commit()
       #print(f"Incremental load completed: {updated_count} records updated")
        print(f"Incremental load completed: {updated_count} records updated, {inserted_count} records inserted")
        return updated_count, inserted_count


    except Exception as e:
        conn.rollback()
        print(f"Error during incremental load: {str(e)}")
    #     raise
    # finally:
    #      # Clean up
    #      cursor.execute("DROP TABLE IF EXISTS temp_fact_churn")

import pandas as pd
import sqlite3
from datetime import datetime
import pytz
import numpy as np

   # 1. Load data from GitHub
   #https://github.com/AkshayWankhade30084/Analytic_solution/blob/main/customer_churn_data_update_insert_check.csv
github_url = 'https://raw.githubusercontent.com/AkshayWankhade30084/Analytic_solution/refs/heads/main/customer_churn_data.csv'
df = pd.read_csv(github_url)

# 2. Create SQLite database connection
conn = sqlite3.connect('customer_churn.db')  # Creates persistent database file

# 3. Create staging table
try:
    cursor = conn.cursor()

    # SQLite doesn't support schemas, so we'll use a prefix for staging tables
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS stg_customer_churn_data (
        CustomerID INTEGER,
        Age INTEGER,
        Gender TEXT,
        Tenure INTEGER,
        MonthlyCharges REAL,
        ContractType TEXT,
        InternetService TEXT,
        TotalCharges REAL,
        TechSupport TEXT,
        Churn TEXT,
        load_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
    )
    """)

    # Load data into staging table
    df.to_sql('stg_customer_churn_data', conn, if_exists='replace', index=False)

    # Verify data loaded correctly
    print("Data successfully loaded into stg_customers (staging table)")
    staging_data = pd.read_sql("SELECT * FROM stg_customer_churn_data LIMIT 5", conn)
    #print("\nSample data from staging table:")
    #display(staging_data)

    # Read raw data
    raw_df = pd.read_sql("SELECT * FROM stg_customer_churn_data", conn)
    data_clean=data_transform(raw_df)
    # Create dimension tables if they don't exist
    create_dimension_tables()



    # # Load dimension data (only if empty)
    # dim_counts = pd.read_sql("""
    #     SELECT 'dim_customer' as table, COUNT(*) as count FROM dim_customer
    #     UNION ALL SELECT 'dim_contract', COUNT(*) FROM dim_contract
    #     UNION ALL SELECT 'dim_service', COUNT(*) FROM dim_service
    #     UNION ALL SELECT 'dim_tech_support', COUNT(*) FROM dim_tech_support
    #     UNION ALL SELECT 'dim_tenure', COUNT(*) FROM dim_tenure
    # """, conn)


    # replace


    data_clean = data_clean.rename(columns={
            'CustomerID': 'customer_id',
            'Age':'age',
            'Gender':'gender',
            'ContractType': 'contract_type',
            'InternetService': 'internet_service',
            'TechSupport': 'tech_support',
            'Tenure': 'tenure_months',
            'MonthlyCharges': 'monthly_charges',
             'TotalCharges': 'total_charges',
             'LifetimeValue': 'lifetime_value',
             'Churn': 'churn_status',
             'load_timestamp_ist': 'load_timestamp'
        })

    # if dim_counts['count'].sum() == 0:
    load_dimension_data(data_clean)
    print("Data successfully loaded into diemension tables")
  #  drop_table_diemension()
    # Create fact table if it doesn't exist
    create_fact_table()

  #  incremental_load_fact_data(data_clean)
    # Incremental load of fact data
    incremental_load_fact_data(data_clean)
    print("Data successfully loaded into fact tables")

except Exception as e:
    print(f"Error loading data: {str(e)}")

# # Export staging table data to CSV
# staging_df = pd.read_sql("SELECT * FROM stg_customer_churn_data", conn)
# staging_df.to_csv('stg_customer_churn_data.csv', index=False)
# print("Exported stg_customer_churn_data.csv")

# # Export dimension tables data to CSV
# dim_customer_df = pd.read_sql("SELECT * FROM dim_customer", conn)
# dim_customer_df.to_csv('dim_customer.csv', index=False)
# print("Exported dim_customer.csv")

# dim_contract_df = pd.read_sql("SELECT * FROM dim_contract", conn)
# dim_contract_df.to_csv('dim_contract.csv', index=False)
# print("Exported dim_contract.csv")

# dim_service_df = pd.read_sql("SELECT * FROM dim_service", conn)
# dim_service_df.to_csv('dim_service.csv', index=False)
# print("Exported dim_service.csv")

# dim_tech_support_df = pd.read_sql("SELECT * FROM dim_tech_support", conn)
# dim_tech_support_df.to_csv('dim_tech_support.csv', index=False)
# print("Exported dim_tech_support.csv")

# dim_tenure_df = pd.read_sql("SELECT * FROM dim_tenure", conn)
# dim_tenure_df.to_csv('dim_tenure.csv', index=False)
# print("Exported dim_tenure.csv")

# # Export fact table data to CSV
# fact_churn_df = pd.read_sql("SELECT * FROM fact_churn", conn)
# fact_churn_df.to_csv('fact_churn.csv', index=False)
# print("Exported fact_churn.csv")

# # Close the database connection
# conn.close()
# print("Database connection closed.")

