from kafka import KafkaConsumer
import json
import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
import snowflake.connector

# Load environment variables from the .env file
load_dotenv()

# Kafka configuration
bootstrap_servers = 'localhost:9092'
topics = ['videoplayback', 'searchevents', 'likedislikeevents', 'navigationevents']

# # Snowflake configuration
# snowflake_config = {
#     "user": os.getenv("SNOWFLAKE_USER"),
#     "password": os.getenv("SNOWFLAKE_PASSWORD"),
#     "account": os.getenv("SNOWFLAKE_ACCOUNT"),
#     "database": os.getenv("SNOWFLAKE_DATABASE"),
#     "schema": os.getenv("SNOWFLAKE_SCHEMA"),
#     "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
# }

# Initialize Snowflake connection
def get_snowflake_connection():
    conn = snowflake.connector.connect(
    user=os.getenv("user"),
    password=os.getenv("password"),
    account=os.getenv("account"),
    warehouse=os.getenv("warehouse"),
    database=os.getenv("database"),
    schema=os.getenv("schema")
    )
    return conn

# Initialize Spark session with the Docker Spark master URL
spark = SparkSession.builder \
    .appName("KafkaToSnowflake") \
    .master("spark://localhost:7077") \
    .config("spark.executor.memory", "2g") \
    .config("spark.executor.cores", "1") \
    .getOrCreate()

# Kafka Consumer
consumer = KafkaConsumer(
    *topics,
    bootstrap_servers=bootstrap_servers,
    group_id='clickstream-consumer-group',
    auto_offset_reset='earliest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# Function to flatten JSON
def flatten_json(json_obj, prefix=''):
    flat_data = {}
    for key, value in json_obj.items():
        if isinstance(value, dict):
            flat_data.update(flatten_json(value, f"{prefix}{key}_"))
        else:
            flat_data[f"{prefix}{key}"] = value
    return flat_data

# Function to infer schema using Spark
def infer_schema(data):
    try:
        # Create Spark DataFrame
        df = spark.createDataFrame(data)
        # Print schema
        print("Inferred Schema:")
        df.printSchema()
        return df
    except Exception as e:
        print(f"Error inferring schema: {e}")

# Function to check if table exists in Snowflake
def check_table_exists(connection, table_name):
    cursor = connection.cursor()
    cursor.execute(f"SHOW TABLES LIKE '{table_name}';")
    result = cursor.fetchall()
    cursor.close()
    return len(result) > 0

# Function to create a Snowflake table
def create_table(connection, table_name, schema_df):
    try:
        create_query = f"CREATE TABLE {table_name} ("
        for field in schema_df.schema.fields:
            col_type = "STRING"  # Map all fields to STRING for simplicity; refine as needed
            create_query += f"{field.name} {col_type}, "
        create_query = create_query.rstrip(", ") + ");"
        connection.cursor().execute(create_query)
        print(f"Created table: {table_name}")
    except Exception as e:
        print(f"Error creating table: {e}")

# # Function to insert data into Snowflake
# def insert_into_snowflake(connection, table_name, schema_df):
#     try:
#         df = schema_df.toPandas()
#         success, _, _ = write_pandas(connection, df, table_name.upper())
#         if success:
#             print(f"Inserted data into {table_name}")
#     except Exception as e:
#         print(f"Error inserting data: {e}")

def insert_into_snowflake(connection, table_name, schema_df):
    try:
        # Convert the DataFrame to a list of tuples
        rows = [tuple(row) for row in schema_df.toPandas().itertuples(index=False)]
        # Generate column placeholders
        columns = ', '.join(schema_df.columns)
        placeholders = ', '.join(['%s'] * len(schema_df.columns))
        # Insert query
        insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
        # Execute insertion
        with connection.cursor() as cur:
            cur.executemany(insert_query, rows)
            print(f"Inserted {len(rows)} rows into {table_name}")
    except Exception as e:
        print(f"Error inserting data with cursor: {e}")

# Process messages from Kafka
try:
    print("Consumer is listening for messages...")
    snowflake_conn = get_snowflake_connection()
    for message in consumer:
        topic = message.topic
        data = message.value
        print(f"Consumed message from topic '{topic}': {data}")

        # Flatten JSON
        flattened_data = flatten_json(data)
        print(f"Flattened Data: {flattened_data}")

        # Infer schema
        schema_df = infer_schema([flattened_data])

        # Determine table name
        table_name = f"flattened_{topic}_data"

        # Check if table exists, create if not
        if not check_table_exists(snowflake_conn, table_name):
            print(f"Table {table_name} does not exist. Creating it.")
            create_table(snowflake_conn, table_name, schema_df)

        # Insert data into Snowflake
        insert_into_snowflake(snowflake_conn, table_name, schema_df)

except KeyboardInterrupt:
    print("Shutting down consumer...")
finally:
    consumer.close()
    snowflake_conn.close()
    spark.stop()
