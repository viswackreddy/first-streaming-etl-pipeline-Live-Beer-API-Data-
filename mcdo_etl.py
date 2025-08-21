from airflow.decorators import dag, task
from datetime import datetime
import http.client
import json
from pyspark.sql import SparkSession

@dag(
    schedule=None,
    start_date=datetime(2025, 8, 20),
    catchup=False,
    tags=["api", "etl", "mcdonalds", "pyspark", "s3"]
)
def mcdonalds_menu_to_s3():

    @task()
    def extract():
        conn = http.client.HTTPSConnection("mcdonald-s-products-api.p.rapidapi.com")
        headers = {
            'x-rapidapi-key': "0404f55699msh953b4c68e71b6b6p165e7bjsn6c0e212854a4",
            'x-rapidapi-host': "mcdonald-s-products-api.p.rapidapi.com"
        }
        conn.request("GET", "/us/currentMenu", headers=headers)
        res = conn.getresponse()
        data = res.read()
        return data.decode("utf-8")

    @task()
    def transform(raw_data):
        return json.loads(raw_data)

    @task()
    def save_to_s3_with_pyspark(data):
        # Initialize Spark with Hadoop AWS support
        spark = (
            SparkSession.builder
            .appName("McDonaldsMenuETL")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.aws.credentials.provider", 
                    "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
            .getOrCreate()
        )

        # Extract menu items
        items = data if isinstance(data, list) else data.get("menu", [])
        if not items:
            raise ValueError("No data to write")

        # Convert to DataFrame
        df = spark.createDataFrame(items)

        # Example transform (keep only name, price if exist)
        if "name" in df.columns and "price" in df.columns:
            df = df.select("name", "price")

        # Define S3 output path (change bucket name)
        bucket = "beer.data"
        s3_output_path = f"s3a://{bucket}/mcdonalds/menu/{datetime.now().strftime('%Y%m%d_%H%M%S')}"

        # Write to S3 as CSV and Parquet
        df.write.mode("overwrite").option("header", True).csv(s3_output_path + "/csv")
        df.write.mode("overwrite").parquet(s3_output_path + "/parquet")

        print(f"Data saved to S3 at {s3_output_path}")

    raw = extract()
    transformed = transform(raw)
    save_to_s3_with_pyspark(transformed)

mcdonalds_menu_to_s3()
