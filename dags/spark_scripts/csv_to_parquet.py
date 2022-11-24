from pyspark.sql import SparkSession

def csv_to_parquet(source_csv_file: str, target_parquet_dir: str) -> None:

    spark = (SparkSession.builder
        .appName('spark_test_csv_to_parquet')
       .getOrCreate()
    )

# Extract
    df = (spark.read
        .format('csv')
        .option('sep', ';')
        .option('header', 'true')
        .option('inferSchema', 'true')
        .load(source_csv_file)
    )

    df.

# Load
    (df
        .repartition(1)
        .write
        .mode('append')
        .format('parquet')
        .save(target_parquet_dir)
    )

    spark.stop()


if __name__ == '__main__':
    fire.Fire(csv_to_parquet)
