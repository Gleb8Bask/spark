from pyspark.sql import SparkSession
from src.conf import SRC_FILE, TARGET_TABLE

spark = SparkSession.builder\
    .config("spark.driver.memory", "2G") \
    .config("spark.jars", "jars/postgresql-42.3.1.jar") \
    .getOrCreate()

#посчитать ежедневные агрегаты в parquet файлы:
#количество просмотров по категориям (date, category, views_count)
#продажи товаров брендов (date, brand, purchase_count)
#*писать агрегаты по дням в базу postgresql из внешнего docker-compose, которая доступна изнутри контейнеров по адресу 172.17.0.1:5432

df = spark.read.parquet(SRC_FILE).na.drop('any')

count_views = df.groupBy(col("date"), col("category")).sum(views_count)


df.show()
df.printSchema()

url = "jdbc:postgresql://postgresql:5432/postgres"

(
df
    .write
    .option("driver", "org.postgresql.Driver")
    .format("jdbc")
    .mode("append")
    .option("url", url)
    .option("user", "p_user")
    .option("password", "password123")
    .option("dbtable", TARGET_TABLE)
    .option("fetchsize", 10000)
    .save(TARGET_TABLE)
)