from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def main():
    spark = SparkSession.builder.appName("IngestConversas").getOrCreate()

    # Origem e Destino
    input_path = "gs://stg-lake-raw-data_governance/conversas.json"
    dest_table = "maga-bigdata.temp_bq.conversas"

    # Leitura
    df = spark.read.option("multiLine", "true").json(input_path)

    # Tratamento: Conversas costuma ter o campo 'text' que pode conter caracteres especiais
    if "publish_time" in df.columns:
        df = df.withColumn("publish_time", F.to_timestamp("publish_time"))

    # Escrita no BQ
    df.write.format("bigquery") \
        .option("table", dest_table) \
        .option("temporaryGcsBucket", "stg-lake-raw-data_governance") \
        .mode("overwrite") \
        .save()
    
if __name__ == "__main__":
    main()