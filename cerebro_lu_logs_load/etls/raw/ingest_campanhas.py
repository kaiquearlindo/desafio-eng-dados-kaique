from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

def main():
    spark = SparkSession.builder.appName("IngestCampanhas").getOrCreate()

    # Origem e Destino
    input_path = "gs://stg-lake-raw-data_governance/campanhas.json"
    dest_table = "maga-bigdata.temp_bq.campanhas"

    # Leitura (Campanhas geralmente é um array JSON, então usamos multiLine)
    df = spark.read.option("multiLine", "true").json(input_path)

    # Tratamento Pleno: Garantir que attributes seja STRING para não quebrar o schema do BQ
    if "attributes" in df.columns:
        df = df.withColumn("attributes", F.col("attributes").cast(StringType()))
    
    if "publish_time" in df.columns:
        df = df.withColumn("publish_time", F.to_timestamp("publish_time"))

    # Escrita no BQ
    # No ingest_campanhas.py e ingest_conversas.py
    df.write.format("bigquery") \
        .option("table", dest_table) \
        .option("temporaryGcsBucket", "stg-lake-raw-data_governance") \
        .mode("overwrite") \
        .save()

if __name__ == "__main__":
    main()