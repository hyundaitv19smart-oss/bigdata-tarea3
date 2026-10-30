
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, from_unixtime
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    DoubleType,
    LongType
)

# Crear la sesión de Spark
spark = SparkSession.builder \
    .appName("PruebaStreamingSensores") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Definir la estructura de los mensajes recibidos
esquema = StructType([
    StructField("sensor_id", IntegerType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("humidity", DoubleType(), True),
    StructField("timestamp", LongType(), True)
])

# Leer los mensajes del tema sensor_data de Kafka
df_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "sensor_data") \
    .option("startingOffsets", "latest") \
    .load()

# Convertir los mensajes JSON en columnas
datos = df_kafka.select(
    from_json(
        col("value").cast("string"),
        esquema
    ).alias("datos")
).select("datos.*")

# Convertir la marca de tiempo
datos = datos.withColumn(
    "fecha_hora",
    from_unixtime(col("timestamp")).cast("timestamp")
)

# Mostrar los registros recibidos en la consola
consulta = datos.writeStream \
    .format("console") \
    .outputMode("append") \
    .option("truncate", "false") \
    .option(
        "checkpointLocation",
        "/tmp/spark_prueba_streaming_checkpoint"
    ) \
    .start()

print("Spark Streaming está esperando datos del tema sensor_data.")

# Mantener la aplicación en ejecución
consulta.awaitTermination()

