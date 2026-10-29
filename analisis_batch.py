
from pathlib import Path
from pyspark.sql import SparkSession


def main():
    spark = SparkSession.builder \
        .appName("AnalisisCalidadAire") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    try:
        ruta_csv = Path(__file__).resolve().parent / "calidad_aire.csv"

        if not ruta_csv.exists():
            print("ERROR: No se encuentra calidad_aire.csv.")
            print("Guarde el archivo CSV junto a analisis_batch.py.")
            return

        df = spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .option("ignoreLeadingWhiteSpace", "true") \
            .option("ignoreTrailingWhiteSpace", "true") \
            .option("mode", "PERMISSIVE") \
            .csv(str(ruta_csv))

        print("\n===== ESTRUCTURA DE LOS DATOS =====")
        df.printSchema()

        print("\n===== PRIMEROS 10 REGISTROS =====")
        df.show(10, truncate=False)

        print("\n===== CANTIDAD DE REGISTROS =====")
        print("Total de filas:", df.count())
        print("Total de columnas:", len(df.columns))

        print("\n===== RESUMEN ESTADISTICO =====")
        df.describe().show(truncate=False)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
