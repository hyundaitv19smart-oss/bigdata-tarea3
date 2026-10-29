# Proyecto Big Data: Análisis de Calidad del Aire en Colombia

## 1. Descripción del proyecto

Este proyecto académico tiene como objetivo aplicar herramientas del ecosistema Big Data para el procesamiento y análisis de datos. Se utilizan Apache Spark para el procesamiento batch y Apache Kafka junto con Spark Streaming para demostrar el procesamiento de información en tiempo real.

## 2. Descripción del problema

La calidad del aire es un tema importante para el análisis ambiental, ya que permite estudiar las mediciones registradas en diferentes estaciones de monitoreo. La información puede contener variables relacionadas con mediciones ambientales, fechas, ubicación geográfica y características de las estaciones.

El propósito de este proyecto es explorar los registros disponibles, identificar la estructura de los datos y obtener estadísticas descriptivas mediante Apache Spark. Adicionalmente, se realiza una práctica independiente de procesamiento en tiempo real utilizando Kafka y Spark Streaming.

## 3. Conjunto de datos seleccionado

Para el procesamiento batch se seleccionó el conjunto de datos **Calidad del Aire en Colombia**, disponible en el portal de Datos Abiertos de Colombia.

**Fuente:** Datos Abiertos de Colombia.

**Enlace:** https://www.datos.gov.co/d/g4t8-zkc3

El archivo utilizado en la práctica se llama `calidad_aire.csv`. Contiene información de estaciones de monitoreo, variables medidas, fechas, ubicación y unidades de medición.

Para esta actividad se utiliza una muestra descargada del conjunto de datos, por lo que los resultados corresponden al archivo procesado y no necesariamente a la totalidad de los registros publicados en el portal.

## 4. Tecnologías utilizadas

- **Apache Spark:** procesamiento batch de los datos.
- **PySpark:** desarrollo del programa de análisis en Python.
- **Apache Kafka:** administración y transmisión de mensajes.
- **Spark Streaming:** recepción y procesamiento de datos en tiempo real.
- **Python:** desarrollo de los programas.
- **Ubuntu Linux:** entorno de ejecución.

## 5. Archivos del proyecto

- `analisis_batch.py`: carga el archivo CSV, muestra la estructura de los datos, presenta los primeros diez registros, cuenta las filas y columnas y genera estadísticas descriptivas.
- `calidad_aire.csv`: archivo de datos utilizado para el análisis batch.
- `kafka_producer.py`: genera datos simulados de sensores, como temperatura y humedad, y los envía al tema `sensor_data`.
- `spark_streaming_consumer.py`: recibe y procesa los mensajes enviados mediante Kafka utilizando Spark Streaming.

## 6. Instrucciones de ejecución del procesamiento batch

### Requisitos

Para ejecutar el programa es necesario tener Java y Apache Spark instalados y configurados correctamente.

También se requiere disponer del archivo `calidad_aire.csv` y del programa `analisis_batch.py` en la misma carpeta.

### Procedimiento

**Paso 1.** Descargar el conjunto de datos desde el portal de Datos Abiertos de Colombia y guardar el archivo como `calidad_aire.csv`.

**Paso 2.** Ubicar el archivo CSV y `analisis_batch.py` en la misma carpeta de trabajo.

**Paso 3.** Abrir una terminal de Ubuntu y acceder a la carpeta donde están guardados los archivos.

**Paso 4.** Ejecutar el programa mediante el siguiente comando:

```bash
spark-submit analisis_batch.py
```

**Paso 5.** Revisar los resultados mostrados en la terminal. El programa presenta la estructura de las columnas, los primeros diez registros, la cantidad de filas y columnas y las estadísticas descriptivas.

## 7. Instrucciones de ejecución de Kafka y Spark Streaming

Esta práctica utiliza datos simulados de sensores y es independiente del conjunto de datos de calidad del aire.

**Paso 1.** Iniciar ZooKeeper y Apache Kafka, de acuerdo con la configuración del entorno.

**Paso 2.** Verificar que el tema `sensor_data` esté disponible en Kafka.

**Paso 3.** Ejecutar el productor de datos:

```bash
python3 kafka_producer.py
```

**Paso 4.** En otra terminal, ejecutar el consumidor de Spark Streaming:

```bash
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8 spark_streaming_consumer.py
```

**Paso 5.** Comprobar que el productor envíe mensajes y que el consumidor muestre los registros recibidos.

La ejecución requiere que Kafka esté funcionando y que las dependencias necesarias estén disponibles.

## 8. Resultados esperados

Con el procesamiento batch se busca explorar los registros del conjunto de datos de calidad del aire y conocer su estructura, cantidad y características estadísticas.

Con Kafka y Spark Streaming se busca demostrar el envío y la recepción continua de datos simulados de sensores.

## 9. Video explicativo

En este apartado se incluirá el enlace al video de máximo cinco minutos, en el que se presenta el código batch, los resultados obtenidos y la práctica de procesamiento en tiempo real.

**Enlace del video:** Pendiente de agregar.

## 10. Autor

Cristian Duvan Chaparro Manrique  
Estudiante de Ingeniería de Sistemas.
