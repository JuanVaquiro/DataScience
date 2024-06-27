from pyspark.sql import SparkSession

# Inicializa SparkSession
spark = SparkSession.builder \
    .appName("Ejemplo de lectura de CSV") \
    .getOrCreate()

# Carga el archivo CSV
df = spark.read.load('AbandonoEmpleados.csv', format='csv', header=True)

# Muestra las primeras 10 filas
df.show(10)

# Detén la sesión de Spark al finalizar el script
spark.stop()
