from pyspark.sql import SparkSession

# Crée une session Spark
spark = SparkSession.builder.getOrCreate()

# Crée un RDD (Resilient Distributed Dataset)
rdd = spark.sparkContext.parallelize([1, 2, 3, 4, 5])

# Applique une transformation simple: compter les éléments
count = rdd.count()

# Affiche le résultat
print(f"Le nombre d'éléments dans l'RDD est : {count}")

# Arrêter la session Spark
spark.stop()
