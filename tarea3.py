from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# 1. Configurar la sesión de Spark
# Forzamos la IP para que la Spark UI sea accesible desde fuera
spark = SparkSession.builder \
    .appName("Tarea3_Final_Beneficiarios") \
    .config("spark.driver.host", "192.168.1.50") \
    .getOrCreate()

# 2. Ruta en HDFS (Usando el host local de Hadoop)
path = "hdfs://localhost:9000/Tarea3/xfif-myr2.csv"

try:
    # 3. Leer el archivo CSV
    df = spark.read.csv(path, header=True, inferSchema=True)

    print("\n" + "="*30)
    print("--- ESQUEMA DEL ARCHIVO ---")
    print("="*30)
    df.printSchema()

    # 4. Categorización (Rango de beneficiarios)
    print("\n--- CATEGORIZACIÓN ---")
    df_rango = df.withColumn("RANGO",
        F.when(F.col("cantidaddebeneficiarios") <= 1, "INDIVIDUAL")
        .when((F.col("cantidaddebeneficiarios") > 1) & (F.col("cantidaddebeneficiarios") <= 5), "PEQUEÑO")
        .otherwise("GRANDE")
    )
    df_rango.groupBy("RANGO").count().show()

    # 5. Filtro de beneficiarios
    print("\n--- REGISTROS ENTRE 1 Y 10 BENEFICIARIOS ---")
    df.filter((F.col("cantidaddebeneficiarios") >= 1) & (F.col("cantidaddebeneficiarios") <= 10)) \
        .select("cantidaddebeneficiarios", "fechainscripcionbeneficiario", "nombredepartamentoatencion") \
        .show(10)
 # 6. Incremento del 10% (Asignamos a df_nuevo para que exista la variable)
    print("\n--- SIMULACIÓN DE INCREMENTO DEL 10% ---")
    df_nuevo = df.withColumn("CANTIDAD_INCREMENTADA", F.col("cantidaddebeneficiarios") * 1.10)
    df_nuevo.select("cantidaddebeneficiarios", "CANTIDAD_INCREMENTADA").show(5)

    # 7. Top 5 valores más altos
    print("\n--- TOP 5 REGISTROS CON MÁS BENEFICIARIOS ---")
    df.orderBy(F.col("cantidaddebeneficiarios").desc()).limit(5).show()

    # 8. Guardar el resultado en HDFS (mode overwrite para que no falle si ya existe)
    print("\nGuardando resultados en HDFS...")
    df_nuevo.write.mode("overwrite").csv("/Tarea3/resultado_tarea3", header=True)
    print("¡Archivo guardado exitosamente en: /Tarea3/resultado_tarea3!")

except Exception as e:
    print(f"\n[ERROR]: No se pudo procesar el archivo.")
    print(f"Detalle: {e}")

# 9. PAUSA CRÍTICA: Esto mantiene viva la Spark UI (puerto 4040)
print("\n" + "="*50)
print("LA SESIÓN ESTÁ ACTIVA.")
print("Ahora puedes entrar a http://192.168.1.50:4040 en tu navegador.")
print("="*50)
input("\nPresiona ENTER cuando hayas terminado de ver la Spark UI para cerrar el programa...")

# Cerrar sesión
spark.stop()
