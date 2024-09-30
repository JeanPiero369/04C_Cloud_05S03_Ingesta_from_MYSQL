import mysql.connector
import boto3
import pandas as pd

# Configuración de la base de datos
db_config = {
    'host': '18.206.168.84',
    'port': 8000,
    'user': 'root',         # Cambia esto con tu usuario de la base de datos
    'password': 'utec',   # Cambia esto con tu contraseña de la base de datos
    'database': 'universidad'
}

# Conexión a la base de datos
connection = mysql.connector.connect(**db_config)
cursor = connection.cursor()

# Consulta de los datos
query = "SELECT * FROM alumnos;"  # Cambia 'tu_tabla' por el nombre de la tabla que deseas consultar
cursor.execute(query)

# Carga de datos en un DataFrame de pandas
data = cursor.fetchall()
columns = [desc[0] for desc in cursor.description]
df = pd.DataFrame(data, columns=columns)

# Guardar el DataFrame en un archivo CSV
ficheroUpload = "data.csv"
df.to_csv(ficheroUpload, index=False)

# Subir a S3
nombreBucket = "04c-cloud-05s03-ingesta-from-database"
s3 = boto3.client('s3')
response = s3.upload_file(ficheroUpload, nombreBucket, ficheroUpload)

print("Ingesta completada")

# Cierre de la conexión
cursor.close()
connection.close()
