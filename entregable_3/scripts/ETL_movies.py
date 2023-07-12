# Este script está pensado para correr en Spark y hacer el proceso de ETL de la tabla users

import requests
from datetime import datetime, timedelta
from os import environ as env

from pyspark.sql.functions import concat, when, lit, col, expr, to_date, current_date, date_format, year, min, max

from commons import ETL_Spark

class ETL_Movies(ETL_Spark):
    def __init__(self, job_name=None):
        super().__init__(job_name)
        self.process_date = datetime.now().strftime("%Y-%m-%d")

    def run(self):
        process_date = "2023-07-09"  # datetime.now().strftime("%Y-%m-%d")
        self.execute(process_date)

    def extract(self):
        """
        Extrae datos de la API
        """
        print(">>> [E] Extrayendo datos de la API...")

        api_key_value = env['API_KEY']
        endpoint = 'https://imdb-api.com/en/API/Top250Movies/' + api_key_value

        response = requests.get(endpoint)

        if response.status_code == 200:
            data = response.json()["items"]
            print(data)
        else:
            print("Error al extraer datos de la API")
            data = []
            raise Exception("Error al extraer datos de la API")

        df = self.spark.read.json(
            self.spark.sparkContext.parallelize(data), multiLine=True
        )
        df.printSchema()
        df.show()

        return df

    def transform(self, df_original):
        """
        Transforma los datos
        """
        print(">>> [T] Transformando datos...")

        df = df_original.dropDuplicates()
        df = df.withColumn('imDbRatingCount', col('imDbRatingCount') / 1000)
        df = df.withColumn('age_of_movie', year(current_date()) - col('year'))

        min_value = df.select(min(col('imDbRating')).cast('double')).collect()[0][0]
        max_value = df.select(max(col('imDbRating')).cast('double')).collect()[0][0]

        df = df.withColumn('rating_scaled', (col('imDbRating') - min_value / (max_value - min_value)))

        df = df.withColumn('rating_category', when(col('imDbRating') <= 5, 'bajo')
                                        .when(col('imDbRating') <= 8, 'medio')
                                        .otherwise('alto'))

        df.printSchema()
        df.show()

        return df

    def load(self, df_final):
        """
        Carga los datos transformados en Redshift
        """
        print(">>> [L] Cargando datos en Redshift...")

        # add process_date column
        df_final = df_final.withColumn("process_date", lit(self.process_date))

        df_final.write \
            .format("jdbc") \
            .option("url", env['REDSHIFT_URL']) \
            .option("dbtable", f"{env['REDSHIFT_SCHEMA']}.top_movies_imdb") \
            .option("user", env['REDSHIFT_USER']) \
            .option("password", env['REDSHIFT_PASSWORD']) \
            .option("driver", "org.postgresql.Driver") \
            .mode("append") \
            .save()

        print(">>> [L] Datos cargados exitosamente")


if __name__ == "__main__":
    print("Corriendo script")
    etl = ETL_Movies()
    etl.run()
