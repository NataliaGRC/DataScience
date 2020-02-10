import os
import json

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

from config.parametros import Parametros


def main():
    # Crear Contexto de Spark
    sc = SparkContext("local[*]", Parametros.APP)

    # Crear el Contexto Streaming (Intervalo de 10s)
    ssc = StreamingContext(sc, 10)

    # Crear un Kafka Stream para consumir la informacion de twitter

    # localhost:2181 = Direccion default de Zookeeper
    kafka_stream = KafkaUtils.createStream(ssc, Parametros.ZOOKEEPER, Parametros.APP, {Parametros.TOPIC: 1})

    # Recibir un coleccion de RDDs con mensajes de twitter segun el intervalo.
    objecto_twitter = kafka_stream.map(lambda x: json.loads(x[1])["text"])

    # Filtrar el texto con el fin de encontrar hashtags
    hashtags = objecto_twitter.flatMap(lambda text: text.split(" ")).filter(lambda text: text.startswith("#"))

    # Conteo general de hashtags de twitter
    metricas = hashtags.map(lambda hashtag: (hashtag, 1)).reduceByKey(lambda a, b: a + b)

    # Mostrar resultados de las metricas
    metricas.pprint()

    # Guardar los archivos que se reciben ya sumarizados
    metricas.saveAsTextFiles("file:///data/", "parquet")

    # Iniciar la computación de streaming
    ssc.start()

    # Esperar que la transmisión termine
    ssc.awaitTermination()


if __name__ == '__main__':
    os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.4.4 pyspark-shell'

    main()
