from kafka import SimpleProducer
from kafka.client import SimpleClient
from tweepy import Stream
from tweepy.streaming import StreamListener

from utils.autenticacion_twitter import autenticacion_twitter
from config.parametros import Parametros


class KafkaProductor(StreamListener):

    def __init__(self):
        # Crear el productor con la direccion de Zookeeper
        self.producer = SimpleProducer(SimpleClient(Parametros.KAFKA))

    def on_data(self, data):
        # Productor envia el mensaje hacia el canal del topic para ser consumido
        self.producer.send_messages(Parametros.TOPIC, bytes(data, "ascii"))

    def on_error(self, status):
        print(status)


def main():
    # Configuracion de Streaming de Twitter
    twitter_stream = Stream(autenticacion_twitter().auth, KafkaProductor())

    # Produce la informacion con el hashtag de Game of Thrones (Tweets)
    twitter_stream.filter(track=['#music'])


if __name__ == '__main__':
    main()
