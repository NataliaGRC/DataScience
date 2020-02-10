
import tweepy
from tweepy import OAuthHandler

from config.parametros import Parametros


def autenticacion_twitter():

    auth = OAuthHandler(Parametros.CONSUMER_KEY, Parametros.CONSUMER_SECRET)
    auth.set_access_token(Parametros.ACCESS_TOKEN, Parametros.ACCESS_SECRET)

    return tweepy.API(auth)