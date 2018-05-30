import tmdbsimple as tmdb
import os
import re
import time
import json
import requests

tmdb.URL = 'https://api.themoviedb.org/3/movie/'
tmdb.API_KEY = 'XXXX'
dir = os.path.dirname(__file__)
outputDir = dir + "/data_movieDB"

# Obtain info about films and cast / crew from a list of movieid and save in format json
def createMoviesDataset(film):
    filename = "movies_metadata.json"
    basePath = dir + "/data_movieDB"
    os.system("mkdir -p %s" % (basePath))
    if(film):
        fh = open(basePath + "/" + filename, "a")
        fh.write(str(film))
        fh.write("\n")
        fh.close()

if __name__ == '__main__':
    fhMovies = open(outputDir + "/filter/list_movies.csv" , "r")
    for id_film in fhMovies:
        url_params = "api_key=" + tmdb.API_KEY + "&language=es-ES"
        # Load info movie
        r = requests.get(url=tmdb.URL + str(id_film), params=url_params)
        film_info = r.text.encode('utf-8')
        # Load info cast and crew
        r2 = requests.get(url=tmdb.URL + str(id_film) + "/credits", params=url_params)
        cast_info = r2.text.encode('utf-8')
        dictA = json.loads(film_info)
        dictB = json.loads(cast_info)
        merge_dict = {key: value for (key, value) in (dictA.items() + dictB.items())}
        jsonString_merged = json.dumps(merge_dict)
        # print "json merge: " + str(jsonString_merged)
        createMoviesDataset(jsonString_merged)
        time.sleep(10) #Se envia una peticion cada 10 sg
