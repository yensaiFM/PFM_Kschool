import tmdbsimple as tmdb
from datetime import datetime
from datetime import timedelta
import os
import re

tmdb.API_KEY = 'XXXX'
dir = os.path.dirname(__file__)

# Save list films with format hastag by filter later in twitter call
def createHastags(twitter_hashtags):
    d = datetime.today()
    filenow = "%i-%02d-%02d.txt" % (d.year, d.month, d.day)
    basePath = dir + "/data_twitter/filter"
    os.system("mkdir -p %s" % (basePath))
    fh = open(basePath + "/" + filenow, "a")
    for hashtag in twitter_hashtags:
        fh.write(hashtag)
        fh.write("\n")
    fh.close()

# Save info film that now are in cinema
def createMoviesDB(films):
    d = datetime.today()
    filenow = "%i-%02d-%02d.json" % (d.year, d.month, d.day)
    basePath = dir + "/data_movieDB"
    os.system("mkdir -p %s" % (basePath))
    fh = open(basePath + "/" + filenow, "a")
    for film in films:
        fh.write(str(film))
        fh.write("\n")
    fh.close()

# Obtain films that are now in cinema
if __name__ == '__main__':
    movies = tmdb.Movies()
    page = 1
    request = 'language=es-ES&region=ES&sort_by=release_date.desc&page='
    response = movies.upcoming(query=request + str(page))
    if response.has_key('total_results'):
        total_results = response['total_results']
        total_pages = response['total_pages']
        page = response['page']
        # print "total results:" + str(total_results) + "total_pages:" + str(total_pages) + " page:" + str(page)
        if (total_results > 0):
            now = datetime.today()
            date_min_film = now - timedelta(days=60)
            films = []
            twitter_hashtags = []
            while total_pages >= page:
                for film in response['results']:
                    # Guardamos las pelis de los dos ultimos meses para obtener los tweets
                    datetime_film = datetime.strptime(film['release_date'], '%Y-%m-%d')
                    if(datetime_film > date_min_film):
                        cleanTitle = re.sub('\W+', ' ', film['title'])
                        hastag = '#'+''.join(s[:1].upper() + s[1:] for s in cleanTitle.split(' '))
                        if hastag not in twitter_hashtags:
                            twitter_hashtags.append(hastag)
                            films.append(film)
                        print("page:"+ str(page) + "title:" + film['title'])
                response = movies.upcoming(query=request + str(page))
                page += 1
            #print ("total pages:" + str(total_pages))
            #print ("pelis:"+str(twitter_hastags))
            #print("dia:" + str(datetime_film.day))
            #print (now - timedelta(days=60))
            #print(film['title'], film['id'], film['release_date'], film['popularity'])
            createHastags(twitter_hashtags)
            createMoviesDB(films)