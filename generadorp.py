import os
import json
import bz2
import networkx as nx
import time
import sys
import argparse
from datetime import date
from mpi4py import MPI


comm = MPI.COMM_WORLD
size = comm.Get_size()
rank = comm.Get_rank()

def Parametros():
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("-d", "--d", default="data", type=str, help="Directorio de los datos")
    parser.add_argument("-fi", "--fi", type=str, help="Fecha inicial")
    parser.add_argument("-ff", "--ff", type=str, help="Fecha final")
    parser.add_argument("-h", "--h", type=str, help="Directorio de hashtags")
    parser.add_argument("-grt", "--grt", action='store_true', help="Generar grafo de retweets")   
    parser.add_argument("-jrt", "--jrt", action='store_true', help="Generar json de retweets")
    parser.add_argument("-gm", "--gm", action='store_true', help="Generar grafo de menciones")
    parser.add_argument("-jm", "--jm", action='store_true', help="Generar json de menciones")
    parser.add_argument("-gcrt", "--gcrt", action='store_true', help="Generar grafo de coretweets")
    parser.add_argument("-jcrt", "--jcrt", action='store_true', help="Generar json de coretweets")
    
    return parser.parse_args()     

def split_data(data, size):
    length = len(data)
    chunk_size = length // size
    remainder = length % size

    data_split = []
    start = 0
    for i in range(size):
        end = start + chunk_size + (1 if i < remainder else 0)
        data_split.append(data[start:end])
        start = end

    return data_split

def TransformarFechas(fi, ff):
    if(fi is not None):
        partes_fi = fi.split("-")
        
        if len(partes_fi) >= 3:
            dia = int(partes_fi[0])
            mes = int(partes_fi[1])
            anio = int(partes_fi[2])
            
            if anio < 100:
                anio += 2000
            
            fi = date(anio, mes, dia)
    
    if(ff is not None):
        partes_ff = ff.split("-")
        
        if len(partes_ff) >= 3:
            dia = int(partes_ff[0])
            mes = int(partes_ff[1])
            anio = int(partes_ff[2])
            
            if anio < 100:
                anio += 2000
            
            ff = date(anio, mes, dia)
    
    if(fi is not None and ff is not None and fi > ff):
        print("Fecha inicial mayor que fecha final")
        sys.exit()
    return fi, ff


def leerHashtags(path):
    h_in = open(path, "r")
    lineas = h_in.readlines()
    h_in.close()
    hashtags = set()  
    for linea in lineas:
        hashtag = linea.strip()
        if hashtag.startswith("#"):
            hashtag = hashtag[1:]
        hashtag = hashtag.lower()  # Convertir a minúsculas
        hashtags.add(hashtag)  
    return list(hashtags) 

def recorrer(path):
    tweets = []
    for carpeta, _, archivos in os.walk(path):
        for archivo in archivos:
            if not archivo.startswith('.DS_Store'):
                ruta_tweet = os.path.join(carpeta, archivo)
                tweets.append(ruta_tweet)
    
    data_split = split_data(tweets, size)
    tweets_subconjunto = comm.scatter(data_split, root=0)
    return tweets_subconjunto



def descomprimirHashtags(data, fi, ff, hashtags):
    tweets = []
    for tweet in data: 
        f_in = open(tweet, "rb")
        f_out = bz2.decompress(f_in.read()).decode('utf-8')
        f_in.close()
        lineas = f_out.strip().split('\n')
        if(fi is not None or ff is not None):
                tweet1 = json.loads(lineas[0])
                fecha = tweet1.get("timestamp_ms")
                fecha = int(fecha)
                fecha = date.fromtimestamp(fecha/1000)
                if(fi is not None and ff is not None):
                    if(fecha >= fi and fecha <= ff): 
                        for linea in lineas:
                            tweet_json = json.loads(linea)
                            list_hashtags_json = []
                            if(tweet_json.get("entities") is not None):
                                hashtags_json = tweet_json.get("entities").get("hashtags")
                                for hashtag_json in hashtags_json:
                                    hashtag_json = hashtag_json.get("text")
                                    list_hashtags_json.append(hashtag_json.lower())
                            if any(hashtag in list_hashtags_json for hashtag in hashtags):
                                tweets.append(tweet_json)
                elif(fi is not None and fecha <= fi or ff is not None and fecha >= ff):
                    for linea in lineas:
                        tweet_json = json.loads(linea)
                        list_hashtags_json = []
                        if(tweet_json.get("entities") is not None):
                            hashtags_json = tweet_json.get("entities").get("hashtags")
                            for hashtag_json in hashtags_json:
                                hashtag_json = hashtag_json.get("text")
                                list_hashtags_json.append(hashtag_json.lower())
                        if any(hashtag in list_hashtags_json for hashtag in hashtags):
                            tweets.append(tweet_json)
        else:
            for linea in lineas:
                tweet_json = json.loads(linea)
                list_hashtags_json = []
                if(tweet_json.get("entities") is not None):
                    hashtags_json = tweet_json.get("entities").get("hashtags")
                    for hashtag_json in hashtags_json:
                        hashtag_json = hashtag_json.get("text")
                        list_hashtags_json.append(hashtag_json.lower())
                if any(hashtag in list_hashtags_json for hashtag in hashtags):
                    tweets.append(tweet_json)
    all_tweets = comm.gather(tweets, root=0)
    if rank == 0:
        tweets_combinados = [tweet for sublist in all_tweets for tweet in sublist]
        return tweets_combinados
    else:
        return []


def descomprimir(data, fi, ff):
    tweets = []
    for tweet in data: 
        f_in = open(tweet, "rb")
        f_out = bz2.decompress(f_in.read()).decode('utf-8')
        f_in.close()
        lineas = f_out.strip().split('\n')
        if(fi is not None or ff is not None):
                tweet1 = json.loads(lineas[0])
                fecha = tweet1.get("timestamp_ms")
                fecha = int(fecha)
                fecha = date.fromtimestamp(fecha/1000)
                if(fi is not None and ff is not None):
                    if(fecha >= fi and fecha <= ff):
                        for linea in lineas:
                            tweet_json = json.loads(linea)               
                            tweets.append(tweet_json)
                elif(fi is not None and fecha <= fi or ff is not None and fecha >= ff):
                    for linea in lineas:
                        tweet_json = json.loads(linea)               
                        tweets.append(tweet_json)
        else:
            for linea in lineas:
                tweet_json = json.loads(linea)               
                tweets.append(tweet_json)
    
    all_tweets = comm.gather(tweets, root=0)
    if rank == 0:
        tweets_combinados = [tweet for sublist in all_tweets for tweet in sublist]
        return tweets_combinados
    else:
        return []


def grafoRt(data):
    G = nx.DiGraph()
    personas = []
    retweets = []
    for tweet in data:
        if tweet.get("retweeted_status") is not None:
            user = tweet.get("user").get("screen_name")
            rt_user = tweet.get("retweeted_status").get("user").get("screen_name")
            personas.append(user)
            personas.append(rt_user)
            retweets.append((user, rt_user))
    G.add_nodes_from(personas)
    G.add_edges_from(retweets)
    nx.write_gexf(G, "rtp.gexf")

def jsonRt(data):
    ReTweets = {}
    for tweet in data:
        if tweet.get("retweeted_status") is not None:
            user = tweet.get("user").get("screen_name")
            rt_user = tweet.get("retweeted_status").get("user").get("screen_name")
            tweetID = tweet.get("retweeted_status").get("id")
            if rt_user not in ReTweets:
                ReTweets[rt_user] = {}
                ReTweets[rt_user]['count'] = 0
            if tweetID not in ReTweets[rt_user]:
                ReTweets[rt_user][tweetID] = []
            if user not in ReTweets[rt_user][tweetID]:
                ReTweets[rt_user][tweetID].append(user)
                ReTweets[rt_user]['count'] += 1
    ReTweets = dict(sorted(ReTweets.items(), key=lambda x: x[1]['count'], reverse=True))
    jsonList = {}
    jsonList["retweets"] = []
    for rt_user in ReTweets:
        Tweet = {}
        Tweet["username"] = rt_user
        Tweet["receivedRetweets"] = ReTweets[rt_user]['count']
        Tweet["tweets"] = {}
        for tweetID in ReTweets[rt_user]:
            if tweetID != 'count':
                Tweet["tweets"][f"tweetID: {tweetID}"] = {}
                Tweet["tweets"][f"tweetID: {tweetID}"]["retweeted by"] = ReTweets[rt_user][tweetID]
        jsonList["retweets"].append(Tweet)
    with open("rtp.json", "w") as f:
        json.dump(jsonList, f, indent=4)
    return ReTweets
                    
def grafoMenciones(data):
    G = nx.DiGraph()
    personas = []
    mentions = []
    for tweet in data:
        if tweet.get("retweeted_status") is None:
            if tweet.get("entities") is not None:
                if tweet.get("entities").get("user_mentions") is not None:
                    user = tweet.get("user").get("screen_name")
                    personas.append(user)
                    menciones = tweet.get("entities").get("user_mentions")
                    for mencion in menciones:
                        men_user = mencion.get("screen_name")
                        if(men_user != "null"):
                            personas.append(men_user)
                            mentions.append((user, men_user))
    G.add_nodes_from(personas)
    G.add_edges_from(mentions)
    nx.write_gexf(G, "menciónp.gexf")
     
def jsonMenciones(data):
    Menciones = {}
    for tweet in data:
        if tweet.get("retweeted_status") is None:
            if tweet.get("entities") is not None:
                if tweet.get("entities").get("user_mentions") is not None:
                    user = tweet.get("user").get("screen_name")
                    menciones = tweet.get("entities").get("user_mentions")
                    tweetId = str(tweet.get("id"))
                    for mencion in menciones:
                        men_user = mencion.get("screen_name")
                        if(men_user != "null"):
                            if men_user not in Menciones:
                                Menciones[men_user] = {}
                                Menciones[men_user]["count"] = 0
                            if user not in Menciones[men_user]:
                                Menciones[men_user][user] = []
                            if tweetId not in Menciones[men_user][user]:
                                Menciones[men_user][user].append(tweetId)
                                Menciones[men_user]["count"] += 1  
    Menciones = dict(sorted(Menciones.items(), key=lambda x: x[1]['count'], reverse=True))
    mencionList = {}
    mencionList["mentions"] = []
    for men_user in Menciones:
        mention = {}
        mention["username"] = men_user
        mention["receivedMentions"] = Menciones[men_user]["count"]
        mention["mentions"] = []
        for user in Menciones[men_user]:
            mencion = {}
            if user != "count":
                mencion["mentionBy"] = user
                mencion["tweets"] = Menciones[men_user][user]
                mention["mentions"].append(mencion)
        mencionList["mentions"].append(mention)
    with open("menciónp.json", "w") as f:
        json.dump(mencionList, f, indent=4)
    return Menciones
    
def grafoCoRt(data):
    G = nx.Graph()
    personas = []
    coretweets = []
    co_retweets = {}
    for tweet in data:
        if tweet.get("retweeted_status") is not None:
            user = tweet.get("user").get("screen_name")
            rt_user = tweet.get("retweeted_status").get("user").get("screen_name")
            if user not in co_retweets:
                co_retweets[user] = []
            if rt_user not in co_retweets[user]:
                co_retweets[user].append(rt_user)
    for user, rt_user in co_retweets.items():
        if len(rt_user) > 1:
            for i in range(len(rt_user)-1):
                personas.append(rt_user[i])
                for j in range(i+1, len(rt_user)):
                    coretweets.append((rt_user[i], rt_user[j]))
            personas.append(rt_user[len(rt_user)-1])           
    G.add_nodes_from(personas)
    G.add_edges_from(coretweets)
    nx.write_gexf(G, "corrtwp.gexf")
    
def jsonCoRt(data):
    co_retweets = {}
    for tweet in data:
        if tweet.get("retweeted_status") is not None:
            user = tweet.get("user").get("screen_name")
            rt_user = tweet.get("retweeted_status").get("user").get("screen_name")
            if user not in co_retweets:
                co_retweets[user] = []
            if rt_user not in co_retweets[user]:
                co_retweets[user].append(rt_user)
    co_retweets2 = {}            
    for user, rt_user in co_retweets.items():
        if len(rt_user) > 1:
            for i in range(len(rt_user)-1):
                for j in range(i+1, len(rt_user)):
                    if(rt_user[i], rt_user[j]) not in co_retweets2:
                        co_retweets2[(rt_user[i], rt_user[j])] = {}
                        co_retweets2[(rt_user[i], rt_user[j])]["count"] = 1
                        co_retweets2[(rt_user[i], rt_user[j])]["users"] = []
                        co_retweets2[(rt_user[i], rt_user[j])]["users"].append(user)
                    else:
                        co_retweets2[(rt_user[i], rt_user[j])]["count"] += 1
                        co_retweets2[(rt_user[i], rt_user[j])]["users"].append(user)
    co_retweets2 = dict(sorted(co_retweets2.items(), key=lambda x: x[1]['count'], reverse=True))
    co_retweetsList = {}
    co_retweetsList["coretweets"] = []
    for coRt in co_retweets2:
        coretweet = {}
        authors = {}
        authors["u1"] = coRt[0]
        authors["u2"] = coRt[1]
        coretweet["authors"] = authors
        coretweet["totalCoretweets"] = co_retweets2[coRt]["count"]
        coretweet["retweeters"] = co_retweets2[coRt]["users"]
        co_retweetsList["coretweets"].append(coretweet)
    with open("corrtw.json", "w") as f:
        json.dump(co_retweetsList, f, indent=4)
    return co_retweets2
       
def main(argv): 
    ti = time.time()
    args = Parametros()
    fi, ff = TransformarFechas(args.fi, args.ff)

    # Procesar los datos
    if args.h is not None:
        data = descomprimirHashtags(recorrer(args.d), fi, ff, leerHashtags(args.h))
    else:
        data = descomprimir(recorrer(args.d), fi, ff)

    # Recolectar todos los datos en el proceso raíz
    all_data = comm.gather(data, root=0)

    if rank == 0:
        combined_data = [tweet for sublist in all_data for tweet in sublist]

        # Si hay menos de 6 procesos, el proceso raíz realiza todas las tareas
        if size <= 6:
            if args.grt:
                grafoRt(combined_data)
            if args.jrt:
                jsonRt(combined_data)
            if args.gm:
                grafoMenciones(combined_data)
            if args.jm:
                jsonMenciones(combined_data)
            if args.gcrt:
                grafoCoRt(combined_data)
            if args.jcrt:
                jsonCoRt(combined_data)
        else:
            for r in range(1, 7):
                comm.send(combined_data, dest=r)
    else:
        if rank < 7:
            combined_data = comm.recv(source=0)
            if rank == 1 and args.grt:
                grafoRt(combined_data)
            elif rank == 2 and args.jrt:
                jsonRt(combined_data)
            elif rank == 3 and args.gm:
                grafoMenciones(combined_data)
            elif rank == 4 and args.jm:
                jsonMenciones(combined_data)
            elif rank == 5 and args.gcrt:
                grafoCoRt(combined_data)
            elif rank == 6 and args.jcrt:
                jsonCoRt(combined_data)

    tf = time.time()
    print(f"Tiempo de proceso {rank}: {tf - ti} segundos")

if __name__ == "__main__":
    main(sys.argv[1:])



