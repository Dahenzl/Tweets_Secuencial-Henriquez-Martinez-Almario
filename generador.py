import os
import json
import bz2
import networkx as nx
import time
import sys
import argparse
from datetime import date

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

def TransformarFechas(fi, ff):
    if(fi is not None):
        fi = fi.split("-")
        fi = date(int(fi[2]), int(fi[1]), int(fi[0]))
    if(ff is not None):
        ff = ff.split("-")
        ff = date(int(ff[2]), int(ff[1]), int(ff[0]))
    if(fi is not None and ff is not None and fi > ff):
        print("Fecha inicial mayor que fecha final")
        sys.exit()
    return fi, ff

def leerHashtags(path):
    h_in = open(path, "r")
    lineas = h_in.readlines()
    h_in.close()
    lineas = [linea.strip() for linea in lineas]
    for i in range(len(lineas)):
        if(lineas[i][0] == "#"):
            lineas[i] = lineas[i][1:]
    return lineas

def recorrer(path):
    tweets = []
    for carpeta, _, archivo in os.walk(path):
        for tweet in archivo:
            ruta_tweet = os.path.join(carpeta, tweet)
            tweets.append(ruta_tweet)
    return tweets

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
                                    list_hashtags_json.append(hashtag_json)
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
                                list_hashtags_json.append(hashtag_json)
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
                        list_hashtags_json.append(hashtag_json)
                if any(hashtag in list_hashtags_json for hashtag in hashtags):
                    tweets.append(tweet_json)
    return tweets

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
    return tweets

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
    nx.write_gexf(G, "rt.gexf")

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
    with open("rt.json", "w") as f:
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
    nx.write_gexf(G, "mención.gexf")
     
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
    with open("mención.json", "w") as f:
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
    nx.write_gexf(G, "corrtw.gexf")
    
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
    if (args.h is not None):
        data = descomprimirHashtags(recorrer(args.d), fi, ff, leerHashtags(args.h))
    else:
        data = descomprimir(recorrer(args.d), fi, ff)
    if(args.grt):
        grafoRt(data)
    if(args.jrt):
        jsonRt(data)
    if(args.gm):
        grafoMenciones(data)
    if(args.jm):
        jsonMenciones(data)
    if(args.gcrt):
        grafoCoRt(data)
    if(args.jcrt):
        jsonCoRt(data)
    tf = time.time()
    print(tf - ti)
    
if __name__ == "__main__":
    main(sys.argv[1:])
