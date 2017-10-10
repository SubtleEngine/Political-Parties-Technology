import subprocess
import os.path
import urllib.parse
import twitter
import pandas as pd
import collections
import datetime
import se_keys # Twitter API keys

api = twitter.Api(consumer_key=se_keys.consumer_key,
    consumer_secret=se_keys.consumer_secret,
    access_token_key=se_keys.access_token_key,
    access_token_secret=se_keys.access_token_secret,
    tweet_mode="extended", # Downloads full text of tweet
    sleep_on_rate_limit=True)

parties = [
    {"name": "Lib Dem", "slug": "lib_dem", "pdf": "Manifesto-Final.pdf", "since": "2017-09-16", "until": "2017-09-20", "hashtags": ["#ldconf", "#libdem", "#libdems", "@libdems"]},
    {"name": "Labour", "slug": "labour", "pdf": "labour-manifesto-2017.pdf", "since": "2017-09-24", "until": "2017-09-28", "hashtags": ["#labour", "#labour17", "#lab17", "#labourconference17", "#labourconf", "#labourconf17", "#labconf", "#labconf17", "@uklabour"]},
    {"name": "UKIP", "slug": "ukip", "pdf": "UKIP_Manifesto_June2017opt.pdf", "since": "2017-09-29", "until": "2017-10-01", "hashtags": ["#ukip", "#ukipconf", "@ukip"]},
    {"name": "Conservative", "slug": "conservative", "pdf": "Manifesto2017.pdf", "since": "2017-10-01", "until": "2017-10-05", "hashtags": ["#cpc17", "#cpc2017", "#conservativepartyconference", "@conservatives"]},
    {"name": "Plaid Cymru", "slug": "plaid_cymru", "pdf": "Plaid_Cymru_-_Defending_Wales_-_2017_Action_Plan.pdf"},
    {"name": "SNP", "slug": "snp", "pdf": "Manifesto_06_01_17.pdf"},
    {"name": "Green", "slug": "green", "pdf": "greenguaranteepdf.pdf"}
]

topics = {
    "technology": {"name": "Technology", "phrases": ["technolog", "automation", "internet", "digital", "cyber", "data", "artificial intel", "algorithm", "augmented real", "blockchain", "bitcoin", "crypt", "drone", "robot", "virtual real", "3d print"], "acronyms": ["ai", "ar", "iot", "tech", "vr", "web"]}
}

#---------- Functions ----------

def fetch_tweets(search_string):
    """ Fetch as many tweets as possible and return a DataFrame. """
    all_tweets = pd.DataFrame()
    query = {"q": search_string, "count": 100} # Get (up to) 100 tweets
    results = api.GetSearch(raw_query=urllib.parse.urlencode(query))
    if len(results) > 0:
        tweets = pd.DataFrame([s.AsDict() for s in results])
        all_tweets = all_tweets.append(tweets)
        while len(results) > 1: # 1 tweet will always be returned when using max_id, as max_id inclusive: https://dev.twitter.com/rest/public/timelines)
            max_id = tweets["id"].min() # Get oldest tweet
            results = api.GetSearch(raw_query=urllib.parse.urlencode(dict(query, **{"max_id": max_id}))) # Get tweets up to & inc. max_id
            tweets = pd.DataFrame([s.AsDict() for s in results])
            all_tweets = all_tweets.append(pd.DataFrame(tweets)) # Append to all_tweets
            print(max_id)
    tweets.drop_duplicates(subset="id", inplace=True)
    all_tweets["created_at"] = pd.to_datetime(all_tweets["created_at"])
    all_tweets.set_index(pd.DatetimeIndex(all_tweets["created_at"]), inplace=True)
    return all_tweets

def count_terms(text, topic):
    """ Search a document for phrases or acronyms and return a dict of counts. """
    counts = {}
    for phrase in topics[topic]["phrases"]:
        counts[phrase] = text.lower().count(phrase) # Phrases can be fragments e.g. "crypt" would be found in "encryption", "cryptocurrency" and "#ProtectEncryption"
    for acronym in topics[topic]["acronyms"]:
        counts[acronym] = text.lower().replace("#", "").split().count(acronym) # Acronyms must be whole e.g. "ai" would be found in "AI" or "#AI" but not in "#AISummit"
    return counts

def count_terms_tweets(text, topic):
    """ Wrapper for count_terms() to strip @mentions and URLs and return a boolean for the topic. """
    text = " ".join(w for w in text.split() if not w.startswith(("@", "http")))
    counts = count_terms(text, topic)
    if any(c > 0 for c in counts.values()):
        return pd.Series({"technology_terms": counts, "technology": True})
    else:
        return pd.Series({"technology_terms": counts, "technology": False})

def search_manifestos():
    """ Convert PDF manifestos to text, search for terms and output a .csv table. """
    data = pd.DataFrame()
    for party in parties:
        manifesto_text_path = "Manifestos/"+party["pdf"].replace(".pdf", ".txt")
        if not os.path.exists(manifesto_text_path):
            subprocess.run(["pdftotext", manifesto_text_path])
        with open(manifesto_text_path, "r") as f:
            text = f.read()
            for topic in topics:
                counts = count_terms(text, "technology")
                data = data.append(pd.Series(counts, name=party["name"]))
    data.sort_index(axis=0, inplace=True)
    data.sort_index(axis=1, inplace=True)
    data = data.transpose()
    data_final = data.loc[(data!=0).any(axis=1)] # Suppress terms with zero mentions
    print(", ".join(sorted(list(set(data.index.values) - set(data_final.index.values))))) # Print the suppressed terms
    data_final.to_csv("manifestos_tech_count.csv")

def search_tweets():
    """ Fetch tweets by hashtag, pickle, search result for terms and output .csv table. """
    data = pd.DataFrame()
    for party in parties[:4]:
        pickle_path = "Tweets/"+party["name"]+".pickle"
        if not os.path.exists(pickle_path):
            search_string = " OR ".join(party["hashtags"])+" exclude:retweets since:" + party["since"] + " until:" + party["until"]
            tweets = fetch_tweets(search_string)
            tweets.to_pickle(pickle_path)
        else:
            tweets = pd.read_pickle(pickle_path)
        tweets.drop_duplicates(subset="id", inplace=True)

        """
        # Use this block to re-run the search
        if "technology" in tweets.columns:
            tweets.drop("technology", axis=1, inplace=True)
            tweets.drop("technology_terms", axis=1, inplace=True)
        tweets.reset_index(inplace=True, drop=True) # Important, reset index before apply and join, as datetime index has duplicates
        technology = tweets["full_text"].apply(count_terms_tweets, args=(["technology"]))
        tweets = tweets.join(technology)
        tweets.set_index(pd.DatetimeIndex(tweets["created_at"]), inplace=True) # Return index to datetime
        tweets.to_pickle(pickle_path)
        """

        tweets.loc[tweets["technology"] == True, ["full_text", "id", "user", "technology_terms", "technology"]].to_csv("tweets_"+party["slug"]+"_tech.csv")
        counts = collections.Counter()
        [counts.update(d) for d in tweets["technology_terms"].tolist()]
        data = data.append(pd.Series(counts, name=party["name"]))
    data.sort_index(axis=0, inplace=True)
    data.sort_index(axis=1, inplace=True)
    data = data.transpose()
    data_final = data.loc[(data>4).any(axis=1)] # Suppress terms with zero mentions
    print(", ".join(sorted(list(set(data.index.values) - set(data_final.index.values))))) # Print the suppressed terms
    data_final.to_csv("tweets_tech_count.csv")

def plot_tweets():
    """ Aggregate all conference and topic tweets by hour and output table. """
    data_hourly = pd.DataFrame()
    data_hourly_tech = pd.DataFrame()
    data_hourly_tech_pct = pd.DataFrame()
    data_daily = pd.DataFrame()
    data_daily_tech = pd.DataFrame()
    data_daily_tech_pct = pd.DataFrame()
    for party in parties[:4]:
        tweets = pd.read_pickle("Tweets/"+party["name"]+".pickle")
        tweets.drop_duplicates(subset="id", inplace=True)
        hourly = tweets["id"].groupby(pd.TimeGrouper("h")).count().rename(party["name"])
        hourly_tech = tweets.loc[tweets["technology"] == True, "id"].groupby(pd.TimeGrouper("h")).count().rename(party["name"])
        hourly_tech_pct = hourly_tech.div(hourly)
        data_hourly = data_hourly.append(hourly)
        data_hourly_tech = data_hourly_tech.append(hourly_tech)
        data_hourly_tech_pct = data_hourly_tech_pct.append(hourly_tech_pct)
        daily = tweets["id"].groupby(pd.TimeGrouper("d")).count().rename(party["name"])
        print(party["name"])
        print(daily.mean())
        daily_tech = tweets.loc[tweets["technology"] == True, "id"].groupby(pd.TimeGrouper("d")).count().rename(party["name"])
        daily_tech_pct = daily_tech.div(daily)
        data_daily = data_daily.append(daily)
        data_daily_tech = data_daily_tech.append(daily_tech)
        data_daily_tech_pct = data_daily_tech_pct.append(daily_tech_pct)
    data_hourly.transpose().to_csv("tweets_all_hour.csv")
    data_hourly_tech.transpose().to_csv("tweets_tech_hour.csv")
    data_hourly_tech_pct.transpose().to_csv("tweets_tech_as_pct_hour.csv")
    data_daily.transpose().to_csv("tweets_all_day.csv")
    data_daily_tech.transpose().to_csv("tweets_tech_day.csv")
    data_daily_tech_pct.transpose().to_csv("tweets_tech_as_pct_day.csv")

#---------- Main programme ----------

#search_manifestos()
#search_tweets()
plot_tweets()