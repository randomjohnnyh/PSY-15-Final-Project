import tweepy
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import os
import sys
import math
import json
import re
import collections
import time
import pickle
import argparse
import numpy as np
import random

states = {"Alabama": "AL", "Alaska": "AK", "Arizona": "AZ", "Arkansas": "AR", "California": "CA", "Colorado": "CO", "Connecticut": "CT", "Delaware": "DE", "Florida": "FL", "Georgia": "GA", "Hawaii": "HI", "Idaho": "ID", "Illinois": "IL", "Indiana": "IN", "Iowa": "IA", "Kansas": "KS", "Kentucky": "KY", "Louisiana": "LA", "Maine": "ME", "Maryland": "MD", "Massachusetts": "MA", "Michigan": "MI", "Minnesota": "MN", "Mississippi": "MS", "Missouri": "MO", "Montana": "MT", "Nebraska": "NE", "Nevada": "NV", "New Hampshire": "NH", "New Jersey": "NJ", "New Mexico": "NM", "New York": "NY", "North Carolina": "NC", "North Dakota": "ND", "Ohio": "OH", "Oklahoma": "OK", "Oregon": "OR", "Pennsylvania": "PA", "Rhode Island": "RI", "South Carolina": "SC", "South Dakota": "SD", "Tennessee": "TN", "Texas": "TX", "Utah": "UT", "Vermont": "VT", "Virginia": "VA", "Washington": "WA", "West Virginia": "WV", "Wisconsin": "WI", "Wyoming": "WY"}
rev_states = {v: k for k, v in states.items()}

southern = {"Delaware", "Maryland", "West Virginia", "Virginia", "North Carolina", "South Carolina", "Georgia", "Florida", "Kentucky", "Tennessee", "Alabama", "Mississippi", "Arkansas", "Oklahoma", "Louisiana", "Texas"}
for state in southern:
    assert state in states
northern = set(states.keys()) - southern

democratic = {"Vermont", "Hawaii", "Rhode Island", "Massachusetts", "New York", "California", "Maryland", "New Mexico", "Illinois", "Connecticut", "New Jersey", "Washington", "Delaware", "Oregon", "Michigan", "Pennsylvania", "Florida", "Minnesota", "North Carolina", "Ohio"}
republican = {"Wisconsin", "Arizona", "Colorado", "Louisiana", "Virginia", "Kentucky", "Iowa", "Maine", "Georgia", "Nevada", "Mississippi", "Texas", "Indiana", "West Virginia", "Arkansas", "Missouri", "Nebraska", "New Hampshire", "Tennessee", "South Carolina", "Kansas", "Oklahoma", "Montana", "South Dakota", "Alaska", "Alabama", "North Dakota", "Utah", "Idaho", "Wyoming"}
for state in democratic:
    assert state in states
for state in republican:
    assert state in states
assert(len(democratic) + len(republican) == len(states))


keys = [
[
"F4MpY3eaH81dZfbB2r7fdynzr",
"KABebQos6eiyrSTBP1RLHqNhad10mEmv8vAvApQqgdDdGzboDF",
"449094976-l8LmPjJY3d01V1kg9dZkQaoyyqW2xwEZzs8Ztmcf",
"BR0bODmDxPwVedySUpuOZwzJ7DhUb73WAIKZeffcqx2jq"],
[
"wUQ8J7uIDnmgY2IAiZiq3K87U",
"4q5EkjpSpWY0CxEocVtsk14uLeXlS0yLWF09UD8uzfgdVXsHRj",
"449094976-BozzTipla7Syyjjx99s7m4pnqSgHGiUbPplYCSqt",
"pqId344dz1sUOzefItZj7cqlE9QhTzTcz4ag5m9iW88ja"],
[
"aV88YOV4Q18gjxPIUBmTHs3rT",
"BD1WS5tweDb8mOERtxMh5kN4q0fWhOmL85OWcDrAnYmu7VQveQ",
"449094976-KY7xNeZyXqzPJv9Ui34NnGjoFGvS299HQiVhRSZD",
"hYpZJVolIBcbrBJDyjSwzKMbOuKaEjSUEJyrT4fjr2uc1"],
[
"EUZPE7N1abaR1VSSsivBMb51f",
"l5VVvepmDoVh5s8mjShhfn7oS2TndlGHF6e3LAgoGpn9AFLFRV",
"449094976-YhoeFwAc5yq5bUXEgjTXu8FFFoRCmy0pMRAbAWDG",
"W9fh8YGo8RR6ab6xLMdZwoRdTDPvLEUvhireccApGh9jT"],
]

apis = []
for consumer_key, consumer_secret, access_token, access_token_secret in keys:
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    api = tweepy.API(auth)
    apis.append(api)

def get_api():
    return random.choice(apis)


class StdOutListener(StreamListener):
    """ A listener handles tweets that are received from the stream.
    This is a basic listener that just prints received tweets to stdout.
    """
    def on_data(self, data):
        d = json.loads(data)
        #d = replace_utf8(d)
        process_tweet(d)
        return True

    def on_error(self, status):
        print(status)

alpha = 0.1
default = lambda: collections.Counter()
global_counter = default()

def get_words(text):
    words = set()
    alpha = re.sub(r'[^a-z]+', ' ', text.lower())
    for word in alpha.split():
        if word:
            words.add(word)
    punc = re.sub(r'[^@$\'".,!?:;\-()]+', ' ', text)
    for word in alpha.split():
        if word:
            words.add(word)
    # Prior in all sentences
    words.add("")
    return words


MULT = 1.5

class NBClassifier(object):
    def __init__(self):
        self.freq = collections.defaultdict(default)
        self.total = default()
        self.tags = set()

    def train(self, filename):
        with open(filename, "r") as f:
            for line in f:
                tokens = line.strip().split(" ", 1)
                if len(tokens) < 2:
                    continue
                tag = tokens[0]
                if not tag:
                    continue
                self.tags.add(tag)
                words = set()
                text = tokens[-1]
                words = get_words(text)
                for word in words:
                    self.freq[word][tag] += 1
                    self.total[word] += 1
        print len(self.total)
        print self.tags

    def train_csv(self, filename):
        with open(filename, "r") as f:
            for line in f:
                tokens = line.strip().split(",", 5)
                if len(tokens) < 6:
                    continue
                tag = tokens[0][1:-1]
                if not tag:
                    continue
                if tag == "4":
                    tag = "1"
                self.tags.add(tag)
                words = set()
                text = tokens[-1][1:-1]
                words = get_words(text)
                for word in words:
                    self.freq[word][tag] += 1
                    self.total[word] += 1
        print len(self.total)
        print self.tags

    def predict(self, text):
        log_p = default()
        words = get_words(text)
        for word in words:
            if word in self.freq:
                freqs = self.freq[word]
                for tag in self.tags:
                    count = freqs[tag]+alpha if tag in freqs else alpha
                    log_p[tag] += math.log(count / (self.total[word] + len(self.tags) * alpha))
        # Add multiplier
        log_p["1"] += math.log(MULT)
        min_tag, min_value = max(log_p.items(), key=lambda a: a[1])
        return min_tag, min_value

classifier = NBClassifier()

users = set()
location = {}
reply_to = {}
sentiment = {}
coordinates = {}

def get(struct, key):
    if isinstance(struct, dict):
        return struct.get(key, None)
    return getattr(struct, key, None)

RADIUS = 3959  # miles
THRESH = 50

def get_distance(coord1, coord2):
    coord1 = coord1 / 180.0 * np.pi
    coord2 = coord2 / 180.0 * np.pi
    diff = coord1 - coord2
    dlon = diff[0]
    dlat = diff[1]
    a = math.sin(dlat/2)**2 + math.cos(coord1[1]) * math.cos(coord2[1]) * math.sin(dlon/2)**2
    c = 2 * math.atan2( math.sqrt(a), math.sqrt(1-a) )
    d = RADIUS * c
    return d

def process_tweet(tweet):
    id = int(get(tweet, 'id'))
    user = get(tweet, 'author') or tweet['user'];
    uid = int(get(user, 'id'))
    reply_uid = get(tweet, 'in_reply_to_user_id')
    reply_id = get(tweet, 'in_reply_to_status_id')
    place = get(tweet, 'place')
    text = get(tweet, 'text')
    if uid == reply_uid:
        return

    global_counter["all"] += 1
    if reply_id:
        global_counter["reply"] += 1
    if place:
        global_counter["place"] += 1

    coords = None
    if place:
        coords = get(place, 'bounding_box')
        coords = get(coords, 'coordinates')[0]
        if coords:
            coords = np.array(coords, dtype=np.float64)
            coords = np.mean(coords, axis=0)
            coordinates[id] = coords

    res = classifier.predict(text)[0]
    sentiment[id] = res

    if reply_id and reply_uid and place:
        global_counter["reply_place"] += 1
        country = get(place, 'country')
        if country == "United States":
            global_counter["US"] += 1
            name = get(place, 'name')
            state = None
            if name in states:
                state = name
            else:
                name = get(place, 'full_name')
                tokens = name.split()
                fin = tokens[-1]
                if fin in rev_states:
                    state = rev_states[fin]
            if state:
                global_counter["state"] += 1
                print "state", state
                #print "id", id
                #print "uid", uid
                #print "reply_id", reply_id
                #print "reply_uid", reply_uid
                print text
                print res
                print
                users.add(uid)
                users.add(reply_uid)
                reply_to[id] = reply_id
                location[id] = state

def get_totals(counts, cur):
    total = default()
    for state in cur:
        total.update(counts[state])
    return total

def main(args):
    classifier.train("mpqa.all")
    classifier.train("custrev.all")
    classifier.train("stsa.binary.train")
    classifier.train("subj.all")
    classifier.train("words.all")
    classifier.train_csv("emoticon.csv")

    users.update(pickle.load(open("users.p", "rb")))
    if args.sample:
        l = StdOutListener()
        stream = Stream(auth, l)
        try:
            stream.sample(languages=["en"])
        except tweepy.TweepError as e:
            print e
            pass
        except KeyboardInterrupt:
            pass

        print global_counter
        pickle.dump(users, open("users.p", "wb"))

    print len(users)
    print users

    state_sent = collections.defaultdict(default)
    dist_sent = collections.defaultdict(default)

    if args.fetch:
        c = 0
        for user in users.copy():
            print c
            try:
                api = get_api()
                tweets = api.user_timeline(user, count=50)
                data = api.rate_limit_status()
                print data['resources']['statuses']['/statuses/user_timeline']

                for tweet in tweets:
                    process_tweet(tweet)
                time.sleep(0.1)
            except tweepy.TweepError as e:
                print e
            c += 1

        for rep in reply_to:
            ori = reply_to[rep]
            if rep in location and ori in sentiment and rep in sentiment:
                loc = location[rep]
                state_sent[loc][(sentiment[ori], sentiment[rep])] += 1
        pickle.dump(dict(state_sent), open("states.p", "wb"))

        for rep in reply_to:
            ori = reply_to[rep]
            if ori in coordinates and rep in coordinates \
                    and ori in sentiment and rep in sentiment:
                dist = get_distance(coordinates[ori], coordinates[rep])
                dist_sent[dist > THRESH][(sentiment[ori], sentiment[rep])] += 1
                print ori, rep, coordinates[ori], coordinates[rep], dist
        pickle.dump(dict(dist_sent), open("dist.p", "wb"))
    else:
        state_sent.update(pickle.load(open("states.p", "rb")))
        dist_sent.update(pickle.load(open("dist.p", "rb")))

    print state_sent
    print dist_sent

    print dist_sent[True]
    print dist_sent[False]

    south_total = get_totals(state_sent, southern)
    north_total = get_totals(state_sent, northern)
    print south_total
    print north_total

    rep_total = get_totals(state_sent, republican)
    dem_total = get_totals(state_sent, democratic)
    print rep_total
    print dem_total

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('sample', help="Sample?",
                        const=0, nargs="?", default=0,
                        type=int)
    parser.add_argument('fetch', help="Fetch?",
                        const=0, nargs="?", default=0,
                        type=int)
    args = parser.parse_args(sys.argv[1:])
    main(args)
