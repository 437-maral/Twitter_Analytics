import time
from datetime import datetime
import csv
from configparser import ConfigParser
from random import randint
import asyncio
from twikit import Client, TooManyRequests


QUERY = '(from:elonmusk) lang:en until:2020-01-01 since:2018-01-01'

#* login credentials
config = ConfigParser()
config.read('config.ini')
username = config['X']['username']
email = config['X']['email']
password = config['X']['password']

#* create a csv file and write the header
with open('tweets.csv', 'w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['Tweet_count', 'Username', 'Text', 'Created At', 'Retweets', 'Likes'])


async def main(tweets):
    client = Client(language='en-US')
    #await client.login(auth_info_1=username, auth_info_2=email, password=password)
    
    #client.save_cookies('cookies.json')
    client.load_cookies('cookies.json')
    
    print(f'{datetime.now()} - Getting tweets...')
    if tweets is None:
        # Initial tweet fetch
        tweets = await client.search_tweet(QUERY, product='Media')
    else:
        wait_time = randint(5, 10)
        print(f'{datetime.now()} - Getting next tweets after {wait_time} seconds...')
        await asyncio.sleep(wait_time)
        tweets = await tweets.next()  # Assuming the client supports this for pagination
    
    return tweets


async def fetch_tweets():
    tweet_count = 0
    tweets = None
    
    # Open the CSV file once for writing all data
    with open('tweets.csv', 'a', newline='') as file:
        writer = csv.writer(file)
        
        while tweet_count < MINIMUM_TWEETS:
            try:
                tweets = await main(tweets)
            except TooManyRequests as e:
                rate_limit_reset = datetime.fromtimestamp(e.rate_limit_reset)
                print(f'{datetime.now()} - Rate limit reached. Waiting until {rate_limit_reset}')
                wait_time = rate_limit_reset - datetime.now()
                await asyncio.sleep(wait_time.total_seconds())  # Non-blocking wait for rate limit reset
                continue  # Retry fetching after the rate limit expires
            
            if not tweets:
                print(f'{datetime.now()} - No more tweets found')
                break
            
            # Process and write tweets to CSV
            for tweet in tweets:
                tweet_count += 1
                tweet_data = [
                    tweet_count,
                    tweet.user.name,
                    tweet.text,
                    tweet.created_at,
                    tweet.retweet_count,
                    tweet.favorite_count
                ]
                writer.writerow(tweet_data)
                
            print(f'{datetime.now()} - Got {tweet_count} tweets so far')

    print(f'{datetime.now()} - Done! Got {tweet_count} tweets in total')


# Entry point to run the async code
if __name__ == "__main__":
    MINIMUM_TWEETS = 100
    asyncio.run(fetch_tweets())
