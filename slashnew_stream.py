import os
import queue
import time
from dotenv import load_dotenv
from threading import Thread
from pymongo import MongoClient
from src import reddit_stream


class Slashnew_Stream:

    def __init__(self):
        dotenv_path = os.path.join(os.path.dirname(__file__), '.env')
        load_dotenv(dotenv_path)

        mongo_address = os.environ['MONGO_ADDRESS']
        if not mongo_address:
            raise ValueError("MONGO_ADDRESS Environment Variable Not Set")
        self.rs = reddit_stream.Reddit_Stream()
        self.mongo_client = MongoClient(mongo_address)
        self.meteor_db = self.mongo_client.meteor
        self.meteor_subreddits = self.meteor_db.subreddits
        self.meteor_push_flag = False
        self.subreddit_queue = queue.Queue(500)

    def run(self):
        rs_thread = Thread(target=self.rs.stream_queue,
                           args=('all', self.subreddit_queue, ))
        rs_thread.daemon = True
        rs_thread.start()
        counter = 0

        while True:
            submission = self.subreddit_queue.get()
            last_subreddit = str(submission.subreddit).lower()
            last_title = str(submission.title)
            last_author = str(submission.author)
            last_shortlink = str(submission.shortlink)
            last_over_18 = submission.over_18
            last_created = str(submission.created)

            self.meteor_subreddits.update_one({'name': last_subreddit, 'time': time.strftime("%d/%m/%Y")},
                                                        {'$inc': {'count': 1},
                                                         '$set': {'last_title': last_title,
                                                                  'last_author': last_author,
                                                                  'last_shortlink': last_shortlink,
                                                                  'last_over_18': last_over_18,
                                                                  'last_subreddit': last_subreddit,
                                                                  'last_timestamp': time.strftime("%d/%m/%Y %H/%m/%s")}},
                                                        upsert=True)
            # Increment grand total count
            self.meteor_subreddits.update_one({'name': 'all', 'time': time.strftime("%d/%m/%Y")},
                                                        {'$inc': {'count': 1},
                                                         '$set': {'last_title':   last_title,
                                                                  'last_author':   last_author,
                                                                  'last_shortlink':   last_shortlink,
                                                                  'last_over_18':    last_over_18,
                                                                  'last_subreddit': last_subreddit,
                                                                  'last_timestamp': time.strftime("%d/%m/%Y %H:%M:%S")}},
                                                        upsert=True)

            counter += 1
            print("ADDED: " + str(counter))


if __name__ == '__main__':
    ss = Slashnew_Stream()
    ss.run()
