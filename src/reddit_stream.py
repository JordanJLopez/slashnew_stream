import praw
import os
import queue

class Reddit_Stream:

    def __init__(self):
        self.reddit_user_agent = os.environ['CLIENT_USER_AGENT']
        self.reddit_client_id = os.environ['CLIENT_ID']
        self.reddit_client_secret = os.environ['CLIENT_SECRET']
        self.reddit_username = os.environ['CLIENT_USERNAME']
        self.reddit_password = os.environ['CLIENT_PASSWORD']

        if not any([self.reddit_user_agent, self.reddit_client_id,
                    self.reddit_client_secret, self.reddit_username,
                    self.reddit_password]):
            raise ValueError('Reddit_Stream Environment Variables Not Set')

        self.reddit = praw.Reddit(user_agent=self.reddit_user_agent,
                                  client_id=self.reddit_client_id,
                                  client_secret=self.reddit_client_secret,
                                  username=self.reddit_username,
                                  password=self.reddit_password)

    def stream_stdout(self, subreddit_name):
        subreddit = self.reddit.subreddit(subreddit_name)
        for submission in subreddit.stream.submissions():
            print(str(submission.subreddit))
        return 0

    def stream_queue(self, subreddit_name, shared_queue):
        subreddit = self.reddit.subreddit(subreddit_name)
        for submission in subreddit.stream.submissions():
            shared_queue.put(str(submission.subreddit))
        return 0

if __name__ == '__main__':
    rs = Reddit_Stream()
    rs.stream_stdout('all')
