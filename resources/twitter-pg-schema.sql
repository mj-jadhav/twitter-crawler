CREATE USER twitter WITH password 'postgres';
CREATE DATABASE twitter OWNER twitter;

CREATE TABLE twitter_profile (id varchar,
                              profile_username varchar NOT NULL PRIMARY KEY,
                              profile_name varchar NOT NULL,
                              profile_link varchar,
                              bio varchar,
                              location varchar,
                              joined_date varchar,
                              total_tweets integer,
                              total_followers integer,
                              total_following integer,
                              total_likes integer);


CREATE TABLE tweets (tweet_id varchar NOT NULL,
                     tweet_username varchar REFERENCES twitter_profile(profile_username),
                     tweet_url varchar NOT NULL,
                     tweet_text varchar,
                     tweet_links TEXT [],
                     tweet_mentions TEXT [],
                     tweet_type varchar,
                     published_date TIMESTAMP,
                     is_retweet boolean,
                     meta json,
                     retweet_id varchar,
                     retweet_text varchar,
                     retweet_username varchar,
                     retweet_likes integer,
                     retweet_comments integer,
                     retweet_retweets integer,
                     total_comments integer,
                     total_likes integer,
                     total_retweets integer,
                     PRIMARY KEY(tweet_id, tweet_url, tweet_username));

