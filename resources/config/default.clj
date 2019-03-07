{:twitter-cron "0 0 1 * * ?" ;; UTC daily at 1:00AM
 :twitter-postgres {:classname "org.postgresql.Driver"
                    :jdbc-url "jdbc:postgresql://localhost:5432/twitter"
                    :user "twitter"
                    :password "postgres"
                    :max-idle-excess  (* 4 60)
                    :idle-test-period (* 5 60)
                    :test-query "SELECT 1"}
 :chrome-driver "/Users/mj/Downloads/chromedriver"
 :queues {:twitter-ids-queue "twitter.ids"
          :tweet-urls-queue "twitter.tweet_urls"}
 :twitter-bucket "twitter"
 :rabbitmq {:host "localhost" :port 5672}
 :headless-browser? false
 :durable-queues? false}
