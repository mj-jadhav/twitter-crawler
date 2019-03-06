(ns twitter-crawler.tweets
  "Crawler for twitter"
  {:author "Mayur Jadhav <mayur@dataorc.in"}
  (:require [twitter-crawler.storage.gcp :as tcsg]
            [twitter-crawler.queue.rabbitmq :as tcqr]
            [twitter-crawler.utils.webdriver :as tcuw]
            [twitter-crawler.storage.postgres :as tcsp]
            [twitter-crawler.utils.settings :as settings]

            [langohr.queue :as lq]
            [langohr.basic :as lb]
            [langohr.core :as rmq]
            [clojure.string :as cs]
            [cheshire.core :as json]
            [clj-time.format :as ctf]
            [clj-time.coerce :as ctc]
            [langohr.channel :as lch]
            [langohr.consumers :as lc]
            [clj-webdriver.taxi :as ct]
            [clojure.tools.logging :as ctl]
            [clj-webdriver.driver :as cwd])
  (:import org.openqa.selenium.remote.DesiredCapabilities
           org.openqa.selenium.chrome.ChromeDriver
           org.openqa.selenium.chrome.ChromeOptions)
  (:gen-class))

(def twitter-date-formatter (ctf/formatter "h:m a - d MMM YYYY"))

(def twitter-url "https://twitter.com")

(defn get-stat-count
  "Get count for given stat class. Stat class includes: likes, comments, retweets"
  [parent-elem stat-class]
  (try
    (let [stat-elem (ct/find-element-under parent-elem
                                           {:class stat-class})
          stat-count-elem (try
                            (-> stat-elem
                                (ct/find-element-under {:class "ProfileTweet-actionCount"}))
                            (catch Exception e
                              nil))]
      (if (and (seq stat-count-elem)
               (ct/attribute stat-count-elem
                             :data-tweet-stat-count))
        (-> stat-count-elem
            (ct/attribute :data-tweet-stat-count)
            read-string)
        (->> (ct/text stat-elem)
             (re-find #"\d+")
             read-string)))
    (catch Exception e
      0)))


(defn get-tweet-details
  [tweet_id tweet_username]
  (let [tweet-json-data (ct/find-element {:data-tweet-id tweet_id})
        tweet-text-container (ct/find-element-under tweet-json-data
                                                    {:class "js-tweet-text-container"})
        tweet-text (ct/text tweet-text-container)
        tweet-links (seq (keep #(when (ct/exists? %)
                                  (ct/attribute % :href))
                               (-> tweet-text-container
                                   (ct/find-elements-under {:tag "a"}))))
        tweet-has-cards? (ct/attribute tweet-json-data
                                       :data-has-cards)
        tweet-mentions-text (ct/attribute tweet-json-data
                                          :data-mentions)
        tweet-mentions (when-not (empty? tweet-mentions-text)
                         (cs/split tweet-mentions-text #" "))
        tweet-screen-name (ct/attribute (ct/find-element {:data-tweet-id tweet_id})
                                        :data-screen-name)
        retweet? (or (not (= tweet-screen-name
                             tweet_username))
                     (ct/exists? ".QuoteTweet-container"))
        tweet-type (cond
                     (and (not (empty? tweet-text))
                          (= "true"
                             (ct/attribute tweet-json-data
                                           :data-has-cards))) "text-card"
                     (not (empty? tweet-text)) "text-only"
                     (= "true"
                        (ct/attribute tweet-json-data
                                      :data-has-cards)) "card-only")
        tweet-comments (get-stat-count tweet-json-data
                                       "ProfileTweet-action ProfileTweet-action--reply")
        tweet-retweets (get-stat-count tweet-json-data
                                       "ProfileTweet-action ProfileTweet-action--retweet js-toggleState js-toggleRt")
        tweet-likes (get-stat-count tweet-json-data
                                    "ProfileTweet-action ProfileTweet-action--favorite js-toggleState")
        published-date (-> (ct/find-element {:class "client-and-actions"})
                           (ct/find-element-under {:class "metadata"})
                           (ct/find-element-under {:tag "span"})
                           ct/text
                           (#(ctf/parse twitter-date-formatter %))
                           ctc/to-long
                           java.sql.Timestamp.)]
    {:tweet_text tweet-text
     :tweet_links (when (seq tweet-links)
                    (into-array String tweet-links))
     :tweet_mentions (when (seq tweet-mentions)
                       (into-array String tweet-mentions))
     :is_retweet retweet?
     :tweet_type tweet-type
     :total_comments tweet-comments
     :total_retweets tweet-retweets
     :total_likes tweet-likes
     :published_date published-date}))


(defn fetch-tweet-data
  [{:keys [tweet_id tweet_username tweet_url]
    :as tweet-info}]
  (let [opt (ChromeOptions.)
        _ (when (settings/get-config :headless-browser?)
            (.addArguments ^ChromeOptions opt ["--headless"]))
        cap (DesiredCapabilities/chrome)
        _ (.setCapability cap ChromeOptions/CAPABILITY opt)
        driver (cwd/init-driver (ChromeDriver. cap))]
    (binding [ct/*driver* driver]
      (try
        (ct/to (str twitter-url tweet_url))
        (let [tweet-details (get-tweet-details tweet_id tweet_username)
              file-path (str tweet_username
                             "/"
                             tweet_id
                             "/"
                             "tweet.html")
              html (ct/html (ct/find-element {:data-tweet-id tweet_id}))
              blob-name (.getName (tcsg/put-blob (settings/get-config :twitter-bucket)
                                                 file-path
                                                 (.getBytes html)))]
          (tcsp/insert! :tweets
                        (merge {:tweet_id tweet_id
                                :tweet_username tweet_username
                                :tweet_url tweet_url}
                               tweet-details)
                        :on-conflict "do-nothing"
                        :pk-fields [:profile_username]))
        (finally
          (ct/quit))))))


(defn init-setup
  []
  (settings/load-config)
  (tcsp/init-pg (settings/get-config :twitter-postgres))
  (tcqr/init-rabbitmq (settings/get-config :rabbitmq))
  (tcuw/init-driver (settings/get-config :chrome-driver)))


(defn tweet-urls-handler
  [ch {:keys [content-type delivery-tag type] :as meta} ^bytes payload]
  (let [tweet-details (json/parse-string (String. payload)
                                           true)]
    (try
      (fetch-tweet-data tweet-details)
      (catch Exception e
        (ctl/error e "tweet url consumer failed for " tweet-details)))))


(defn tweet-urls-consumer
  []
  (let [qname (settings/get-config [:queues :tweet-urls-queue])]
    (lq/declare tcqr/rabbit-chan qname {:exclusive false
                                        :auto-delete true
                                        :durable (settings/get-config :durable-queues?)})
    (let [ch (lch/open tcqr/rabbit-conn)]
      (lb/qos ch 1)
      (ctl/info "Starting tweet urls consumer: ")
      (lc/subscribe ch qname tweet-urls-handler {:auto-ack true}))))


(defn -main
  []
  (init-setup)
  (tweet-urls-consumer))
