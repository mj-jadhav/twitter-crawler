(ns twitter-crawler.twitter
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
            [langohr.channel :as lch]
            [clj-time.core :as ctime]
            [clj-time.format :as ctf]
            [langohr.consumers :as lc]
            [clj-http.client :as http]
            [clj-webdriver.taxi :as ct]
            [clj-webdriver.driver :as cwd]
            [clojure.tools.logging :as ctl])
  (:import org.openqa.selenium.remote.DesiredCapabilities
           org.openqa.selenium.chrome.ChromeDriver
           org.openqa.selenium.chrome.ChromeOptions)
  (:gen-class))


(def file-date-format  (ctf/formatter "YYYYMMddhh"))

(def twitter-xhr-url (partial format
                              "https://twitter.com/i/profiles/show/%s/timeline/tweets?include_available_features=1&include_entities=1&max_position=%s&reset_error_state=false"))

(def twitter-cookie-names ["_ga" "_twitter_sess" "guest_id"
                           "personalization_id" "lang" "ct0" "_gid"])

(defn cookie->map
  "Convert all browser cookies to map"
  []
  (reduce (fn [acc cookie]
            (assoc acc
                   (:name cookie)
                   (:value cookie)))
          {}
          (ct/cookies)))

(defn get-twitter-cookie
  "Get twitter specific cookies data"
  []
  (let [cookies-map (cookie->map)
        twitter-cookies (select-keys cookies-map
                                     twitter-cookie-names)
        cookie-str "tfw_exp=0;"]
    (reduce (fn [acc [k v]]
              (str acc
                   " "
                   k
                   "="
                   v
                   ";"))
            cookie-str
            twitter-cookies)))

(defn get-tweets-data
  "Return all tweet urls given html body"
  [body]
  [(->> (re-seq #"data-permalink-path=\"\S+\"" (:items_html body))
        (mapv #(json/parse-string (last (cs/split %
                                                  #"=")))))
   (-> (re-seq #"tweet-id=\"\d+\"" (:items_html body))
       last
       (cs/split #"=")
       last
       json/parse-string)])


(defn make-twitter-xhr
  "Make XHR to fetch next set of tweets"
  [twitter-id last-tweet-id]
  (-> (http/get (twitter-xhr-url twitter-id last-tweet-id)
                {:headers {:cookie (get-twitter-cookie)
                           :accept-encoding "gzip, deflate, br"
                           :accept-language "en-IN,en;q=0.9,mr-IN;q=0.8,mr;q=0.7,en-GB;q=0.6,en-US;q=0.5"
                           :user-agent (ct/execute-script "return navigator.userAgent;")
                           :accept "application/json, text/javascript, */*; q=0.01"
                           :referer (str "https://twitter.com/" twitter-id)
                           :authority "twitter.com"
                           :x-requested-with "XMLHttpRequest"
                           :x-twitter-active-user "yes"}})
      :body
      (json/parse-string true)))


(defn fetch-tweet-streams
  "Fetch all tweets of given twitter id till last fetched tweet"
  [twitter-id last-id]
  (loop [last-tweet-id last-id
         tweet-links []]
    (let [response (make-twitter-xhr twitter-id last-tweet-id)]
      (if (:has_more_items response)
        (let [[tweet-permalinks last-tweet] (get-tweets-data response)
              last-tweet-link (last tweet-permalinks)
              last-tweet-id (last (cs/split last-tweet-link #"/"))
              tweet-count-query (format "select count(*) from tweets where tweet_id='%s' and tweet_url='%s' and tweet_username='%s'"
                                        last-tweet-id
                                        last-tweet-link
                                        twitter-id)
              last-tweet-exists? (pos? (-> (tcsp/execute-query [tweet-count-query])
                                           first
                                           :count))]
          (ctl/info "Tweet urls fetched: " (count tweet-permalinks))
          (when-not last-tweet-exists?
            (recur last-tweet
                   (concat tweet-links tweet-permalinks))))
        (let [[tweet-permalinks last-tweet] (get-tweets-data response)]
          (concat tweet-links tweet-permalinks))))))


(defn get-tweet-urls
  "Return tweet-urls given tweet li html elements"
  [tweet-items]
  (keep (fn [li-elem]
          (try
            (let [div-element (ct/find-element-under li-elem
                                                     {:tag "div"})
                  uri (ct/attribute div-element
                                    :data-permalink-path)]
              uri)
            (catch Exception e
              nil)))
        tweet-items))


(defn fetch-tweets
  [{:keys [profile_username]
    :as twitter-id-info}]
  (let [opt (ChromeOptions.)
        _ (when (settings/get-config :headless-browser?)
            (.addArguments ^ChromeOptions opt ["--headless"]))
        cap (DesiredCapabilities/chrome)
        _ (.setCapability cap ChromeOptions/CAPABILITY opt)
        driver (cwd/init-driver (ChromeDriver. cap))]
    (binding [ct/*driver* driver]
      (try
        (ct/to (str "https://twitter.com/" profile_username))
        (let [tweet-items (ct/find-elements-under (ct/find-element {:id "timeline"})
                                                  {:tag "li"})
              tweet-urls (get-tweet-urls tweet-items)
              last-id (re-find #"\d{7,}"
                               (last tweet-urls))
              all-tweet-links (concat tweet-urls
                                      (fetch-tweet-streams profile_username
                                                           last-id))
              output-csv-path (str profile_username
                                   (str "/tweet_links_"
                                        (ctf/unparse file-date-format (ctime/now))
                                        ".csv"))
              ch (lch/open tcqr/rabbit-conn)]
          (tcsg/put-blob (settings/get-config :twitter-bucket)
                         output-csv-path
                         (.getBytes (cs/join "\n"
                                             all-tweet-links)))
          (ctl/info "Total tweets for username: " profile_username " are " (count all-tweet-links))
          (doseq [tweet-link all-tweet-links]
            (let [tweet-id (last (cs/split tweet-link #"/"))]
              (lb/publish ch
                          tcqr/default-exchange-name
                          (settings/get-config [:queues :tweet-urls-queue])
                          (json/generate-string (assoc twitter-id-info
                                                       :tweet_username profile_username
                                                       :tweet_id tweet-id
                                                       :tweet_url tweet-link))
                          {:persistent (settings/get-config :durable-queues?)
                           :content-type "application/json"
                           :type "twitter.tweet_urls"})))
          (rmq/close ch))
        (finally
          (ct/quit))))))

(defn push-tweet-links
  [profiles tweet-links-filename]
  (doseq [profile profiles]
    (try
      (let [blob (tcsg/get-blob-content (settings/get-config :twitter-bucket)
                                        (str (:profile_username profile)
                                             "/"
                                             tweet-links-filename))
            tweet-links (cs/split (apply str (map char blob))
                                  #"\n")
            ch (lch/open tcqr/rabbit-conn)]
        (doseq [tweet-link tweet-links]
          (Thread/sleep 2000)
          (ctl/info "pushing tweet: " tweet-link)
          (let [tweet-id (last (cs/split tweet-link #"/"))]
            (lb/publish ch
                        tcqr/default-exchange-name
                        (settings/get-config [:queues :tweet-urls-queue])
                        (json/generate-string (assoc profile
                                                     :tweet_username (:profile_username profile)
                                                     :tweet_id tweet-id
                                                     :tweet_url tweet-link))
                        {:persistent (settings/get-config :durable-queues?)
                         :content-type "application/json"
                         :type "twitter.tweet_urls"}))))
      (catch Exception e
        (ctl/error "Failed to get tweet links for profile " (:profile_username profile))))))


(defn twitter-ids-handler
  [ch {:keys [content-type delivery-tag type] :as meta} ^bytes payload]
  (let [twitter-id-info (json/parse-string (String. payload)
                                           true)]
    (try
      (ctl/info "Fetching tweet links for username: " (:profile_username twitter-id-info))
      (fetch-tweets twitter-id-info)
      (catch Exception e
        (ctl/error e "twitter ID consumer failed for " twitter-id-info)))))


(defn twitter-ids-consumer
  []
  (let [qname (settings/get-config [:queues :twitter-ids-queue])]
    (lq/declare tcqr/rabbit-chan qname {:exclusive false
                                       :auto-delete true
                                       :durable (settings/get-config :durable-queues?)})
    (let [ch (lch/open tcqr/rabbit-conn)]
      (lb/qos ch 1)
      (ctl/info "Starting twitter ids consumer: ")
      (lc/subscribe ch qname twitter-ids-handler {:auto-ack true}))))


(defn init-setup
  []
  (settings/load-config)
  (tcsp/init-pg (settings/get-config :twitter-postgres))
  (tcqr/init-rabbitmq (settings/get-config :rabbitmq))
  (tcuw/init-driver (settings/get-config :chrome-driver)))


(defn -main
  []
  (init-setup)
  (twitter-ids-consumer))
