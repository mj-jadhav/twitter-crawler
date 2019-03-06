(ns twitter-crawler.twitter-scheduler
  "Publish twitter ids to the queue"
  {:author "Mayur Jadhav <mayur@dataorc.in>"}
  (:require [twitter-crawler.queue.rabbitmq :as tcqr]
            [twitter-crawler.utils.webdriver :as tcuw]
            [twitter-crawler.storage.postgres :as tcsp]
            [twitter-crawler.utils.settings :as settings]

            [langohr.queue :as lq]
            [langohr.basic :as lb]
            [clojure.string :as cs]
            [cheshire.core :as json]
            [langohr.channel :as lch]
            [clj-webdriver.taxi :as ct]
            [clj-webdriver.driver :as cwd]
            [clojure.tools.logging :as ctl]
            [clojurewerkz.quartzite.jobs :as qj]
            [clojurewerkz.quartzite.triggers :as t]
            [clojurewerkz.quartzite.scheduler :as qs]
            [clojurewerkz.quartzite.schedule.cron :refer [schedule cron-schedule]])
  (:import org.openqa.selenium.remote.DesiredCapabilities
           org.openqa.selenium.chrome.ChromeDriver
           org.openqa.selenium.chrome.ChromeOptions)
  (:gen-class))

(defonce twiiter-scheduler nil)


(defn publish-twitter-ids
  "Push all twitter profiles to the queue. Consumer will fetch latest tweets
  of all the profiles in the queue"
  []
  (let [twitter-profiles (tcsp/query :twitter_profile
                                     nil
                                     :fields [:*]
                                     :limit nil)
        qname (settings/get-config [:queues :twitter-ids-queue])]
    (lq/declare tcqr/rabbit-chan
                qname
                {:exclusive false
                 :auto-delete true
                 :durable (settings/get-config :durable-queues?)})
    (ctl/info "Publishing twitter profiles to the queue")
    (doseq [twitter-profile twitter-profiles]
      (ctl/info "Pushing twitter profile: " (:profile_username twitter-profile))
      (lb/publish tcqr/rabbit-chan
                  tcqr/default-exchange-name
                  qname
                  (json/generate-string twitter-profile)
                  {:persistent (settings/get-config :durable-queues?)
                   :content-type "application/json"
                   :type "election.district"}))
    (ctl/info "Done with the publishing of twitter profiles")))


(qj/defjob daily-twitter-crawler-job
  [ctx]
  (try
    (publish-twitter-ids)
    (catch Exception e
      (ctl/error e "Daily twitter crawler job failed with following exception: "))))


(defn daily-twitter-crawler
  "Schedule daily twitter crawler"
  []
  (let [job (qj/build
             (qj/of-type daily-twitter-crawler-job)
             (qj/with-identity (qj/key "daily.twitter.crawler")))
        trigger-key (t/key "daily.triggers.twitter.1")
        trigger (t/build
                 (t/with-identity trigger-key)
                 (t/with-schedule (schedule
                                   (cron-schedule (settings/get-config :twitter-cron)))))]
    (qs/schedule twiiter-scheduler job trigger)))


(defn init-setup
  []
  (settings/load-config)
  (tcsp/init-pg (settings/get-config :twitter-postgres))
  (tcqr/init-rabbitmq (settings/get-config :rabbitmq))
  (tcuw/init-driver (settings/get-config :chrome-driver))
  (let [s (-> (qs/initialize)
              qs/start)]
    (alter-var-root #'twiiter-scheduler (constantly s)))
  (ctl/info "Done with the initial setup!"))


(defn -main
  []
  (init-setup)
  (.addShutdownHook (Runtime/getRuntime)
                    (Thread. (fn [& args]
                               (tcqr/close-rabbitmq)
                               (shutdown-agents))))
  (daily-twitter-crawler))


(defn get-stat-count
  [stat-class]
  (try
    (-> (ct/find-element {:class stat-class})
        (ct/find-element-under {:class "ProfileNav-value"})
        (ct/attribute :data-count)
        read-string)
    (catch Exception e
      0)))


(defn get-profile-details
  []
  (let [profile-id (->> (ct/attribute (ct/find-element {:id "init-data"})
                                      :value)
                        (re-find #"\"profile_id\":\d+")
                        (re-find #"\d+")
                        read-string)
        profile-name (ct/text (ct/find-element {:class "ProfileHeaderCard-name"}))
        bio (ct/text (ct/find-element {:class "ProfileHeaderCard-bio u-dir"}))
        joined-date (ct/text (ct/find-element {:class "ProfileHeaderCard-joinDateText js-tooltip u-dir"}))
        username-elem (ct/find-element {:class "ProfileHeaderCard-screennameLink u-linkComplex js-nav"})
        username (cs/replace (ct/text username-elem)
                             "@"
                             "")
        profile-link (ct/attribute username-elem
                                   :href)
        total-tweets (get-stat-count "ProfileNav-item ProfileNav-item--tweets is-active")
        total-following (get-stat-count "ProfileNav-item ProfileNav-item--following")
        total-followers (get-stat-count "ProfileNav-item ProfileNav-item--followers")
        total-likes (get-stat-count "ProfileNav-item ProfileNav-item--favorites")]
    {:id profile-id
     :profile_username username
     :profile_name profile-name
     :profile_link profile-link
     :bio bio
     :joined_date joined-date
     :total_tweets total-tweets
     :total_followers total-followers
     :total_following total-following
     :total_likes total-likes}))


(defn insert-twitter-profile-info
  "Insert twitter profile to start crawling the tweets"
  [twitter-profile-link]
  (let [opt (ChromeOptions.)
        _ (when (settings/get-config :headless-browser?)
            (.addArguments ^ChromeOptions opt ["--headless"]))
        cap (DesiredCapabilities/chrome)
        _ (.setCapability cap ChromeOptions/CAPABILITY opt)
        driver (cwd/init-driver (ChromeDriver. cap))]
    (binding [ct/*driver* driver]
      (try
        (ct/to twitter-profile-link)
        (tcsp/insert! :twitter_profile
                      (get-profile-details)
                      :on-conflict "do-nothing"
                      :pk-fields [:profile_username])
        (finally
          (ct/quit))))))


