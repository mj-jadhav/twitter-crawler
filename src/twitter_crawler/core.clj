(ns twitter-crawler.core
  "Standalone mode twitter crawler"
  {:author "Mayur Jadhav <dataorc.in>"}
  (:require [twitter-crawler.twitter :as tct]
            [twitter-crawler.tweets :as tctt]
            [twitter-crawler.queue.rabbitmq :as tcqr]
            [twitter-crawler.utils.webdriver :as tcuw]
            [twitter-crawler.storage.postgres :as tcsp]
            [twitter-crawler.twitter-scheduler :as tcts]
            [twitter-crawler.utils.settings :as settings]

            [langohr.queue :as lq]
            [langohr.basic :as lb]
            [langohr.core :as rmq]
            [cheshire.core :as json]
            [langohr.channel :as lch]
            [clojure.tools.logging :as ctl]))


(defn init-setup
  []
  (settings/load-config)
  (tcsp/init-pg (settings/get-config :twitter-postgres))
  (tcqr/init-rabbitmq (settings/get-config :rabbitmq))
  (tcuw/init-driver (settings/get-config :chrome-driver))
  (ctl/info "Initializing subscribers...")
  (tctt/tweet-urls-consumer)
  (tct/twitter-ids-consumer)
  (ctl/info "Done with the initial setup!"))


(defn -main
  []
  (init-setup)
  (.addShutdownHook (Runtime/getRuntime)
                      (Thread. (fn [& args]
                                 (tcqr/close-rabbitmq)
                                 (shutdown-agents))))
  (tcts/publish-twitter-ids))
