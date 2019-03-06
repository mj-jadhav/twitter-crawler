(ns twitter-crawler.queue.rabbitmq
  "Utility functions for rabbitmq queue"
  {:author "Mayur Jadhav<mayur@dataorc.in>"}
  (:require [langohr.core :as rmq]
            [langohr.queue :as lq]
            [langohr.basic :as lb]
            [cheshire.core :as json]
            [langohr.channel :as lch]
            [langohr.exchange  :as le]
            [langohr.consumers :as lc]
            [clojure.tools.logging :as ctl])
  (:import java.util.concurrent.Executors))

(def rabbit-conn nil)
(def rabbit-chan nil)

(def ^{:const true}
  default-exchange-name "")

(defn init-rabbitmq
  ([]
   (init-rabbitmq {:host "localhost" :port 5672}))
  ([config]
   (let [conn (rmq/connect (merge config
                                  (when (:executor config)
                                    {:executor (Executors/newFixedThreadPool (:executor config))})))
         ch (lch/open conn)]
     (alter-var-root #'rabbit-conn (constantly conn))
     (alter-var-root #'rabbit-chan (constantly ch))
     (ctl/info "Connected. Channel id: " (.getChannelNumber ch)))))


(defn close-rabbitmq
  []
  (rmq/close rabbit-chan)
  (rmq/close rabbit-conn))
