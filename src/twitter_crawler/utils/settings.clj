(ns twitter-crawler.utils.settings
  "Fetch enviroment settings"
  {:author "Mayur Jadhav <mayur@dataorc.in>"})

(defonce config (atom {}))

(defn load-config
  []
  (reset! config
          (eval
           (read-string (slurp (get (System/getenv)
                                    "config_file"))))))

(defn get-config
  [k]
  (cond
    (vector?  k) (get-in @config k)
    (keyword? k) (get @config k)))
