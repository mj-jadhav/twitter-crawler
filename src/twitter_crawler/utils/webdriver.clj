(ns twitter-crawler.utils.webdriver
  "Clj-webdriver utility fns"
  {:author "Mayur Jadhav <mayur@dataorc.in>"}
  (:require [clj-webdriver.taxi :as ct]))

(defn init-driver
  [driver-location]
  (System/setProperty "webdriver.chrome.driver"
                      driver-location))
