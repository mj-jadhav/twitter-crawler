(defproject twitter-crawler "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [clj-webdriver "0.7.2"]
                 [com.google.guava/guava "22.0"]
                 [enlive "1.0.0" :exclusions [org.clojure/clojure]]
                 ;; Needed by "remote" code
                 [org.seleniumhq.selenium/selenium-server "2.47.1"]
                 ;; Needed by core code
                 [org.seleniumhq.selenium/selenium-java "2.47.0"]
                 [org.seleniumhq.selenium/selenium-remote-driver "2.47.1"
                  :exclusions [com.google.guava/guava]]
                 [com.codeborne/phantomjsdriver "1.2.1"
                  :exclusions [org.seleniumhq.selenium/selenium-java
                               org.seleniumhq.selenium/selenium-server
                               org.seleniumhq.selenium/selenium-remote-driver]]
                 [clj-time "0.15.0"]
                 [cheshire "5.5.0"]
                 [clj-http "2.3.0"]
                 [org.clojure/tools.logging "0.3.1"]
                 [org.slf4j/slf4j-log4j12 "1.7.12"]
                 [log4j/log4j "1.2.17"]
                 [org.clojure/java.jdbc "0.7.8"]
                 [honeysql "0.9.4"]
                 [com.mchange/c3p0 "0.9.5.2"]
                 [org.postgresql/postgresql "42.2.5"]
                 [nilenso/honeysql-postgres "0.2.5"]
                 [com.google.cloud/google-cloud-storage "1.59.0"]
                 [com.novemberain/langohr "5.1.0"]
                 [clojurewerkz/quartzite "2.1.0"]]
  :jvm-opts ["-Dlogfile=./twitter-crawler.log"]
  :aot [twitter-crawler.tweets
        twitter-crawler.twitter-scheduler
        twitter-crawler.twitter]
  :main twitter-crawler.core)
