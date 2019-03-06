(ns twitter-crawler.storage.postgres
  "Utility functions for postgres"
  {:author "Mayur Jadhav <mayur@dataorc.in>"}
  (:require [honeysql.core :as sql]
            [clojure.java.jdbc :as jdbc]
            [honeysql.helpers :as helpers]
            [honeysql-postgres.format :as psqlf]
            [honeysql-postgres.helpers :as psqlh])
  (:import (com.mchange.v2.c3p0 ComboPooledDataSource)))


(defonce db-conn nil)

(defn create-c3p0-pool
  "Create C3P0 pooled datasource"
  [spec]
  (let [cpds (doto (ComboPooledDataSource.)
               (.setDriverClass (:classname spec))
               (.setJdbcUrl (:jdbc-url spec))
               (.setUser (:user spec))
               (.setPassword (:password spec))
               (.setMaxIdleTimeExcessConnections (:max-idle-excess spec))
               (.setIdleConnectionTestPeriod (:idle-test-period spec))
               (.setPreferredTestQuery (:test-query spec)))]
    {:datasource cpds}))


(defn init-pg
  "Initializes PG connection"
  [dbspec]
  (alter-var-root #'db-conn (constantly (create-c3p0-pool dbspec))))


(defn insert!
  "Inserts given key value map into specified table

   Params:
   table: (keyword) Name of the table
   kv-map: (map) Key representing the columns in the table and their values"
  [table kv-map & {:keys [on-conflict
                          update-fields
                          pk-fields]
                   :or {on-conflict "do-nothing"}}]
  (let [on-conflict-action (condp = on-conflict
                             "do-nothing" (-> (psqlh/on-conflict)
                                              (psqlh/do-nothing))
                             "update-set" (if (and (seq update-fields)
                                                   (seq pk-fields))
                                            (-> (apply psqlh/on-conflict pk-fields)
                                                ((fn [m]
                                                   (apply psqlh/do-update-set m update-fields))))
                                            (AssertionError. "Update and pk fields required for update-set conflict action")))]
    (jdbc/with-db-connection [db-spec db-conn]
      (jdbc/execute! db-spec
                     (-> (helpers/insert-into table)
                         (helpers/values [kv-map])
                         (psqlh/upsert on-conflict-action)
                         sql/format)))))


(defn insert-multi!
  "Bulk Inserts into specified table

   Params:
   table: (keyword) Name of the table
   data: (list of maps) Key representing the columns in the table and their values"
  [table data]
  (jdbc/with-db-connection [db-spec db-conn]
    (jdbc/execute! db-spec
                   (-> (helpers/insert-into table)
                       (helpers/values data)
                       (psqlh/upsert (-> (psqlh/on-conflict)
                                         (psqlh/do-nothing)))
                       sql/format))))


(defn upsert!
  "Updates columns or inserts a new row in the specified table"
  [table row where-clause]
  (jdbc/with-db-transaction [t-con db-conn]
    (let [result (jdbc/update! t-con table row where-clause)]
      (if (zero? (first result))
        (jdbc/insert! t-con table row)
        result))))


(defn upsert-multi!
  "Update multiple rows or insert if not exists"
  [table rows]
  (doseq [{:keys [row where-clause]} rows]
    (upsert! table row where-clause)))


(defn query
  "Makes select query against the table

  Params:
  table: (keyword) Name of the table
  where-clause: (list) e.g. [:and [:= :ac_index ac-index]
                                  [:= :district_id district_id]]
  fields: (list)(optional) list of fields for projections. Default is *
  limit: (Integer) No. of rows to fetch. Default is 10
  offset: (Integer)Starting offset. Default is 0"
  [table where-clause & {:keys [fields limit offset]
                         :or {fields :*
                              limit 10
                              offset 0}}]
  (jdbc/with-db-transaction [t-conn db-conn]
    (jdbc/query t-conn
                (-> (apply helpers/select fields)
                    (helpers/from table)
                    (helpers/where where-clause)
                    (helpers/limit limit)
                    (helpers/offset offset)
                    sql/format))))


(defn execute-query
  "Executes raw query

  Params:
  query: (vector) list of sql params"
  [query]
  (jdbc/with-db-connection [db-spec db-conn]
    (jdbc/query db-spec
                query)))
