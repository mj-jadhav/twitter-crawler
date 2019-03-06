(ns twitter-crawler.storage.gcp
  "Ref: https://github.com/hswick/google-cloud-storage-clj/blob/master/src/google_cloud_storage_clj/core.clj"
  (:require [clojure.java.io :as io])
  (:import [com.google.cloud.storage StorageOptions
            BlobId
            Blob
            BucketInfo
            BlobInfo
            StorageImpl
            Blob$BlobSourceOption
            Storage$BucketTargetOption
            Storage$BucketGetOption
            Storage$BucketSourceOption
            Storage$BucketListOption
            Storage$BlobGetOption
            Storage$BlobListOption
            Storage$BlobTargetOption
            Storage$BlobSourceOption
            Storage$BlobWriteOption]))


(defn storage-client
  "Instantiate storage client using default authorization"
  []
  (.getService (StorageOptions/getDefaultInstance)))


(defn get-blob
  [bucket-name blob-name]
  (.get (storage-client)
        bucket-name
        blob-name
        (make-array Storage$BlobGetOption 0)))


(defn put-blob
  [bucket blob-name bytes-content]
  (.create (storage-client)
           (.build (BlobInfo/newBuilder (BlobId/of bucket
                                                   blob-name)))
           bytes-content
           (make-array Storage$BlobTargetOption 0)))


(defn download-to
  [bucket blob-name destination]
  (let [blob (get-blob bucket
                       blob-name)]
    (io/make-parents destination)
    (.downloadTo blob
                 (.toPath (io/file destination))
                 (make-array Blob$BlobSourceOption
                             0))))

(defn get-blob-content
  [bucket blob-name]
  (.getContent (get-blob bucket
                         blob-name)
               (make-array Blob$BlobSourceOption
                           0)))
