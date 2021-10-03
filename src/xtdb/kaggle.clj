(ns xtdb.kaggle
  (:require [clj-http.client :as http]
            [xtdb.api :as xt]
            [jsonista.core :as json]
            [clojure.java.io :as io]
            [clojure.data.csv :as csv]
            [clojure.instant :as instant])
  (:import (java.io File)
           (clojure.lang IReduceInit)
           (java.util.zip ZipInputStream)))

(defn read-kaggle-auth [^File file]
  (map (json/read-value (slurp file)) ["username" "key"]))

(def ^:dynamic *kaggle-auth*
  (some-> (System/getenv "KAGGLE_KEY_FILE") io/file read-kaggle-auth))

(def host "https://www.kaggle.com/api/v1")

(defn dataset-reader [{:keys [owner-slug dataset-slug dataset-version file-name]}]
  (let [resp (http/get (str host (format "/datasets/download/%s/%s/%s" owner-slug dataset-slug file-name))
                       {:basic-auth *kaggle-auth*
                        :headers {"accept" "text/csv"}
                        :as :stream})]
    (if (= "application/zip" (get-in resp [:headers :content-type]))
      (io/reader (doto (ZipInputStream. (:body resp))
                   (.getNextEntry)))
      (io/reader (:body resp)))))

(defmulti dataset-file-names (juxt :owner-slug :dataset-slug))
(defmulti csv-row->ops-fn (juxt :owner-slug :dataset-slug :file-name))

(defn dataset->ops [{:keys [owner-slug dataset-slug] :as dataset-key}]
  (reify IReduceInit
    (reduce [_ f init]
      (reduce (fn [acc file-name]
                (let [file-key {:owner-slug owner-slug
                                :dataset-slug dataset-slug
                                :file-name file-name}]
                  (with-open [resp (dataset-reader file-key)]
                    (let [[header & rows] (csv/read-csv resp)]
                      (->> rows
                           (transduce (comp (map #(zipmap header %))
                                            (mapcat (csv-row->ops-fn file-key)))
                                      f
                                      acc))))))
              init
              (dataset-file-names dataset-key)))))

(defn ops->stream [stream ops]
  (with-open [w (io/writer stream)]
    (reduce (completing (fn [_ op]
                          (.write w (prn-str op))))
            nil
            ops)))

(defn ops<-stream [stream]
  (reify IReduceInit
    (reduce [_ f init]
      (with-open [rdr (io/reader stream)]
        (->> (line-seq rdr)
             (transduce (map read-string)
                        f
                        init))))))

(defmethod dataset-file-names ["tmdb" "tmdb-movie-metadata"] [_]
  #{"tmdb_5000_movies.csv" "tmdb_5000_credits.csv"})

(defmethod csv-row->ops-fn ["tmdb" "tmdb-movie-metadata" "tmdb_5000_movies.csv"] [_]
  (fn [{:strs [id title runtime budget revenue keywords genres release_date] :as row}]
    [[::xt/put
      {:xt/id (keyword (name 'tmdb) (str "movie-" id))
       :tmdb/type :movie
       :tmdb.movie/id (Long/parseLong id)
       :tmdb.movie/title title
       :tmdb.movie/budget (some-> budget Long/parseLong)
       :tmdb.movie/revenue (some-> revenue Long/parseLong)
       :tmdb.movie/keywords (->> (json/read-value keywords)
                                 (into #{} (map #(get % "name"))))
       :tmdb.movie/genres (->> (json/read-value genres)
                               (into #{} (map #(get % "name"))))
       :tmdb.movie/release_date release_date}
      (when-not (empty? release_date)
        (instant/read-instant-date release_date))]]))

(defmethod csv-row->ops-fn ["tmdb" "tmdb-movie-metadata" "tmdb_5000_credits.csv"] [_]
  (fn [{:strs [movie_id cast crew] :as row}]
    (let [movie-id (Long/parseLong movie_id)]
      (concat (->> (for [{cast-name "name", :strs [credit_id id character]} (json/read-value cast)]
                     [[::xt/put {:xt/id (keyword (name 'tmdb) (str "person-" id))
                                 :tmdb/type :person
                                 :tmdb.person/id id
                                 :tmdb.person/name cast-name}]
                      [::xt/put {:xt/id (keyword (name 'tmdb) (str "cast-credit-" credit_id))
                                 :tmdb/type :cast-credit
                                 :tmdb.movie/id (keyword (name 'tmdb) (str "movie-" movie-id))
                                 :tmdb.person/id (keyword (name 'tmdb) (str "person-" id))
                                 :tmdb.cast/character character}]])
                   (apply concat))
              (->> (for [{crew-name "name", :strs [credit_id id job]} (json/read-value crew)]
                     [[::xt/put {:xt/id (keyword (name 'tmdb) (str "person-" id))
                                 :tmdb/type :person
                                 :tmdb.person/id id
                                 :tmdb.person/name crew-name}]
                      [::xt/put {:xt/id (keyword (name 'tmdb) (str "crew-credit-" credit_id))
                                 :tmdb/type :crew-credit
                                 :tmdb.movie/id (keyword (name 'tmdb) (str "movie-" movie-id))
                                 :tmdb.person/id (keyword (name 'tmdb) (str "person-" id))
                                 :tmdb.crew/job job}]])
                   (apply concat))))))

(comment
  (->> (dataset->ops {:owner-slug "tmdb", :dataset-slug "tmdb-movie-metadata"})
       (ops->stream (io/output-stream (io/file "/tmp/movies.edn")))))
