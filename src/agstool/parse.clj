(ns agstool.parse
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.data.csv :as csv]
            [clojure.data.json :as json])
  (:import [java.sql DriverManager]
           [org.apache.poi.xssf.usermodel XSSFWorkbook]
           [org.apache.poi.ss.usermodel CellType]))

(defn read-and-print [link]
  (
   with-open [rdr (io/reader link)]
   (doseq [line (line-seq rdr)]
     (println line))))




(defn split-line [line]
  (str/split line #","))


(defn parse-line [line]
  (when (seq line)
    {:type (first line)
     :content (vec (rest line))}))

(defn group-tables [lines]
  (->> lines
       (partition-by #(or (empty? %) 
                         (and (seq %) (= "GROUP" (first %)))))
       (partition-all 2)
       (map #(apply concat %))
       (remove #(every? empty? %))))

(defn parse-table [table-lines]
  (let [parsed-lines (keep parse-line table-lines)
        group-line (first (filter #(= (:type %) "GROUP") parsed-lines))
        heading-line (first (filter #(= (:type %) "HEADING") parsed-lines))
        unit-line (first (filter #(= (:type %) "UNIT") parsed-lines))
        type-line (first (filter #(= (:type %) "TYPE") parsed-lines))
        data-lines (filter #(= (:type %) "DATA") parsed-lines)]
    {:group (first (:content group-line))
     :heading (vec (:content heading-line))
     :unit (vec (:content unit-line))
     :type (vec (:content type-line))
     :data (mapv :content data-lines)}))

(defn parse-file [file-path]
  (with-open [reader (io/reader file-path)]
    (let [csv-data (csv/read-csv reader)
          tables (group-tables csv-data)]
      (mapv parse-table tables))))

;; Implementations of converter
(defn export-to-excel [ast file-path]
  (let [workbook (XSSFWorkbook.)]
    (doseq [table ast]
      (let [sheet (.createSheet workbook (:group table))
            header-row (.createRow sheet 0)]
        
        ;; Write headers
        (doseq [[idx header] (map-indexed vector (:heading table))]
          (let [cell (.createCell header-row idx)]
            (.setCellValue cell header)))
        
        ;; Write data rows
        (doseq [[row-idx row-data] (map-indexed vector (:data table))]
          (let [row (.createRow sheet (inc row-idx))]
            (doseq [[col-idx value] (map-indexed vector row-data)]
              (let [cell (.createCell row col-idx)]
                (.setCellValue cell value)))))))
    
    ;; Write to file
    (with-open [out (io/output-stream file-path)]
      (.write workbook out))))

;; SQLite Export
(defn create-table-sql [table]
  (let [table-name (str/replace (:group table) #"\s+" "_")
        columns (map-indexed 
                 (fn [idx col] 
                   (format "%s TEXT" (str/replace col #"\s+" "_")))
                 (:heading table))]
    (format "CREATE TABLE IF NOT EXISTS %s (%s)"
            table-name
            (str/join ", " columns))))

(defn insert-data-sql [table]
  (let [table-name (str/replace (:group table) #"\s+" "_")
        value-placeholders (str/join ", " (repeat (count (:heading table)) "?"))]
    (format "INSERT INTO %s VALUES (%s)" table-name value-placeholders)))

(defn export-to-sqlite [ast db-path]
  (let [db-url (str "jdbc:sqlite:" db-path)]
    (Class/forName "org.sqlite.JDBC")
    (with-open [conn (DriverManager/getConnection db-url)]
      (doseq [table ast]
        (let [stmt (.createStatement conn)]
          ;; Create table
          (.executeUpdate stmt (create-table-sql table))
          ;; Insert data
          (let [insert-sql (insert-data-sql table)
                prepared-stmt (.prepareStatement conn insert-sql)]
            (doseq [row (:data table)]
              (doseq [[idx value] (map-indexed vector row)]
                (.setString prepared-stmt (inc idx) value))
              (.executeUpdate prepared-stmt))))))))

;; Separate CSVs Export
(defn export-to-separate-csvs [ast base-path]
  (doseq [table ast]
    (let [file-name (str base-path "/" (str/replace (:group table) #"\s+" "_") ".csv")
          csv-data (cons (:heading table) (:data table))]
      (with-open [writer (io/writer file-name)]
        (csv/write-csv writer csv-data)))))

;; JSON Export
(defn export-to-json [ast file-path]
  (let [json-data (json/write-str 
                    (into {} (map (fn [table] 
                                   [(:group table) 
                                    {:headers (:heading table)
                                     :units (:unit table)
                                     :types (:type table)
                                     :data (:data table)}]) 
                                 ast)))]
    (spit file-path json-data)))

;; Utility function to export to all formats
(defn export-all-formats [ast base-path]
  (let [base-name (str/replace base-path #"\..*$" "")]
    (export-to-excel ast (str base-name ".xlsx"))
    (export-to-sqlite ast (str base-name ".db"))
    (export-to-separate-csvs ast (str base-name "_csvs"))
    (export-to-json ast (str base-name ".json"))))


;; Example usage

(def test-path "/home/sam/Personalcode/agstool/resources/testfile.ags")
(def out (parse-file test-path))
(println (type out))

