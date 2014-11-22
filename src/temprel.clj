(ns temprel
  (:require [clojure.set :as sets]))
 
; relation is a sequence of maps
; maps contain :start and :end keywords defining the timebounds and a set of values that apply during those time bounds
;
; states are opaque to the framework
;
; 1) Two implementation choices: each map represents one time tick from t to t+1 and values that span times are in multiple maps
; or, 2) each map represents a range of times to which the values apply
;
; using (1) means that we need to have a value for all ticks, which with milliseconds could mean lots of records in sparsely populated events
;
; starting with (2) (since I implemented (1) in javascript)
 
(def infinity Long/MAX_VALUE)
 
(def tuple-comparator
  (reify java.util.Comparator
    (compare [this a b] (compare (:start a) (:start b)))))
 
(defn temporal-relation
  "constructor for temporal relations - takes a sequence of tuples to be added"
  ([] (sorted-set-by tuple-comparator))
  ([tuples] (apply (partial sorted-set-by tuple-comparator) tuples)))
 
(defn overlap?
  [{start1 :start end1 :end} {start2 :start end2 :end}]
  (and (< start1 end2)
       (> end1 start2)))
 
(defn from-timed-list
  "returns a temporal relation for a timed list of events"
  ([evts] (from-timed-list (fn [ix v] ix) evts))
  ([timestamp-fn evts ]
    (temporal-relation
      (map-indexed
        (fn [ix v]
          (let [start-time (timestamp-fn ix v)
                end-time (inc start-time)] ; events are by definition a point in time
            {:start start-time :end end-time :values #{v}}))
        evts))))
 
(defn starting-states
  [start-fn evts]
  (for [e evts v (:values e) s (start-fn v)] {:start (:start e) :end infinity :state s}))
 
(defn states-at-time
  [states t]
  (set (filter #(and (>= t (:start %)) (< t (:end %))) states)))
 
(defn end-state
  [states s t]
  (set (map #(if (and (= (:state %) s) (>= t (:start %)) (< t (:end %)))
                 ; Note that the t+1 logic lives here
                 (assoc % :end (inc t))
                 %)
            states)))
 
(defn end-states
  [states ended-states t]
  (if (seq ended-states)
    (recur (end-state states (first ended-states) t)
           (rest ended-states)
           t)
    states))
 
(defn xform-states
  [states-orig xform-fn event-relation]
  (loop [evts event-relation
         states states-orig] ; collection of states with their start/end - not yet a temporal relation
    (if (seq evts)
      (let [e (first evts)
            ; note: these are the states at t (directly, not in the map with start/end)
            st (set (map :state (states-at-time states (:start e))))
            ; for each value in e and each states at time t, invoke xform-fn to get It+1
            xform-states (set (flatten (for [v (:values e) s st] (xform-fn s v))))
            ; states not returned by the xform-fn are ended as of t+1
            ended-states (sets/difference st xform-states)
            ; states starting in the xform-fn - TODO: can't start and end a function in the xform for the same event
            new-states (sets/difference xform-states st)
            ]
        (recur (rest evts)
               (set (concat
                      (end-states states ended-states (:start e))
                      (map (fn [s] {:start (:start e) :end infinity :state s}) new-states)))))
      states)))
 
(defn distinct-ranges
  "Takes an overlapping sequence of time ranges and returns
   (empty value) tuples for the non-overlapping ranges that make up the same total time duration
   Ranges are maps with :start :end numeric values
   e.g.  0->10, 1->5, 0->13 =>  0->1, 1->5, 5->10, 10->13  "
  [ranges]
  (loop [endpoints (apply sorted-set (mapcat (fn [r] [(:start r) (:end r)]) ranges))
         relation (temporal-relation)]
    (if (seq (rest endpoints)) ; need to have 2 values left
        (recur (rest endpoints)
               (conj relation {:start (first endpoints) :end (second endpoints) :values #{}}))
        relation)))
 
; for each event
;   get states starting at time t
;   for each event/state pair
;     get transform states with xform-fn
;   if a state at time t is not in the combined list of transformed states, end it at t+1
;   if a transform state is not in the "states at t" add it from e to inf
; Note: this function only makes sense with a relation of "events", something that happens at one instant in time
(defn stateful
  "see paper"
  [start-fn xform-fn event-relation]
  (let [states (xform-states (starting-states start-fn event-relation) xform-fn event-relation)
        ranges (distinct-ranges states)]
    ; now we need to put these things into a temporal relation
    (temporal-relation
      (for [r ranges]
        ; for each range, add the state value for any states that overlap with this time range
        (assoc r :values (set (map :state (filter (partial overlap? r) states))))))))
        ; TODO: if I used :values and state as a collection, this could re-use pack-relation
 
 
(defn product
  " t (product r1 r2) {x1,x2} is defined as  t r1 x1 AND t r2 x2
  The relation that is the product of r1 and r2 relates time t to #{x1 x2} iff r1 relates t to x1 and t2 relates t to x2"
  [r1 r2]
  ; seems like this should be harder...
  (temporal-relation
    (for [t1 r1 t2 r2 :when (overlap? t1 t2)]
      {:start (max (:start t1) (:start t2))
       :end (min (:end t1) (:end t2))
       :values (set (for [v1 (:values t1) v2 (:values t2)] #{v1 v2})) })))
 
(defn pack-relation
  [relation]
  (temporal-relation
    (filter (comp seq :values) ; filter out ranges with no values
      (for [r (distinct-ranges relation)]
        ; note difference from above in map/mapcat and :state/:values
        (assoc r :values (set (mapcat :values (filter (partial overlap? r) relation))))))))
 
(defn union
  " the resulting relation relates t to x if r1 relates t to x, or r2 relates t to x"
  [r1 r2]
  (pack-relation (concat r1 r2)))
 
(defn filter-relation
  "regular old filer, just preserves the ordered-set type, and handles the multiple values per time window"
  [predicate relation]
  (temporal-relation
    (filter
      (comp seq :values)
      (map (fn [tuple]
              (assoc tuple :values (set (filter predicate (:values tuple)))))
            relation))))
 
; Note to self - having to redefine these operations = symptom that I should be using transducers?
(defn map-relation
  [mapper relation]
  (temporal-relation
    (map
      (fn [tuple]
        (assoc tuple :values (set (map mapper (:values tuple)))))
      relation)))
 
;
; Temporal Operations ------------------------
;
; TODO: is my only option to convert to timelines, adjust starts, then re-pack?
; does make for simple temporal functions though
 
(defn merge-ranges
  "note that this assumes the values are the same"
  [[t1 & more]]
  (cond (nil? t1) []
        (nil? more) [t1]
        :else
        ; compare first two items - if continugous replace first two items with merged and continues
        ; if not contiguous leave first two items and continue (rest)
        (let [t2 (first more)]
            (if (= (:end t1) (:start t2))
              (recur (cons (assoc t1 :end (:end t2)) (rest more))) ;replace the first item with the merged first two items
              (cons t1 (merge-ranges more))))))
   
 
(defn unpack-relation
  [relation]
  ; break into individual values
  (let [values (for [tuple relation value (:values tuple)] (assoc tuple :values #{value}))
        ranges-by-value (group-by (comp first :values) values)
        merged-ranges (map merge-ranges (vals ranges-by-value))]
    ; Note: not returning a relation, should I?
    (flatten merged-ranges)))
 
(defn all-past
  [n relation]
  (pack-relation
    (map
      #(assoc % :start (+ (:start %) n))
      (filter
        #(> (- (:end %) (:start %)) n)
        (unpack-relation relation)))))
 
(defn any-past
  [n relation]
  (pack-relation
    (map
      #(assoc % :end (+ (:end %) n))
      (unpack-relation relation))))
 
(defn all-future
  [n relation]
  (pack-relation
    (map
      #(assoc % :end (- (:end %) n))
      (filter
        #(> (- (:end %) (:start %)) n)
        (unpack-relation relation)))))
 
(defn any-future
  [n relation]
  (pack-relation
    (map
      #(assoc % :start (max (- (:start %) n) 0))
      (unpack-relation relation))))
 
; Monad operations ------------------------------------------------------------
(defn bind
  [relation f]
  ; f is a function that takes a value and returns a temporal relation
  ; apply f to each value in each tuple in the source relation to get a mapping
  ; from t1 to x (see paper)
  (pack-relation
    (for [t relation v (:values t) x-tup (f v)]
      ; now we need to compute the final t (which is the sum of the times from the original relation and the F(y))
      (let [adjusted-start (+ (:start x-tup) (:start t))
            adjusted-end (+ (:end x-tup) (:end t))] ; TODO: don't think this is right
          (assoc x-tup :start adjusted-start :end adjusted-end)))))
 
   
(defn return
  [v]
  (temporal-relation [{:start 0 :end 1 :values #{v}}]))
