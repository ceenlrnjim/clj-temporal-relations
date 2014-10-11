(ns temprel-tests
  (:use temprel)
  (:use clojure.test))
 
(deftest test-overlap
  (is (overlap? {:start 0 :end 10} {:start 5 :end 15}))
  (is (overlap? {:start 5 :end 15} {:start 0 :end 10}))
  (is (not (overlap? {:start 0 :end 10} {:start 10 :end 15})))
  (is (overlap? {:start 5 :end 15} {:start 5 :end 10}))
  (is (not (overlap? {:start 10 :end 20} {:start 0 :end 10})))
  (is (not (overlap? {:start 0 :end 10} {:start 10 :end 20}))))
 
(deftest test-end-state
  (let [states [{:start 1 :end infinity :state :a}
                {:start 50 :end infinity :state :b}
                {:start 1 :end infinity :state :c}]
        result (end-state states :a 5)
        updated-a (first (filter #(= (:state %) :a) result))]
    (is (= (:end updated-a) 6))))
 
(deftest test-end-states
  (let [states [{:start 1 :end infinity :state :a}
                {:start 50 :end infinity :state :b}
                {:start 1 :end infinity :state :c}]
        result (end-states states [:a :c] 5)
        updated-a (first (filter #(= (:state %) :a) result))
        updated-c (first (filter #(= (:state %) :c) result))]
    (is (= (:end updated-a) 6))
    (is (= (:end updated-c) 6))))
 
 
 
(defn ranges-equal?
  [m1 m2]
  (and (= (:start m1) (:start m2)) (= (:end m1) (:end m2))))
 
(deftest test-distinct-ranges
  (let  [ranges [{:start 0 :end 100}
                 {:start 0 :end 100}
                 {:start 50 :end 60}
                 {:start 0 :end 10}
                 {:start 10 :end 20}
                 {:start 15 :end 20}
                 {:start 75 :end 100}]
         results (distinct-ranges ranges)]
    (is (ranges-equal? (first results) {:start 0 :end 10}))
    (is (ranges-equal? (first (drop 1 results)) {:start 10 :end 15}))
    (is (ranges-equal? (first (drop 2 results)) {:start 15 :end 20}))
    (is (ranges-equal? (first (drop 3 results)) {:start 20 :end 50}))
    (is (ranges-equal? (first (drop 4 results)) {:start 50 :end 60}))
    (is (ranges-equal? (first (drop 5 results)) {:start 60 :end 75}))
    (is (ranges-equal? (first (drop 6 results)) {:start 75 :end 100}))
    (is (= (count results) 7))
    ))
 
(deftest test-stateful-start
  (let [evts (from-timed-list [:a :b :c :d :e])
        result (stateful (fn [evt] [(str "state_" evt)])
                         (fn [state evt] [state]) ; not doing anything in transform, just propagating
                         evts)]
    (is (= (first result) {:start 0 :end 1 :values #{"state_:a"}}))
    (is (= (first (drop 1 result)) {:start 1 :end 2 :values #{"state_:a" "state_:b"}}))
    (is (= (first (drop 2 result)) {:start 2 :end 3 :values #{"state_:a" "state_:b" "state_:c"}}))
    (is (= (first (drop 3 result)) {:start 3 :end 4 :values #{"state_:a" "state_:b" "state_:c" "state_:d"}}))
    (is (= (first (drop 4 result)) {:start 4 :end infinity :values #{"state_:a" "state_:b" "state_:c" "state_:d" "state_:e"}}))
    ))
 
(deftest test-stateful-end
  (let [evts (from-timed-list [:a :b :c :d :e])
        result (stateful (fn [evt] (if (= evt :b) ["stateB"] [])) ; start B at event :b
                         (fn [state evt]
                            (if
                              (and (= evt :d) (= state "stateB")) [] [state]))
                         evts)] ; end B at event :d
    (is (= (first result) {:start 1 :end 4 :values #{"stateB"}}))
    (is (= (count result) 1))))
 
(deftest test-stateful-change
  (let [evts (from-timed-list [:a :b :c :d :e])
        result (stateful (fn [evt] (if (= evt :b) ["stateB"] []))
                         (fn [state evt]
                            (if (and (= evt :d) (= state "stateB"))
                              ["stateBtoD"]
                              [state]))
                          evts)]
    ; Note: I believe this is correct based on the definition of transform - It+1 is where the state is removed
    (is (= (first result) {:start 1 :end 3 :values #{"stateB"}}))
    (is (= (second result) {:start 3 :end 4 :values #{"stateB", "stateBtoD"}}))
    (is (= (first (drop 2 result)) {:start 4 :end infinity :values #{"stateBtoD"}}))
    (is (= (count result) 3))
    ))
 
(deftest test-product-events
  (let [e1 (from-timed-list [:a :b :c :d])
        e2 (from-timed-list [1 2 3 4])
        [a b c d] (vec (product e1 e2))]
    (is (= (first (:values a)) #{:a 1}))
    (is (= (:start a) 0))
    (is (= (:end a) 1))
    (is (= (first (:values b)) #{:b 2}))
    (is (= (:start b) 1))
    (is (= (:end b) 2))
    ))
 
(deftest test-product-ranges
  (let [r1 (temporal-relation [{:start 0 :end 10 :values #{:a}} {:start 10 :end 20 :values #{:b}}])
        r2 (temporal-relation [{:start 5 :end 15 :values #{:c}}])
        [a b] (vec (product r1 r2))]
    (is (= (first (:values a)) #{:a :c}))
    (is (= [(:start a) (:end a)] [5 10]))
    (is (= (first (:values b)) #{:b :c}))
    (is (= [(:start b) (:end b)] [10 15]))
  ))
 
(deftest test-product-multiple-values
)
 
(deftest test-multiple-product-ranges
  (let [r1 (temporal-relation [{:start 0 :end 10 :values #{:a}} {:start 10 :end 20 :values #{:b}}])
        r2 (temporal-relation [{:start 5 :end 8 :values #{:c}} {:start 8 :end 22 :values #{:d}}])
        [a b c] (vec (product r1 r2))]
    (is (= (first (:values a)) #{:a :c}))
    (is (= [(:start a) (:end a)] [5 8]))
    (is (= (first (:values b)) #{:a :d}))
    (is (= [(:start b) (:end b)] [8 10]))
    (is (= (first (:values c)) #{:b :d}))
    (is (= [(:start c) (:end c)] [10 20]))
    ))
 
(deftest test-union-no-overlap
  (let [r1 (temporal-relation [{:start 0 :end 10 :values #{:a}} {:start 10 :end 20 :values #{:b}}])
        r2 (temporal-relation [{:start 20 :end 30 :values #{:c}}])
        [a b c] (vec (union r1 r2))]
    (is (= (:values a) #{:a}))
    (is (= [(:start a) (:end a)] [0 10]))
    (is (= (:values b) #{:b}))
    (is (= [(:start b) (:end b)] [10 20]))
    (is (= (:values c) #{:c}))
    (is (= [(:start c) (:end c)] [20 30]))
    ))
 
(deftest test-union-no-overlap-duplicate-values
  (let [r1 (temporal-relation [{:start 0 :end 10 :values #{:a}} {:start 10 :end 20 :values #{:b}}])
        r2 (temporal-relation [{:start 5 :end 20 :values #{:c}} {:start 20 :end 30 :values #{:a}}])
        [a b c d] (vec (union r1 r2))]
    (is (= (:values a) #{:a}))
    (is (= [(:start a) (:end a)] [0 5]))
    (is (= (:values b) #{:a :c}))
    (is (= [(:start b) (:end b)] [5 10]))
    (is (= (:values c) #{:c :b}))
    (is (= [(:start c) (:end c)] [10 20]))
    (is (= (:values d) #{:a}))
    (is (= [(:start d) (:end d)] [20 30]))
    ))
 
 
(deftest test-filter-relation-single-value
  (let [r1 (temporal-relation [{:start 0 :end 10 :values #{:a}} {:start 10 :end 20 :values #{:b}}])
        result (vec (filter-relation #(= % :b) r1))
        [b] result]
    (is (= (count result) 1))
    (is (= (:values b) #{:b}))
    (is (= [(:start b) (:end b)] [10 20]))
    ))
 
(deftest test-filter-relation-multiple-values
  (let [r1 (temporal-relation [{:start 0 :end 10 :values #{:a :b}} {:start 10 :end 20 :values #{:d :c}}])
        result (vec (filter-relation #(= % :b) r1))
        [b] result]
    (is (= (count result) 1))
    (is (= (:values b) #{:b}))
    (is (= [(:start b) (:end b)] [0 10]))
    ))
 
(deftest test-map-relation
   (let [r1 (temporal-relation [{:start 0 :end 10 :values #{:a :b}} {:start 10 :end 20 :values #{:d :c}}])
        result (map-relation #(.toUpperCase (str %)) r1)
        [a b] (vec result)]
    (is (= (count result) 2))
    (is (= (:values a) #{":A" ":B"}))
    (is (= [(:start a) (:end a)] [0 10]))
    (is (= (:values b) #{":C" ":D"}))
    (is (= [(:start b) (:end b)] [10 20]))
    ))
 
(defn range-is?
  [tuples s e v n]
  (let [{start :start end :end} (nth (filter #(= (first (:values %)) v) tuples) n)]
    (and (= s start) (= e end))))
 
(deftest test-unpack-relation
  (let [r1 (temporal-relation [{:start 0 :end 5 :values #{:a}}
                              {:start 5 :end 10 :values #{:a :b}}
                              {:start 10 :end 15 :values #{:b}}
                              {:start 15 :end 20 :values #{:a :c}}])
        unpacked (unpack-relation r1)]
    (is (range-is? unpacked 0 10 :a 0))
    (is (range-is? unpacked 15 20 :a 1))
    (is (range-is? unpacked 5 15 :b 0))
    (is (range-is? unpacked 15 20 :c 0))
    ))
 
(deftest test-all-past-single
  (let [r1 (temporal-relation [{:start 10 :end 20 :values #{:a}} {:start 25 :end 35 :values #{:b}}])]
    (is (= (first (all-past 5 r1)) {:start 15 :end 20 :values #{:a}}))
    (is (= (second (all-past 5 r1)) {:start 30 :end 35 :values #{:b}}))
    ))
 
(deftest test-all-past-overlaps
  (let [r1 (temporal-relation [{:start 0 :end 5 :values #{:a}}
                              {:start 5 :end 10 :values #{:a :b}}
                              {:start 10 :end 15 :values #{:b}}
                              {:start 15 :end 20 :values #{:a :c}}])
        result (all-past 3 r1)]
    (is (= (first result) {:start 3 :end 8 :values #{:a}}))
    (is (= (first (drop 1 result)) {:start 8 :end 10 :values #{:a :b}}))
    (is (= (first (drop 2 result)) {:start 10 :end 15 :values #{:b}}))
    (is (= (first (drop 3 result)) {:start 18 :end 20 :values #{:a :c}}))
    ))
 
(deftest test-any-past-single
  (let [r1 (temporal-relation [{:start 10 :end 20 :values #{:a}} {:start 25 :end 35 :values #{:b}}])]
    (is (= (first (any-past 5 r1)) {:start 10 :end 25 :values #{:a}}))
    (is (= (second (any-past 5 r1)) {:start 25 :end 40 :values #{:b}}))
    ))
 
(deftest test-all-future-single
  (let [r1 (temporal-relation [{:start 10 :end 20 :values #{:a}} {:start 25 :end 35 :values #{:b}}])]
    (is (= (first (all-future 5 r1)) {:start 10 :end 15 :values #{:a}}))
    (is (= (second (all-future 5 r1)) {:start 25 :end 30 :values #{:b}}))
    ))
 
(deftest test-any-future-single
  (let [r1 (temporal-relation [{:start 10 :end 20 :values #{:a}} {:start 25 :end 35 :values #{:b}}])]
    (is (= (first (any-future 5 r1)) {:start 5 :end 20 :values #{:a}}))
    (is (= (second (any-future 5 r1)) {:start 20 :end 35 :values #{:b}}))
    ))
 
(deftest test-bind
  (let [R (temporal-relation [{:start 0 :end 10 :values #{:a :b}} {:start 10 :end 20 :values #{:a :c}}])
        f_y (fn [v] (temporal-relation [{:start 5 :end 6 :values #{(str v)}}]))
        result (bind R f_y)]
    ;t0  R :a
    ;t10 R :a
    ;t5  f_y ":a"
    ;t6  f_y ":a"
    ;t5  result ":a"
    ;t15 result ":a"
    (is (contains? (:values (first (states-at-time result 5))) ":a"))
    (is (contains? (:values (first (states-at-time result 5))) ":b"))
    (is (contains? (:values (first (states-at-time result 14))) ":a"))
    (is (contains? (:values (first (states-at-time result 14))) ":b"))
    (is (not (contains? (:values (first (states-at-time result 14))) ":c")))))
       
 
(run-tests)
