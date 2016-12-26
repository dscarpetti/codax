(ns codax.bench.users
  (:require
   [clojure.string :as str]
   [codax.core :refer :all]
   [codax.store :refer [open-database destroy-database]]
   [codax.swaps :refer :all]))


(def wordlist ["aardvark" "abyssinian" "accelerator" "accordion" "account" "accountant" "acknowledgment" "acoustic" "acrylic" "act" "action" "activity" "actor" "actress" "adapter" "addition" "address" "adjustment" "adult" "advantage" "advertisement" "aftermath" "afternoon" "aftershave" "afterthought" "age" "agenda" "agreement" "air" "airbus" "airmail" "airplane" "airport" "airship" "alarm" "albatross" "alcohol" "algebra" "algeria" "alibi" "alley" "alligator" "alloy" "almanac" "alphabet" "alto" "aluminium" "aluminum" "ambulance" "america" "amount" "amusement" "anatomy" "anethesiologist" "anger" "angle" "angora" "animal" "anime" "ankle" "answer" "ant" "anteater" "antelope" "anthony" "anthropology" "apartment" "apology" "apparatus" "apparel" "appeal" "appendix" "apple" "appliance" "approval" "april" "aquarius" "arch" "archaeology" "archeology" "archer" "architecture" "area" "argentina" "argument" "aries" "arithmetic" "arm" "armadillo" "armchair" "army" "arrow" "art" "ash" "ashtray" "asia" "asparagus" "asphalt" "asterisk" "astronomy" "athlete" "ATM" "atom" "attack" "attempt" "attention" "attic" "attraction" "august" "aunt" "australia" "australian" "author" "authority" "authorization" "avenue" "baboon" "baby" "back" "backbone" "bacon" "badge" "badger" "bag" "bagel" "bagpipe" "bail" "bait" "baker" "bakery" "balance" "balinese" "ball" "balloon" "bamboo" "banana" "band" "bandana" "bangle" "banjo" "bank" "bankbook" "banker" "bar" "barbara" "barber" "barge" "baritone" "barometer" "base" "baseball" "basement" "basin" "basket" "basketball" "bass" "bassoon" "bat" "bath" "bathroom" "bathtub" "battery" "battle" "bay" "beach" "bead" "beam" "bean" "bear" "beard" "beast" "beat" "beautician" "beauty" "beaver" "bed" "bedroom" "bee" "beech" "beef" "beer" "beet" "beetle" "beggar" "beginner" "begonia" "behavior" "belgian" "belief" "bell" "belt" "bench" "bengal" "beret" "berry" "bestseller" "betty" "bibliography" "bicycle" "bike" "bill" "billboard" "biology" "biplane" "birch" "bird" "birth" "birthday" "bit" "bite" "black" "bladder" "blade" "blanket" "blinker" "blizzard" "block" "blouse" "blow" "blowgun" "blue" "board" "boat" "bobcat" "body" "bolt" "bomb" "bomber" "bone" "bongo" "bonsai" "book" "bookcase" "booklet" "boot" "border" "botany" "bottle" "bottom" "boundary" "bow" "bowl" "box" "boy" "bra" "brace" "bracket" "brain" "brake" "branch" "brand" "brandy" "brass" "brazil" "bread" "break" "breakfast" "breath" "brian" "brick" "bridge" "british" "broccoli" "brochure" "broker" "bronze" "brother" "brother-in-law" "brow" "brown" "brush" "bubble" "bucket" "budget" "buffer" "buffet" "bugle" "building" "bulb" "bull" "bulldozer" "bumper" "bun" "burglar" "burma" "burn" "burst" "bus" "bush" "business" "butane" "butcher" "butter" "button" "buzzard" "cabbage" "cabinet" "cable" "cactus" "cafe" "cake" "calculator" "calculus" "calendar" "calf" "call" "camel" "camera" "camp" "can" "cancer" "candle" "cannon" "canoe" "canvas" "cap" "capital" "cappelletti" "capricorn" "captain" "caption" "car" "caravan" "carbon" "card" "cardboard" "cardigan" "care" "carnation" "carol" "carp" "carpenter" "carriage" "carrot" "cart" "cartoon" "case" "cast" "castanet" "cat" "catamaran" "caterpillar" "cathedral" "catsup" "cattle" "cauliflower" "cause" "caution" "cave" "c-clamp" "cd" "ceiling" "celery" "celeste" "cell" "cellar" "cello" "celsius" "cement" "cemetery" "cent" "centimeter" "century" "ceramic" "cereal" "certification" "chain" "chair" "chalk" "chance" "change" "channel" "character" "chard" "charles" "chauffeur" "check" "cheek" "cheese" "cheetah" "chef" "chemistry" "cheque" "cherry" "chess" "chest" "chick" "chicken" "chicory" "chief" "child" "children" "chill" "chime" "chimpanzee" "chin" "china" "chinese" "chive" "chocolate" "chord" "christmas" "christopher" "chronometer" "church" "cicada" "cinema" "circle" "circulation" "cirrus" "citizenship" "city" "clam" "clarinet" "class" "claus" "clave" "clef" "clerk" "click" "client" "climb" "clipper" "cloakroom" "clock" "close" "closet" "cloth" "cloud" "clover" "club" "clutch" "coach" "coal" "coast" "coat" "cobweb" "cockroach" "cocktail" "cocoa" "cod" "coffee" "coil" "coin" "coke" "cold" "collar" "college" "collision" "colombia" "colon" "colony" "color" "colt" "column" "columnist" "comb" "comfort" "comic" "comma" "command" "commission" "committee" "community" "company" "comparison" "competition" "competitor" "composer" "composition" "computer" "condition" "condor" "cone" "confirmation" "conga" "congo" "conifer" "connection" "consonant" "continent" "control" "cook" "copper" "copy" "copyright" "cord" "cork" "cormorant" "corn" "cornet" "correspondent" "cost" "cotton" "couch" "cougar" "cough" "country" "course" "court" "cousin" "cover" "cow" "cowbell" "crab" "crack" "cracker" "craftsman" "crate" "crawdad" "crayfish" "crayon" "cream" "creator" "creature" "credit" "creditor" "creek" "crib" "cricket" "crime" "criminal" "crocodile" "crocus" "croissant" "crook" "crop" "cross" "crow" "crowd" "crown" "crush" "cry" "cub" "cuban" "cucumber" "cultivator" "cup" "cupboard" "cupcake" "curler" "currency" "current" "curtain" "curve" "cushion" "custard" "customer" "cut" "cuticle" "cycle" "cyclone" "cylinder" "cymbal" "dad" "daffodil" "dahlia" "daisy" "damage" "dance" "dancer" "danger" "daniel" "dash" "dashboard" "database" "date" "daughter" "david" "day" "dead" "deadline" "deal" "death" "deborah" "debt" "debtor" "decade" "december" "decimal" "decision" "decrease" "dedication" "deer" "defense" "deficit" "degree" "delete" "delivery" "den" "denim" "dentist" "deodorant" "department" "deposit" "description" "desert" "design" "desire" "desk" "dessert" "destruction" "detail" "detective" "development" "dew" "diamond" "diaphragm" "dibble" "dictionary" "dietician" "difference" "digestion" "digger" "digital" "dill" "dime" "dimple" "dinghy" "dinner" "dinosaur" "diploma" "dipstick" "direction" "dirt" "disadvantage" "discovery" "discussion" "disease" "disgust" "dish" "distance" "distribution" "distributor" "division" "dock" "doctor" "dog" "dogsled" "doll" "dollar" "dolphin" "domain" "donald" "donkey" "donna" "door" "double" "doubt" "downtown" "dragon" "dragonfly" "drain" "drake" "drama" "draw" "drawbridge" "drawer" "dream" "dredger" "dress" "dresser" "drill" "drink" "drive" "driver" "drizzle" "drop" "drug" "drum" "dry" "dryer" "duck" "duckling" "dugout" "dungeon" "dust" "eagle" "ear" "earth" "earthquake" "ease" "edge" "edger" "editor" "editorial" "education" "edward" "eel" "effect" "egg" "eggnog" "eggplant" "egypt" "eight" "elbow" "element" "elephant" "elizabeth" "ellipse" "emery" "employee" "employer" "encyclopedia" "end" "enemy" "energy" "engine" "engineer" "english" "enquiry" "entrance" "environment" "epoch" "epoxy" "equinox" "equipment" "era" "error" "estimate" "ethernet" "ethiopia" "euphonium" "europe" "evening" "event" "examination" "example" "exchange" "exclamation" "exhaust" "ex-husband" "existence" "expansion" "experience" "expert" "explanation" "ex-wife" "eye" "eyebrow" "eyelash" "eyeliner" "face" "fact" "factory" "fahrenheit" "fall" "family" "fan" "fang" "farm" "farmer" "fat" "father" "father-in-law" "faucet" "fear" "feast" "feather" "feature" "february" "fedelini" "feedback" "feeling" "feet" "felony" "female" "fender" "ferry" "ferryboat" "fertilizer" "fiber" "fiberglass" "fibre" "fiction" "field" "fifth" "fight" "fighter" "file" "find" "fine" "finger" "fir" "fire" "fired" "fireman" "fireplace" "firewall" "fish" "fisherman" "flag" "flame" "flare" "flat" "flavor" "flax" "flesh" "flight" "flock" "flood" "floor" "flower" "flugelhorn" "flute" "fly" "foam" "fog" "fold" "font" "food" "foot" "football" "footnote" "force" "forecast" "forehead" "forest" "forgery" "fork" "form" "format" "fortnight" "foundation" "fountain" "fowl" "fox" "foxglove" "fragrance" "frame" "france" "freckle" "freeze" "freezer" "freighter" "french" "freon" "friction" "Friday" "fridge" "friend" "frog" "front" "frost" "frown" "fruit" "fuel" "fur" "furniture" "galley" "gallon" "game" "gander" "garage" "garden" "garlic" "gas" "gasoline" "gate" "gateway" "gauge" "gazelle" "gear" "gearshift" "geese" "gemini" "gender" "geography" "geology" "geometry" "george" "geranium" "german" "germany" "ghana" "ghost" "giant" "giraffe" "girdle" "girl" "gladiolus" "glass" "glider" "glockenspiel" "glove" "glue" "goal" "goat" "gold" "goldfish" "golf" "gondola" "gong" "good-bye" "goose" "gore-tex" "gorilla" "gosling" "government" "governor" "grade" "grain" "gram" "granddaughter" "grandfather" "grandmother" "grandson" "grape" "graphic" "grass" "grasshopper" "gray" "grease" "great-grandfather" "great-grandmother" "greece" "greek" "green" "grenade" "grey" "grill" "grip" "ground" "group" "grouse" "growth" "guarantee" "guatemalan" "guide" "guilty" "guitar" "gum" "gun" "gym" "gymnast" "hacksaw" "hail" "hair" "haircut" "half-brother" "half-sister" "halibut" "hall" "hallway" "hamburger" "hammer" "hamster" "hand" "handball" "handicap" "handle" "handsaw" "harbor" "hardboard" "hardcover" "hardhat" "hardware" "harmonica" "harmony" "harp" "hat" "hate" "hawk" "head" "headlight" "headline" "health" "hearing" "heart" "heat" "heaven" "hedge" "height" "helen" "helicopter" "helium" "hell" "helmet" "help" "hemp" "hen" "heron" "herring" "hexagon" "hill" "himalayan" "hip" "hippopotamus" "history" "hockey" "hoe" "hole" "holiday" "home" "honey" "hood" "hook" "hope" "horn" "horse" "hose" "hospital" "hot" "hour" "hourglass" "house" "hovercraft" "hub" "hubcap" "humidity" "humor" "hurricane" "hyacinth" "hydrant" "hydrofoil" "hydrogen" "hyena" "hygienic" "ice" "icebreaker" "icicle" "icon" "idea" "ikebana" "illegal" "imprisonment" "improvement" "impulse" "inch" "income" "increase" "index" "india" "indonesia" "industry" "ink" "innocent" "input" "insect" "instruction" "instrument" "insulation" "insurance" "interactive" "interest" "internet" "interviewer" "intestine" "invention" "inventory" "invoice" "iris" "iron" "island" "israel" "italian" "italy" "jacket" "jaguar" "jail" "jam" "james" "january" "japan" "japanese" "jar"])



(defmacro bench
  "Times the execution of forms, discarding their output and returning
a long in nanoseconds."
  ([name & forms]
   `(let [start# (System/nanoTime)
          result# (do ~@forms)]
       [~name result# (- (System/nanoTime) start#)])))

(defn assoc-user [db user-id]
  (bench
   "assoc user"
   (let [timestamp (System/nanoTime)
         test-keys (reduce #(assoc %1 (str %2) (str %2)) {} user-id)
         user {:id user-id
               :timestamp timestamp
               :test-keyset (set (map str user-id))
               :test-keys test-keys}]
     (with-write-transaction [db tx]
       (let [tx (if (get-at tx [:users user-id :id])
                  tx
                  (-> tx
                      (update-val [:metrics :user-counts :all] inc-count)
                      (update-val [:metrics :user-counts :starts-with (str (first user-id))] inc-count)))]
         (assoc-at tx [:users user-id] user)))
     user-id)))

(defn put-user [db user-id]
  (bench
   "put user"
   (let [timestamp (System/nanoTime)
         test-keys (reduce #(assoc %1 (str %2) (str %2)) {} user-id)
         user {:id user-id
               :timestamp timestamp
               :test-keyset (set (map str user-id))
               :test-keys test-keys}]
     (with-write-transaction [db tx]
       (let [tx (if (get-at tx [:users user-id :id])
                  tx
                  (-> tx
                      (update-at [:metrics :user-counts :all] inc-count)
                      (update-at [:metrics :user-counts :starts-with (str (first user-id))] inc-count)))]
         (put tx [:users user-id] user)))
     user-id)))

(defn put-val-user [db user-id]
  (bench
   "put val user"
   (with-write-transaction [db tx]
     (let [tx (if (get-at tx [:users user-id :id])
                  tx
                  (-> tx
                      (update-val [:metrics :user-counts :all] inc-count)
                      (update-val [:metrics :user-counts :starts-with (str (first user-id))] inc-count)))
           tx (reduce #(put-val %1 [:users user-id :test-keys (str %2)] (str %2)) tx user-id)]
       (-> tx
           (put-val [:users user-id :id] user-id)
           (put-val [:users user-id :timestamp] (System/nanoTime))
           (put-val [:users user-id :test-keyset] (set (map str user-id))))))
   user-id))

(defn dissoc-user [db user-id]
  (bench
   "delete user"
   (let [user-existed (with-read-transaction [db tx] (get-val tx [:users user-id :id]))]
     (if user-existed
       (with-write-transaction [db tx]
         (-> tx
             (update-at [:metrics :user-counts :all] dec-count)
             (update-at [:metrics :user-counts :starts-with (str (first user-id))] dec-count)
             (dissoc-at [:users user-id])))))))

(defn verify-user-data [db user-id]
  (bench
   "verify user"
   (with-read-transaction [db tx]
     (let [user (get-at tx [:users user-id])]
       (when user
         (if (not (= (set (keys (:test-keys user))) (:test-keyset user)))
           (throw (Exception. (str user-id ": user keys/keyset mismatch " user)))))
       [user-id (if (nil? user) "user not found" "user found")]))))

(defn verify-all-users [db]
  (bench
   "verify all users"
   (with-read-transaction [db tx]
     (let [users (get-at tx [:users])
           all-user-count (get-at tx [:metrics :user-counts :all])]
;       (println (count users) all-user-count)
       (if (and all-user-count (not (= (count users) all-user-count)))
         (throw (Exception. (str "count mismatch: " (count users) " " all-user-count "\n"))))))))

(defn compute-times [x]
  (into {}
        (map (fn [[type content]]
         (let [op-count (count content)
               total-time (/ (reduce #(+ %1 (nth %2 2)) 0 content) 1000000000.0)]
           [type {:op-count op-count
                  :total-seconds total-time
                  :ops-per-second (/ op-count total-time)}]))
       x)))

(defn create-user-operation-set [database write-count read-count verification-count]
  (let [;wordlist (take 250 wordlist)
        assoc-users (doall (map (fn [word] #(assoc-user database word)) (take write-count (shuffle wordlist))))
        put-users (doall (map (fn [word] #(put-user database word)) (take write-count (shuffle wordlist))))
        put-val-users (doall (map (fn [word] #(put-val-user database word)) (take write-count (shuffle wordlist))))
        dissoc-users (doall (map (fn [word] #(dissoc-user database word)) (take write-count (shuffle wordlist))))
        user-verification (repeatedly read-count #(fn [] (verify-user-data database (first (shuffle wordlist)))))
        all-users-verification (repeatedly verification-count #(fn [] (verify-all-users database)))]
    (shuffle (concat assoc-users put-users put-val-users user-verification dissoc-users all-users-verification))))


(defn run-user-test [& {:keys [no-cache writes reads verifications] :or {writes 50 reads 500 verifications 10}}]
  (open-database "data/BENCH_user_test")
  (destroy-database "data/BENCH_user_test")
  (let [database (open-database "data/BENCH_user_test")]
    (try
      (let [opset-1 (create-user-operation-set database writes reads verifications)
            opset-2 (create-user-operation-set database writes reads verifications)
            start-time (System/nanoTime)
            results-1 (doall (pmap #(%) opset-1))
            results-2 (doall (pmap #(%) opset-2))
            stop-time (System/nanoTime)
            results (concat results-1 results-2)
            results (group-by first results)
            verification-results (group-by (fn [x] (second (second x))) (get results "verify user"))
            results (merge (dissoc results "verify user") verification-results)
            total-time (compute-times results)]
        (clojure.pprint/pprint total-time)
        (verify-all-users database)
        {:clock-time (/ (- stop-time start-time) 1000000000.0)
         :stats total-time
         :cache (not no-cache)})
      (catch Throwable e (println (.getMessage e)) e))))

(defn increment-path [db]
  (with-write-transaction [db tx]
    (update-val tx [:metrics :user-counts :all] inc-count)))

(defn increment-test [n]
  (open-database "data/inc_test")
  (destroy-database "data/inc_test")
  (let [database (open-database "data/inc_test")
        ops (repeat n #(increment-path database))]
    (doall (pmap #(%) ops))
    (with-read-transaction [database tx]
      (get-at tx [:metrics :user-counts :all]))))
