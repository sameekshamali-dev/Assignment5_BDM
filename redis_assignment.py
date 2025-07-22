import redis
import codecs
import csv
from traceback import print_stack
from redis.commands.search.field import TextField, NumericField, TagField
from redis.commands.search.indexDefinition import IndexDefinition
from redis.commands.search.query import Query

REDISEARCH_AVAILABLE = True

class Redis_Client():
    redis = None

    def connect(self):
        try:
            self.redis = redis.Redis(
                host='localhost',
                port=6379,
                db=0,
                decode_responses=True
            )
            self.redis.ping()
            print("Connected to Redis.")
        except Exception as e:
            print(f"Connection failed: {e}")
            print_stack()

    def load_users(self, file):
        result = 0
        try:
            with codecs.open(file, 'r', 'utf-8') as f:
                lines = f.read().strip().split('\n')

            pipe = self.redis.pipeline()

            for line in lines:
                parts = []
                current = ""
                in_quotes = False
                for c in line:
                    if c == '"':
                        in_quotes = not in_quotes
                        if not in_quotes:
                            parts.append(current)
                            current = ""
                    elif c == ' ' and not in_quotes:
                        if current:
                            parts.append(current)
                            current = ""
                    else:
                        current += c
                if current:
                    parts.append(current)

                if len(parts) >= 22:
                    user_id = parts[0]
                    user_data = {
                        'first_name': parts[2],
                        'last_name': parts[4],
                        'email': parts[6],
                        'gender': parts[8],
                        'ip_address': parts[10],
                        'country': parts[12],
                        'country_code': parts[14],
                        'city': parts[16],
                        'longitude': parts[18],
                        'latitude': parts[20],
                        'last_login': parts[22] if len(parts) > 22 else parts[21]
                    }
                    pipe.hset(user_id, mapping=user_data)
                    result += 1

            pipe.execute()
            print(f"Loaded {result} users.")
            return result
        except Exception as e:
            print(f"Error: {e}")
            print_stack()
            return 0

    def load_scores(self):
        result_count = 0
        try:
            pipe = self.redis.pipeline()
            with codecs.open('userscores.csv', 'r', 'utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    user_id = row['user:id']
                    score = float(row['score'])
                    leaderboard_key = f"leaderboard:{row['leaderboard']}"
                    pipe.zadd(leaderboard_key, {user_id: score})
                    result_count += 1
            pipe.execute()
            print(f"Loaded {result_count} scores.")
            return result_count
        except Exception as e:
            print(f"Error: {e}")
            print_stack()
            return 0

    def query1(self, usr):
        print("Executing query1: get all attributes.")
        try:
            result = self.redis.hgetall(f"user:{usr}")
            print(result)
            return result
        except Exception as e:
            print(f"Error: {e}")
            return None

    def query2(self, usr):
        print("Executing query2: get coordinates.")
        try:
            coords = self.redis.hmget(f"user:{usr}", 'longitude', 'latitude')
            result = {'longitude': coords[0], 'latitude': coords[1]} if coords[0] and coords[1] else None
            print(result)
            return result
        except Exception as e:
            print(f"Error: {e}")
            return None

    def query3(self):
        print("Executing query3: users with even ID.")
        try:
            userids = []
            lastnames = []
            cursor = 1280
            while True:
                cursor, keys = self.redis.scan(cursor=cursor, match="user:*", count=10)
                for key in keys:
                    uid = key.split(":")[1]
                    if uid[0] in ['0', '2', '4', '6', '8']:
                        lname = self.redis.hget(key, 'last_name')
                        if lname:
                            userids.append(key)
                            lastnames.append(lname)
                if cursor == 0:
                    break
            print(userids[:10], lastnames[:10])
            return userids, lastnames
        except Exception as e:
            print(f"Error: {e}")
            return [], []

    def create_index(self):
        try:
            self.redis.ft("user_index").create_index([
                TextField("first_name"),
                TagField("gender"),
                TagField("country"),
                NumericField("latitude")
            ], definition=IndexDefinition(prefix=["user:"]))
            print("RediSearch index created.")
        except Exception as e:
            print(f"Index creation failed: {e}")

    def query4(self):
        print("Executing query4: females in CN or RU with latitude 40–46.")
        try:
            if REDISEARCH_AVAILABLE:
                self.create_index()
                q = Query("@gender:{female} (@country:{China}|@country:{Russia}) @latitude:[40 46]")
                res = self.redis.ft("user_index").search(q)
                for doc in res.docs:
                    print(doc.id, doc.first_name, doc.last_name, doc.country, doc.latitude)
                return res.docs
            else:
                print("RediSearch not available.")
                return []
        except Exception as e:
            print(f"Error: {e}")
            return []

    def query5(self):
        print("Executing query5: top 10 scores from leaderboard:2.")
        try:
            users = self.redis.zrevrange("leaderboard:2", 0, 9, withscores=True)
            result = []
            for uid, score in users:
                email = self.redis.hget(uid, 'email')
                if email:
                    result.append(email)
                    print(f"{uid}: {email} (score: {score})")
            return result
        except Exception as e:
            print(f"Error: {e}")
            return []

if __name__ == "__main__":
    rs = Redis_Client()
    rs.connect()
    rs.load_users("users.txt")
    rs.load_scores()
    rs.query1(1)
    rs.query2(1)
    rs.query3()
    rs.query4()
    rs.query5()
