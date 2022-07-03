import argparse, urllib, json
from datetime import datetime
from pymongo import InsertOne, MongoClient, GEOSPHERE
from pymongo.errors import (PyMongoError, BulkWriteError)
import numpy as np

parser = argparse.ArgumentParser(description='Bulk import GeoJSON file into MongoDB')
#parser.add_argument('-f', required=True, help='input file')
parser.add_argument('-s', default='localhost', help='target server name (default is localhost)')
parser.add_argument('-port', default='27017', help='server port (default is 27017)')
#parser.add_argument('-d', required=True, help='target database name')
#parser.add_argument('-c', required=True, help='target collection to insert to')
parser.add_argument('-u', help='username (optional)')
parser.add_argument('-p', help='password (optional)')
args = parser.parse_args()

inputfile = ["impianti_sportivi.geojson", "musei.geojson", "parcheggi.geojson", "wifi_hotspot.geojson", "aree-verdi.geojson"]
to_collection = ["impiantiSportivi", "musei", "parcheggi", "wifiHotspots", "areeVerdi"]
to_database = "sistemiContextAware_db"

#inputfile = args.f
#to_collection = args.c
#to_database = args.d
to_server = args.s
to_port = args.port
db_user = args.u

if db_user is None:
  uri = 'mongodb://' + to_server + ':' + to_port +'/'
else:
  db_password = urllib.quote_plus(args.p)
  uri = 'mongodb://' + db_user + ':' + db_password + '@' + to_server + ':' + to_port +'/' + to_database

i=0
while i < len(inputfile):
  with open(inputfile[i],'r') as f:
    geojson = json.loads(f.read())

  client = MongoClient(uri)
  db = client[to_database]
  collection = db[to_collection[i]]

  collection.create_index([("geometry", GEOSPHERE)])

  for feature in geojson['features']:
    feature["properties"]["rank"] = np.random.randint(0, 11)
    collection.bulk_write([
      InsertOne(feature)
    ], ordered=False)
    

  print(inputfile[i], ' DONE.')
  i+=1