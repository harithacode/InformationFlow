import ast
import json
import sys
from pytz import timezone
import pyodbc
from datetime import datetime
from pyodbc import IntegrityError

#the following script is used to feed tweets into data base. Tweets are to be read from a file named "infile.txt".

def tweetParse(pyobj):
	tweet_id = pyobj['id_str'] #text
	coordinates = pyobj['coordinates'] #array, nullable
	if(str(coordinates)=='None'):
		latitude = None
		longitude = None
		geo_type = None
	else:
		latitude = pyobj['coordinates']['coordinates'][1]
		longitude = pyobj['coordinates']['coordinates'][0]
		geo_type = pyobj['coordinates']['type']
	created_at = datetime.strptime(pyobj['created_at'], '%a %b %d %H:%M:%S +0000 %Y')
	utc_created_at = utc.localize(created_at)
	tweetTime = utc_created_at.strftime('%Y-%m-%d %H:%M:%S')
	favorite_count = pyobj['favorite_count'] #integer
	in_reply_to_status_id = pyobj['in_reply_to_status_id_str'] #text,nullable
	in_reply_to_screen_name = pyobj['in_reply_to_screen_name'] #text,nullable
	in_reply_user_id = pyobj['in_reply_to_user_id_str'] #text,nullable
	tweet_language = pyobj['lang']
	if(pyobj['place']!=None):
		origin_place_id = pyobj['place']['id']
		origin_place_name = pyobj['place']['full_name'].encode("utf-8",errors="replace")
		origin_place_type = pyobj['place']['place_type']
	else:
		origin_place_id = None
		origin_place_name = None
		origin_place_type = None

	if(pyobj.has_key('possibly_sensitive'))	:
		possibly_sensitive = str(pyobj['possibly_sensitive']) #boolean
	else:
		possibly_sensitive = None

	retweet_count = pyobj['retweet_count']
	if(pyobj.has_key('retweeted_status') and pyobj['retweeted_status']!=None):
		retweeted_status_id = pyobj['retweeted_status']['id_str']
		retweeted_status_user_id = pyobj['retweeted_status']['user']['id_str']
		retweet_created_at = datetime.strptime(pyobj['retweeted_status']['created_at'], '%a %b %d %H:%M:%S +0000 %Y')
		retweet_utc_created_at = utc.localize(retweet_created_at)
		retweeted_status_created_at = retweet_utc_created_at.strftime('%Y-%m-%d %H:%M:%S')
	else:
		retweeted_status_id = None
		retweeted_status_user_id = None
		retweeted_status_created_at = None

	tweet_text = pyobj['text'].encode("utf-8",errors="replace") #text
	user_id = pyobj['user']['id_str']
	screen_name = pyobj['user']['screen_name'].encode("utf-8",errors="replace")
	follower_count = pyobj['user']['followers_count']	
	friends_count = pyobj['user']['friends_count']
	user_protected = str(pyobj['user']['protected']) #boolean

	if(pyobj['user']['name']!=None):
		user_name = pyobj['user']['name'].encode("utf-8",errors="replace")
	else:
		user_name = None

	if(pyobj['user']['description']!=None):
		description = pyobj['user']['description'].encode("utf-8",errors="replace")
	else:
		description = None
	
	if(pyobj['user']['location']!=None):
		user_location = pyobj['user']['location'].encode("utf-8",errors="replace")
	else:
		user_location = None

	verified = str(pyobj['user']['verified']) #boolean
	
	if(pyobj['user']['url']!=None):
		user_url = pyobj['user']['url'].encode("utf-8",errors="replace")
	else:
		user_url = None

	geo_enabled = str(pyobj['user']['geo_enabled']) #boolean
	time_zone = pyobj['user']['time_zone']
	user_created_time = datetime.strptime(pyobj['user']['created_at'], '%a %b %d %H:%M:%S +0000 %Y')
	user_created_utc = utc.localize(user_created_time)
	user_created_at = user_created_utc.strftime('%Y-%m-%d %H:%M:%S')
	statuses_count = pyobj['user']['statuses_count']
	listed_count = pyobj['user']['listed_count']
	favorites_count = pyobj['user']['favourites_count']
	try:
		cursor.execute("insert into tweets(tweet_id,longitude,latitude,geo_type,created_at,favorite_count,in_reply_to_status_id,in_reply_to_user_id,in_reply_to_screen_name,tweet_language,origin_place_id,origin_place_name,origin_place_type,possibly_sensitive,retweet_count,retweeted_status_id,retweeted_status_user_id,retweeted_status_created_at,tweet_text,user_id,screen_name,follower_count,friends_count,user_protected) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",tweet_id,longitude,latitude,geo_type,tweetTime,favorite_count,in_reply_to_status_id,in_reply_user_id,in_reply_to_screen_name,tweet_language,origin_place_id,origin_place_name,origin_place_type,possibly_sensitive,retweet_count,retweeted_status_id,retweeted_status_user_id,retweeted_status_created_at,tweet_text,user_id,screen_name,follower_count,friends_count,user_protected)
		for currentURL in pyobj['entities']['urls']:
			cursor.execute("insert into urlstable(url,tweet_id,user_id,created_at,user_friends_count,user_followers_count,tweet_location,user_location) values (?,?,?,?,?,?,?,?)",currentURL['url'],tweet_id,user_id,tweetTime,friends_count,follower_count,origin_place_name,user_location)	
		for currentMention in pyobj['entities']['user_mentions']:
			cursor.execute("insert into usermentions(mentioned_user_id,screen_name,user_name,tweet_id,user_id,created_at,user_friends_count,user_followers_count,tweet_location,user_location) values (?,?,?,?,?,?,?,?,?,?)",currentMention['id_str'],currentMention['screen_name'],currentMention['name'],tweet_id,user_id,tweetTime,friends_count,follower_count,origin_place_name,user_location)	
		for currentHashtag in pyobj['entities']['hashtags']:
			cursor.execute("insert into hashtags(hashtag,tweet_id,user_id,created_at,user_friends_count,user_followers_count,tweet_location,user_location) values(?,?,?,?,?,?,?,?)",currentHashtag['text'],tweet_id,user_id,tweetTime,friends_count,follower_count,origin_place_name,user_location)	
		cursor2.execute("select user_id from users where user_id=?",user_id)
		x = 0
		for row in cursor2:
			x = x+1
		if(x==0):
			cursor.execute("insert into users(user_id,user_name,screen_name,description,friends_count,followers_count,user_protected,user_location,verified,url,geo_enabled,time_zone,created_at,statuses_count,listed_count,favorites_count) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",user_id,user_name,screen_name,description,friends_count,follower_count,user_protected,user_location,verified,user_url,geo_enabled,time_zone,user_created_at,statuses_count,listed_count,favorites_count)
	
	except IntegrityError as e:
		print e
		print tweet_id


db_file = 'Tweets.mdb'
user = 'Admin'
password = 'haritha'

log = open('log.txt','a')

odbc_conn_str = 'DRIVER={Microsoft Access Driver (*.mdb)};DBQ=%s;UID=%s;PWD=%s' % \
                (db_file, user, password)

cnxn = pyodbc.connect(odbc_conn_str)
cursor = cnxn.cursor()
cursor2  = cnxn.cursor()
utc = timezone('UTC')
y=0;
with open("infile.txt") as f:
	for line in f:
		pyobj = ast.literal_eval(line)
		tweetParse(pyobj)
		y = y+1
		if(pyobj.has_key('retweeted_status') and pyobj['retweeted_status']!=None):
			x=0
			cursor2.execute("select tweet_id from tweets where tweet_id=?",pyobj['retweeted_status']['id_str'])
			for row in cursor2:
				x = x + 1
			if(x==0):
				tweetParse(pyobj['retweeted_status'])
		
		print y	
		if(y%1000==0):
			cnxn.commit()
			print 'commit'
			log.write(str(y))
			log.write('\n')

		
cnxn.commit()
cnxn.close()
f.close()
raw_input("Hi")

#extract common paramenters
# perform checks for primary keys