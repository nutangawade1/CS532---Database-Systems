import pandas as pd
import matplotlib.pyplot as plt
from pymongo import MongoClient

# Connection to Local MongoDB
client = MongoClient(port=27017)
db = client.youtube_trend

#Extract the needed data
result = db.inVideos.aggregate(
	[{
		"$group" : {
			"_id":"$category_id",
			"count":{"$sum":1},
			"total_views" : {"$sum" : "$views"},
			"total_comments" : {"$sum" : "$comment_count"},
			"total_likes" : {"$sum" : "$likes"},
			"total_dislikes" : {"$sum" : "$dislikes"}
			}
	},
	{
		"$sort": {
			"count": -1
		}
	}#,
	#{
	#	"$limit": 5
	#}
])

data_df =  pd.DataFrame(list(result))

#print(data_df.head())

inCategoryResult = db.inCategory.aggregate([
	{
		"$project" : {
			"_id" : 0,
			"id":"$id",
			"title":"$snippet.title"
			}
	}
])

cat_df = pd.DataFrame(list(inCategoryResult))

cat_df['id'] = cat_df['id'].astype(str).astype(int)

# 5
# Top 5 categories which are trending
toptrend_df = pd.merge(left=data_df, right=cat_df, left_on='_id', right_on='id')
toptrend_df = toptrend_df[['title', 'count']]

print(toptrend_df)
toptrend_df.plot.bar(x='title', y= 'count');
plt.show()

#6
#Likes/dislikes ratio in each category
likedislike_df = pd.merge(left=data_df, right=cat_df, left_on='_id', right_on='id')
likedislike_df['ratio'] = likedislike_df['total_likes']/ likedislike_df['total_dislikes']
likedislike_df = likedislike_df[['title','ratio']]
likedislike_df.plot.bar(x='title', y= 'ratio');
plt.show()

#7
#In which category people have expressed their opinions most and less.
comments_df = pd.merge(left=data_df, right=cat_df, left_on='_id', right_on='id')
comments_df = comments_df[['title','total_comments']]
#print(comments_df)
comments_df.plot.bar(x='title', y= 'total_comments');
plt.show()

#8
#Most controversial videos based on highest comments and few likes. Some analysis on the reason behind controversy
controversy_df = pd.merge(left=data_df, right=cat_df, left_on='_id', right_on='id')
controversy_df['ratio'] = controversy_df['total_comments']/ controversy_df['total_likes']
controversy_df = controversy_df[['title','ratio']]
controversy_df.plot.bar(x='title', y= 'ratio');
plt.show()


