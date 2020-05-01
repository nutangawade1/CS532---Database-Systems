import pandas as pd
import matplotlib.pyplot as plt
from pymongo import MongoClient
from bson.json_util import dumps


def query1(video_keyword):
    resultkey = db.usVideos.find(
        {"title": {"$regex": ".*" + video_keyword + ".*"}},
        {"video_id": 1, "title": 1, "publish_time": 1, "views": 1, "likes": 1, "_id": 0}
    ).limit(10);
    # print the result:
    print("\n VIDEO SEARCHED BASED ON A KEYWORD ENTERED")

    from bson.json_util import dumps
    print(dumps(resultkey, indent=2))
    # for x in resultkey:
    #  print(x)


def query2():
    # result2 = db.inVideos.find({"category_id" :"28" },{"channe_title": 1}).limit(5).pretty();
    result1 = db.usVideos.find(
        {"category_id": 28},
        {"channel_title": 1, "title": 1, "views": 1, "_id": 0}
    ).sort("views", -1).limit(1);
    # print the result:
    print("\n POPULAR VIDEO OF A CHANNEL BASED ON VIEWS IN SCIENCE AND TECHNOLOGY CATEGORY")
    from bson.json_util import dumps
    print(dumps(result1, indent=2))


def query3():
    result2 = db.usVideos.find({
        "publish_time": {
            "$gte": "2018-01-01T00:00:00.000Z",
            "$lt": "2018-12-31T00:00:00.000Z"}
    },
        {"title": 1, "likes": 1, "publish_time": 1, "_id": 0}
    ).sort("likes", -1).limit(1);

    print("\n POPULAR VIDEO OF BASED ON LIKES in a YEAR OF 2018")
    '''
    for x in result2:
      print(x) 
    '''
    from bson.json_util import dumps
    print(dumps(result2, indent=2))


def query4(catg):
    print("\n CHANNELS AVAILABLE IN THE CATEGORY")
    result3 = db.usVideos.distinct("channel_title", {"category_id": catg})
    for x in result3:
        print(x)


def query5():
    print("\n MOST POPULAR NEWS CHANNEL")
    result_popular = db.usVideos.aggregate([
        {
            "$match": {"category_id": 25}
        },
        {
            "$group": {
                "_id": {"channel_title": "$channel_title", "video_id": "$video_id"},
                "count": {"$sum": 1},
                "max_views": {"$sum": "$views"},
                "max_likes": {"$sum": "$likes"},
            }
        },
        {
            "$sort": {"max_views": -1, "max_likes": -1}
        },
        {
            "$limit": 1
        }
    ])
    # test_df =  pd.DataFrame(result1,columns=['Channel Title','title','views'],dtype=)
    from bson.json_util import dumps
    print(dumps(result_popular, indent=2))
    # video_df = pd.DataFrame(list(result_popular))
    # video_df = pd.concat([video_df.drop(['_id'], axis=1), video_df['_id'].apply(pd.Series)],
    #                               axis=1)
    # print(video_df)


'''
def query6():
    result6 = db.usVideos.find(
      {},
      {"video_id" : 1, "title" : 1 , "dislikes" : 1 , "_id" : 0}
      ).min( {"dislikes" : 1000} ).hint( {"dislikes" : 1} ).limit(5);
    #print the result:
    print("\n VIDEO SEARCHED BASED ON A KEYWORD ENTERED")

    from bson.json_util import dumps
    print(dumps(result6, indent=2)) 
'''


def trend_month(country, category):
    if country == 'USA':
        data_collection = db.usVideos
        category_collection = db.usCategory

    result_monthly = data_collection.aggregate([
        {
            "$group": {
                "_id": {"category": "$category_id", "month": {"$substr": ["$trending_date", 6, 2]}},
                "count": {"$sum": 1},
            }
        }

    ])

    monthly_trend_df = pd.DataFrame(list(result_monthly))
    monthly_trend_df = pd.concat([monthly_trend_df.drop(['_id'], axis=1), monthly_trend_df['_id'].apply(pd.Series)],
                                 axis=1)

    filter = monthly_trend_df['category'] == category
    monthly_trend_df = monthly_trend_df[filter].sort_values('month')
    # print(monthly_trend_df)
    monthly_trend_df.plot.bar(x='month', y='count');
    plt.title("category trending month wise", fontdict=None, loc='center', pad=None)
    plt.show()


def trend_videos(country, category):
    if country == 'USA':
        data_collection = db.usVideos
        category_collection = db.usCategory

    in_category = category
    print(category)
    result_videos = data_collection.aggregate([
        {"$match": {"category_id": category}
         },
        {
            "$group": {
                "_id": {"category": "$category_id", "title": "$title"},
                # "count": {"$sum": 1},
                "total_views": {"$sum": "$views"}
            }
        },
        {
            "$sort": {
                "total_views": -1
            }
        },
        {
            "$limit": 5
        }
    ])
    video_df = pd.DataFrame(list(result_videos))
    video_df = pd.concat([video_df.drop(['_id'], axis=1), video_df['_id'].apply(pd.Series)],
                         axis=1)
    print(video_df)

def display_title(category):
    id=str(category)
    title = db.usCategory.find({"id": id}, {"snippet.title": 1, "_id": 0})
    print(dumps(title, indent=2))

if __name__ == '__main__':
    # Connection to Local MongoDB
    client = MongoClient(port=27017)
    db = client.youtube_trend
    option = 0
    while (option != 10):
        print("Available options:")
        print("1. VIDEO SEARCHED BASED ON A KEYWORD ENTERED")
        print("2. POPULAR VIDEO OF A CHANNEL BASED ON VIEWS IN SCIENCE AND TECHNOLOGY CATEGORY")
        print("3. MOST POPULAR VIDEO THAT WAS PUBLISHED IN THE YEAR 2018")
        print("4. VIEW CHANNELS AVAILABLE IN PARTICULAR CATEGORY")
        print("5. MOST POPULAR NEWS CHANNEL")
        # print("6. VIEW 5 VIDOES WHICH GOT ATLEST 1000 DISLIKES")
        print("6. VIEW CATEGORIES TRENDING MONTH WISE")
        print("7. TRENDING VIDEOS")
        print("8. DISPLAY TITLE OF THE CATEGORY WHEN NUMBER ENTERED")
        print("10. EXIT")
        option = int(input())

        if option == 1:
            print("Please enter the keyword to search the video")
            video_keyword = str(input())
            query1(video_keyword)
        elif option == 2:
            query2()
        elif option == 3:
            query3()
        elif option == 4:
            print("Please enter the category id to search the channel for")
            catg = int(input())
            query4(catg)
        elif option == 5:
            query5()
        # elif option == 6:
        # query6()
        elif option == 6:
            print("Please enter the category id")
            in_category = int(input())
            trend_month("USA", in_category)
        elif option == 7:
            print("Please enter the category id")
            in_category = int(input())
            trend_videos("USA", in_category)
        elif option == 8:
            print("Please enter the category id")
            in_category = int(input())
            display_title(in_category)
        elif option == 10:
            break
