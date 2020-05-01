import pandas as pd
import matplotlib.pyplot as plt
from pymongo import MongoClient

# Connection to Local MongoDB
client = MongoClient(port=27017)
db = client.youtube_trend


def read_trend_data(country):
    if country == 'India':
        data_collection = db.inVideos
        category_collection = db.inCategory
    elif country == 'Canada':
        data_collection = db.caVideos
        category_collection = db.caCategory
    elif country == 'USA':
        data_collection = db.usVideos
        category_collection = db.usCategory
    elif country == 'France':
        data_collection = db.frVideos
        category_collection = db.frCategory

    result = data_collection.aggregate([
        {
            "$group": {
                "_id": "$category_id",
                "count": {"$sum": 1},
                "total_views": {"$sum": "$views"},
                "total_comments": {"$sum": "$comment_count"},
                "total_likes": {"$sum": "$likes"},
                "total_dislikes": {"$sum": "$dislikes"}
            }
        },
        {
            "$sort": {
                "count": -1
            }
        }
        #{
        #	"$limit": 5
        #}
    ])

    data_df = pd.DataFrame(list(result))

    inCategoryResult = category_collection.aggregate([
        {
            "$project": {
                "_id": 0,
                "id": "$id",
                "title": "$snippet.title"
            }
        }
    ])

    cat_df = pd.DataFrame(list(inCategoryResult))

    cat_df['id'] = cat_df['id'].astype(str).astype(int)

    final_df = pd.merge(left=data_df, right=cat_df, left_on='_id', right_on='id')
    final_df['country'] = country
    final_df['ratio'] = final_df['total_likes'] / final_df['total_dislikes']
    final_df['controratio'] = final_df['total_comments'] / final_df['total_likes']

    return final_df

if __name__ == '__main__':
    India_data_df = read_trend_data('India')
    Usa_data_df = read_trend_data('USA')
    Canada_data_df = read_trend_data('Canada')
    France_data_df = read_trend_data('France')

    #India_data = India_data[['title', 'count','country']]
    #print(India_data)
    #India_data.plot.bar(x='title', y='count');
    #plt.show()
    #final_df = pd.concat([India_data_df, Usa_data_df, Canada_data_df, France_data_df], axis=0)
    #print(final_df.head(100))

    option=0 
    while (option != 10):
      print("Available options:")
      print("1. Top 5 trending categories")
      print("2. Likes/dislikes ratio in each category")
      print("3. The Category people have expressed most and less opinions ")
      print("4. Most controversial videos based on highest comments and few likes")
      print("10. EXIT")
      option = int(input())

      if option == 1: 
        #Top 5 trending categories
        trend_df_in=India_data_df.nlargest(5,'count')
        trend_df_usa = Usa_data_df.nlargest(5, 'count')
        trend_df_ca = Canada_data_df.nlargest(5, 'count')
        trend_df_fr = France_data_df.nlargest(5, 'count')

        trend_final_df = pd.concat([trend_df_in, trend_df_usa, trend_df_ca, trend_df_fr], axis=0)

        trend_df_plot = trend_final_df[['title', 'count','country']]
        test = trend_df_plot.pivot(index='country',columns = 'title')
        #print(test.head(15))
        test.plot.bar();
        plt.title("Trending by category",fontdict=None, loc='center', pad=None)
        plt.show()
      elif option == 2:
        # Likes/dislikes ratio in each category
        likes_df_in = India_data_df.nlargest(5, 'ratio')
        likes_df_usa = Usa_data_df.nlargest(5, 'ratio')
        likes_df_ca = Canada_data_df.nlargest(5, 'ratio')
        likes_df_fr = France_data_df.nlargest(5, 'ratio')

        likes_final_df = pd.concat([likes_df_in, likes_df_usa, likes_df_ca, likes_df_fr], axis=0)

        likedislike_df = likes_final_df[['title', 'ratio', 'country']]
        test = likedislike_df.pivot(index='country', columns='title')
        #print(test.head(15))
        test.plot.bar();
        plt.title("Likes/Dislikes ratio for each category", fontdict=None, loc='center', pad=None)
        plt.show()

      elif option == 3:
        # In which category people have expressed their opinions most and less.
        comments_df_in = India_data_df.nlargest(5, 'total_comments')
        comments_df_usa = Usa_data_df.nlargest(5, 'total_comments')
        comments_df_ca = Canada_data_df.nlargest(5, 'total_comments')
        comments_df_fr = France_data_df.nlargest(5, 'total_comments')

        comments_final_df = pd.concat([comments_df_in, comments_df_usa, comments_df_ca, comments_df_fr], axis=0)

        comments_df = comments_final_df[['title', 'total_comments', 'country']]
        test = comments_df.pivot(index='country', columns='title')
        #print(test.head(15))
        test.plot.bar();
        plt.title("People expressed their opinions through comments", fontdict=None, loc='center', pad=None)
        plt.show()

      elif option == 4:
        # Most controversial videos based on highest comments and few likes. Some analysis on the reason behind controversy
        contro_df_in = India_data_df.nlargest(5, 'controratio')
        contro_df_usa = Usa_data_df.nlargest(5, 'controratio')
        contro_df_ca = Canada_data_df.nlargest(5, 'controratio')
        contro_df_fr = France_data_df.nlargest(5, 'controratio')

        contro_final_df = pd.concat([contro_df_in, contro_df_usa, contro_df_ca, contro_df_fr], axis=0)

        controversy_df = contro_final_df[['title', 'controratio', 'country']]
        test = controversy_df.pivot(index='country', columns='title')
        #print(test.head(15))
        test.plot.bar();
        plt.title("controversial videos based on highest comments and few likes", fontdict=None, loc='center', pad=None)
        plt.show()
      elif option == 10:
        break
