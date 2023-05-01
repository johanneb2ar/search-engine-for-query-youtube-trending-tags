# %% [markdown]
# Prepare Data for Searching

# %%
from pyspark.conf import SparkConf
from pyspark import SparkContext
from pyspark.sql import SQLContext
sc = SparkContext() 
sc.stop()
config = sc.getConf()
config.set('spark.cores.max','4')
config.set('spark.executor.memory', '8G')
config.set('spark.driver.maxResultSize', '8g')
config.set('spark.kryoserializer.buffer.max', '512m')
config.set("spark.driver.cores", "4")
sc.stop()
sc = SparkContext(conf = config) 
sqlContext = SQLContext(sc)
print("Using Apache Spark Version", sc.version)

# %%
# Read the csv dataset into Spark Dataframe and output the rows in the dataframe
all_videos_file='youtube videos datasets/YouTubeVideos_trending_data.csv'
all_videos_sdf = sqlContext.read.option("header", "true") \
                         .option("delimiter", ",") \
                         .option("inferSchema", "true") \
                         .csv(all_videos_file)
all_videos_sdf.count()

# %%
# Filter out all the videos in USA
usa_videos=all_videos_sdf.filter(all_videos_sdf['country']=='US')

# %%
# Show the first 10 rows of videos in usa alongwith title, view_couunt,likes, and category
usa_videos.select('video_id', 'view_count', 'likes', 'category').show(10)

# %%
from pyspark.sql import SparkSession
# Create a sparksession object
spark = SparkSession.builder.appName("Convert to Pandas").getOrCreate()
# Convert the Spark DataFrame to a Pandas DataFrame
usa_videos_pandas_df = usa_videos.toPandas()
# Print the rows of Pandas DataFrame
print(len(usa_videos_pandas_df))


# %%
import pandas as pd
# Write the usa_videos dataframe to a csv file
usa_videos_pandas_df.to_csv('USA_Videos_trending_data.csv')
print('write successfully!')


# Import Filtered Dataset into Neo4j Database for Query

# %%
from neo4j import GraphDatabase

# Connect to database
database_name = "neo4j"
username = "apan5400"
password = "12345678"
uri = "bolt://localhost:7689/" + database_name

driver = GraphDatabase.driver(uri, auth=(username, password))
session = driver.session()

print("Successfully connected to Neo4j!")

# %%
print(usa_videos_pandas_df.columns)


# %%
# Delete all nodes and relationships
query1 = ("MATCH (n) OPTIONAL MATCH (n)-[r]-() DELETE n,r")
result1 = session.run(query1)
print("All Nodes and relationships are deleted!")

# %%
# Import graph from csv and create relationships
query2 = (
    "LOAD CSV WITH HEADERS FROM 'http://localhost:11001/project-f46884dd-b89d-4a4e-8dca-2a9c5950888d/USA_Videos_trending_data.csv' AS line "
    'CREATE (videos:Videos { \
                       id: line.video_id, \
                title: line.video_title,\
                  tags: line.tags,\
                       description: line.description\
                            } \
            )'
    'CREATE(performance:Performance{\
                          view_count: toInteger(line.view_count), \
                          likes: toInteger(line.likes),\
                         dislikes: toInteger(line.dislikes),\
                       comment_count: toInteger(line.comment_count)\
                                   } \
            )'
     'MERGE  (category:Category {name: line.category})'  
     'CREATE (videos)-[:BELONG_TO]->(category)'
     'CREATE (videos)-[:HAS_PERFORMANCE]->(performance)'
     'CREATE (performance)-[:BELONG_TO]->(category)'
    )

result2 = session.run(query2)
print("All videos are imported from a csv file!")

# %% [markdown]
# How many videos in each category？

# %%
query3=('MATCH (videos:Videos)-[:BELONG_TO]->(category:Category) \
RETURN category.name, count(videos) AS num_videos \
ORDER BY num_videos DESC')
result3 = session.run(query3)   
[f"{record['category.name']}: {record['num_videos']}" for record in result3]


# %% [markdown]
# How many total count_views in each category?

# %%
query4=('MATCH (videos:Videos)-[:HAS_PERFORMANCE]->(performance:Performance)-[:BELONG_TO]->(category:Category) \
        RETURN category.name, SUM(performance.view_count) AS total_views \
           ORDER BY total_views DESC')

result4 = session.run(query4)   
[f"{record['category.name']}: {record['total_views']}" for record in result4]

# %% [markdown]
# Print the top 10 videos with most comments

# %%
query5=('MATCH (videos:Videos)-[:HAS_PERFORMANCE]->(performance:Performance) \
RETURN videos.title, performance.comments \
ORDER BY performance.comments DESC \
LIMIT 10')
result5 = session.run(query5)   
[f"{record['videos.title']}: {record['performance.comments']}" for record in result5]

# %% [markdown]
# What are the top 10 most frequent words appeared in the top 1000 most viewed videos'tags in the sports category

# %%
query6 = (
    "MATCH (videos:Videos)-[:BELONG_TO]->(category:Category{name: 'Pets & Animals'}) "
    "WHERE size(videos.tags) > 0 "
    "WITH videos "
    "ORDER BY videos.views DESC "
    "LIMIT 100 "
    "UNWIND split(videos.tags, '|') as tag "
    "WITH TOLOWER(tag) as lowercase_tag "
    "WITH lowercase_tag, count(*) as tag_count "
    "WHERE lowercase_tag <> '[none]' AND lowercase_tag <> ''"
    "RETURN lowercase_tag, tag_count "
    "ORDER BY tag_count DESC "
    "LIMIT 10"
)
result6 = session.run(query6)
[{record["lowercase_tag"]:record['tag_count']} for record in result6]

# %% [markdown]
# Build the Flask web search engine for top 10 highest frequency tags of top 1000 videos in each category

# %%
#  Create a flask application instance
from flask import Flask, render_template, request
app = Flask(__name__)

driver = GraphDatabase.driver("bolt://localhost:7689", auth=("apan5400", "12345678"))



# %%
mkdir templates

# %%
# Build a function that input a category then return 10 most frequent tags
def get_top_tags(category_name):
    with driver.session() as session:
        query = (
            f"MATCH (videos:Videos)-[:BELONG_TO]->(category:Category{{name: '{category_name}'}}) "
            "WHERE size(videos.tags) > 0 "
            "WITH videos "
            "ORDER BY videos.views DESC "
            "LIMIT 1000 "
            "UNWIND split(videos.tags, '|') as tag "
            "WITH TOLOWER(tag) as lowercase_tag "
            "WITH lowercase_tag, count(*) as tag_count "
            "WHERE lowercase_tag <> '[none]' AND lowercase_tag <> ''"
            "RETURN lowercase_tag, tag_count "
            "ORDER BY tag_count DESC "
            "LIMIT 10"
        )
        result = session.run(query)
        tags = [{record["lowercase_tag"]: record["tag_count"]} for record in result]
        return tags


# %%
get_top_tags('Sports')

# %%
# Create a flask route

app.view_functions.pop('home', None)
app.view_functions.pop('search', None)
app.view_functions.pop('result', None)

@app.route('/', methods=['GET', 'POST'])
def search():
    return render_template('search.html')

# %%
@app.route('/results', methods=['GET', 'POST'])
def result():
    print(request.method) # Print out the request method
    if request.method == 'POST':
        category_name = request.form['category']
        result = get_top_tags(category_name)
        return render_template('result.html', result=result, category=category_name)
    else:
        return render_template('search.html'), 200

# %%
if __name__ == '__main__':
    app.config['DEBUG'] = True
    app.run()


