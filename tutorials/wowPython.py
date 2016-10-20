
# coding: utf-8

# # 0. Preparation and setup

# One Python library that makes GraphX support available to our Jupyter notebooks is not yet bound to the runtime by default. 
# 
# To get it added to the Spark context you have to use the `!pip` magic cell command `install` first to bind the library to the existing runtime.
# 
# The `pixiedust` library is implemented and loaded from [https://github.com/ibm-cds-labs/pixiedust](https://github.com/ibm-cds-labs/pixiedust). See the project documentation for details.

# In[2]:

get_ipython().system(u'pip install --user --upgrade --no-deps pixiedust')


# Pixiedust provides a nice visualization plugin for d3 style plots. Have a look at [https://d3js.org/](https://d3js.org/) if you are not yet familiar with d3. 
# 
# Having non-ascii characters in some of your tweets requires the Python interpreter to be set to support UTF-8. Reload your Python sys settings with UTF-8 encoding.

# In[3]:

import sys
reload(sys)
sys.setdefaultencoding('utf-8')


# When the library has been loaded successfully you have access to the PackageManager. Use the PackageManager to install a package to supply GraphFrames. Those are needed later in the notebook to complete the instructions for Spark GraphX.

# In[4]:

from pixiedust.packageManager import PackageManager
pkg=PackageManager()
pkg.installPackage("graphframes:graphframes:0")


# At this point you are being asked to _Please restart Kernel to complete installation of the new package_. Use the Restart Kernel dialog from the menu to do that. Once completed, you can start the analysis and resume with the next section.

# **************************************************
# **Please restart your Kernel before you proceed!**
# **************************************************

# # 1. Load data from Twitter to Cloudant
# 
# Following the lab instructions you should at this point have:
# 
# - a Cloudant account
# - an empty database in your Cloudant account
# - an IBM Insights for Twitter service instance

# Provide the details for both into the global variables section below, including
# 
# *Twitter*:
# - _restAPI_ - the API endpoint we use to query the Twitter API with. Use the URL for your IBM Insights for Twitter service and add `/api/v1/messages/search` as path (for example `https://cdeservice.stage1.mybluemix.net/api/v1/messages/search`)
# - _username_ - the username for your IBM Insights for Twitter service instance
# - _password_ - the password for your IBM Insights for Twitter service instance
# 
# *Cloudant*:
# - _account_ - the fully qualified account https URL (for example `https://testy-dynamite-001.cloudant.com`)
# - _username_ - the Cloudant account username
# - _password_ - the Cloudant account password
# - _database_ - the database name you want your tweets to be loaded into (Note: the database will NOT get created by the script below. Please create the database manually into your Cloudant account first.)

# In[1]:

properties = {
            'twitter': {
                'restAPI': 'https://xxx:xxx@cdeservice.mybluemix.net/api/v1/messages/search',
                'username': 'xxx',
                'password': 'xxx'
                },
            'cloudant': {
                'account':'https://xxx:xxx@xxx.cloudant.com', 
                'username':'xxx', 
                'password':'xxx',
                'database':'yyy'
                }
            }


# Import all required Python libraries.

# In[2]:

import requests
import json

from requests.auth import HTTPBasicAuth

import http.client


# Define a class with helper functions to query the Twitter service API and load documents in the Cloudant database using the bulk load API. (Note: no code is being executed yet and you don't expect any output for these declarations.)

# In[3]:

class TwitterToCloudant:

    count = 100
    
    def query_twitter(self, config, url, query, loop):

        loop = loop + 1
        if loop > (int(self.count) / 100):
            return

        # QUERY TWITTER
        if url is None:
            url = config["twitter"]["restAPI"]
            print(url, query)
            tweets = self.get_tweets(config, url, query)
   
        else:
            print(url)
            tweets = self.get_tweets(config, url, query)
 
        # LOAD TO CLOUDANT
        self.load_cloudant(config, tweets)

        # CONTINUE TO PAGE THROUGH RESULTS ....
        if "related" in tweets:
            url = tweets["related"]["next"]["href"]

            #!! recursive call
            self.query_twitter(config, url, None, loop)

    def get_tweets(self, config, url, query):

        # GET tweets from twitter endpoint
        user = config["twitter"]["username"]
        password = config["twitter"]["password"]
    
        print ("GET: Tweets from {} ".format(url))
       
        if query is None:
            payload = {'country_code' :' us', 'lang' : 'en'}
            response = requests.get(url, auth=HTTPBasicAuth(user, password))
        else:
            payload = {'q': query, 'country_code' :' us', 'lang' : 'en'}
            response = requests.get(url, params=payload, auth=HTTPBasicAuth(user, password))

        print ("Got {} response ".format(response.status_code))
        
        tweets = json.loads(response.text)

        return tweets

    def load_cloudant(self, config, tweets):
     
        # POST tweets to Cloudant database
        url = config["cloudant"]["account"] + "/" + config["cloudant"]["database"] + "/_bulk_docs"
        user = config["cloudant"]["username"]
        password = config["cloudant"]["password"]
        headers = {"Content-Type": "application/json"}

        if "tweets" in tweets:
            docs = {}
            docs["docs"] = tweets["tweets"]

            print ("POST: Docs to {}".format(url))
     
            response = requests.post(url, data=json.dumps(docs), headers=headers, auth=HTTPBasicAuth(user, password))
  
            print ("Got {} response ".format(response.status_code))


# Finally we make the call the load our Cloudant database with tweets. To do that, we require two parameters:
# 
# - _query_ - the query string to pass to the Twitter API. Use **#election2016** as default or experiment with your own.
# - _count_ - the number of tweets to process. Use **200** as a good start or scale up if you want. (Note: Execution time depends on ....)

# In[4]:

query = "#election2016"
count = 1000


# In[5]:

TtC = TwitterToCloudant()
TtC.count = count
    
TtC.query_twitter(properties, None, query, 0)


# At this point you should see a number of debug messages with response codes 200 and 201. As a result your database is loaded with the number of tweets you provided in _count_ variable above.
# 
# If there are response codes like 401 (unauthorized) or 404 (not found) please check your credentails and URLs provided in the _properties_ above. Changes you make to these settings are applied when you execute the cell again. There is no need to execute other cells (that have not been changed) and you can immediately come back here to re-run your TwitterToCloudant functions.
# 
# Should there be any severe problems that can not be resolved, we made a database called `tweets` already avaialable in your Cloudant account. You can continue to work through the following instructions using the `tweets` database instead.

# # 2. Analyze tweets with Spark SQL
# 
# In this section your are going to explore the tweets loaded into your Cloudant database using Spark SQL queries. The Cloudant Spark connector library available at [https://github.com/cloudant-labs/spark-cloudant](https://github.com/cloudant-labs/spark-cloudant) is already linked with the Spark deployment underneath this notebook. All you have to do at this point is to read your Cloudant documents into a DataFrame.

# First, this notebook runs on a shared Spark cluster but obtains a dedicated Spark context for isolated binding. The Spark context (sc) is made available automatically when the notebook is launched and should be started at this point. With a few statements you can inspect the Spark version and resources allocated for this context.
# 
# _Note: If there is ever a problem with the running Spark context, you can submit sc.stop() and sc.start() to recycle it_

# In[6]:

sc.version


# In[7]:

sc._conf.getAll()


# Now you want to create a Spark SQL context object off the given Spark context.

# In[8]:

sqlContext = SQLContext(sc)


# The Spark SQL context (sqlContext) is used to read data from the Cloudant database. We use a schema sample size and specified number of partitions to load the data with. For details on these parameters check [https://github.com/cloudant-labs/spark-cloudant#configuration-on-sparkconf](https://github.com/cloudant-labs/spark-cloudant#configuration-on-sparkconf)

# In[9]:

tweetsDF = sqlContext.read.format("com.cloudant.spark").    option("cloudant.host",properties['cloudant']['account'].replace('https://','')).    option("cloudant.username", properties['cloudant']['username']).    option("cloudant.password", properties['cloudant']['password']).    option("schemaSampleSize", "-1").    option("jsonstore.rdd.partitions", "5").    load(properties['cloudant']['database'])


# In[10]:

tweetsDF.show(5)


# For performance reasons we will cache the Data Frame to prevent re-loading.

# In[11]:

tweetsDF.cache()


# The schema of a Data Frame reveals the structure of all JSON documents loaded from your Cloudant database. Depending on the setting for the parameter `schemaSampleSize` the created RDD contains attributes for the first document only, for the first N documents, or for all documents. Please have a look at [https://github.com/cloudant-labs/spark-cloudant#schema-variance](https://github.com/cloudant-labs/spark-cloudant#schema-variance) for details on schema computation. 

# In[12]:

tweetsDF.printSchema()


# With the use of the IBM Insights for Twitter API all tweets are enriched with metadata. For example, the gender of the Twitter user or the state of his account location are added in clear text. Sentiment analysis is also done at the time the tweets are loaded from the original Twitter API. This allows us to group tweets according to their positive, neutral, or negative sentiment.
# 
# In a first example you can extract the gender, state, and polarity details from the DataFrame (or use any other field available in the schema output above). 
# 
# _Note: To extract a nested field you have to use the full attribute path, for example cde.author.gender or cde.content.sentiment.polarity. The alias() function is available to simplify the name in the resulting DataFrame._

# In[13]:

tweetsDF2 = tweetsDF.select(tweetsDF.cde.author.gender.alias("gender"), 
                 tweetsDF.cde.author.location.state.alias("state"),
                 tweetsDF.cde.content.sentiment.polarity.alias("polarity"))


# The above statement executes extremely fast because no actual function or transformation was computed yet. Spark uses a lazy approach to compute functions only when they are actually needed. The following function is used to show the output of the Data Frame. At that point only do you see a longer runtime to compute `tweetsDF2`. 

# In[14]:

tweetsDF2.count()


# In[15]:

tweetsDF2.printSchema()


# Work with other Spark SQL functions to do things like counting, grouping etc.

# In[16]:

# count tweets by state
tweets_state = tweetsDF2.groupBy(tweetsDF2.state).count()
tweets_state.show(100)

# count by gender & polarity
tweets_gp0 = tweetsDF2.groupBy(tweetsDF2.gender, tweetsDF2.polarity).count()
tweets_gp0.show(100)

tweets_gp= tweetsDF2.where(tweetsDF2.polarity.isNotNull()).groupBy("polarity").pivot("gender").count()
tweets_gp.show(100)


# ## 2.1 Plot results using matplotlib
# 
# In Python you can use simple libraries to plot your DataFrames directly in diagrams. However, the use of matplotlib is not trivial and once the data is rendered in the diagram it is static. For more comprehensive graphing Spark provides the GraphX extension. Here the data is transformed into a directed multigraph model (similar to those used in GraphDBs) called GraphFrames. 
# 
# You will explore GraphFrames later in this lab. Let's first have a look at simply plotting your DataFrames using matplotlib.  

# In[17]:

import pandas as pd
get_ipython().magic(u'matplotlib inline')
import matplotlib.pyplot as plt
import numpy as np


# Plot the number of tweets per state. Notice again how Spark computes the result lazily. In no previous output did we require the full DataFrame and it did not have to get fully computed until now.

# In[18]:

tweets_state_pd = tweets_state.toPandas()
values = tweets_state_pd['count']
labels = tweets_state_pd['state']

plt.gcf().set_size_inches(16, 12, forward=True)
plt.title('Number of tweets by state')

plt.barh(range(len(values)), values)
plt.yticks(range(len(values)), labels)

plt.show()


# More plots to group data by gender and polarity.

# In[19]:

tweets_gp_pd = tweets_gp.toPandas()
labels = tweets_gp_pd['polarity']

N = len(labels)
male = tweets_gp_pd['male']
female = tweets_gp_pd['female']
unknown = tweets_gp_pd['unknown']
ind = np.arange(N)  # the x locations for the groups
width = 0.2      # the width of the bars

fig, ax = plt.subplots()
rects1 = ax.bar(ind-width, male, width, color='b', label='male')
rects2 = ax.bar(ind, female, width, color='r', label='female')
rects3 = ax.bar(ind + width, unknown, width, color='y', label='unknown')

ax.set_ylabel('Count')
ax.set_title('Tweets by polarity and gender')
ax.set_xticks(ind + width)
ax.set_xticklabels(labels)
ax.legend((rects1[0], rects2[0], rects3[0]), ('male', 'female', 'unknown'))


plt.show()


# ## 2.2 Create SQL temporary tables

# With Spark SQL you can create in-memory tables and query your Spark RDDs in tables using SQL syntax. This is just an alternative represenation of your RDD where SQL functions (like filters or projections) are converted into Spark functions. For the user it mostly provides a SQL wrapper over Spark and a familiar way to query data.

# In[20]:

tweetsDF.registerTempTable("tweets_DF")


# Run SQL statements using the sqlContext.sql() function and render output with show(). The result of a SQL function could again be mapped to a data frame.

# In[21]:

sqlContext.sql("SELECT count(*) AS cnt FROM tweets_DF").show()


# In[22]:

sqlContext.sql("SELECT message.actor.displayName AS author, count(*) as cnt FROM tweets_DF GROUP BY message.actor.displayName ORDER BY cnt DESC").show(10)


# With multiple temporary tables (potentially from different databases) you can execute JOIN and UNION queries to analyze the database in combination.

# In the next query we will return all hashtags used in our body of tweets. 

# In[23]:

hashtags = sqlContext.sql("SELECT message.object.twitter_entities.hashtags.text as tags                 FROM tweets_DF                 WHERE message.object.twitter_entities.hashtags.text IS NOT NULL")


# The hashtags are in lists, one per tweet. We flat map this list to a large list and then store it back into a temporary table. The temporary table can be used to define a hashtag cloud to understand which hashtag has been used how many times.

# In[24]:

l = hashtags.map(lambda x: x.tags).collect()
tagCloud = [item for sublist in l for item in sublist]


# Create a DataFrame from the Python dictionary we used to flatten our hashtags into. The DataFrame has a simple schema with just a single column called `hastag`.

# In[25]:

from pyspark.sql import Row

tagCloudDF = sc.parallelize(tagCloud)
row = Row("hashtag")
hashtagsDF = tagCloudDF.map(row).toDF()


# Register a new temp table for hashtags. Group and count tags to get a sense of trending issues.

# In[26]:

hashtagsDF.registerTempTable("hashtags_DF")


# In[27]:

trending = sqlContext.sql("SELECT count(hashtag) as CNT, hashtag as TAG FROM hashtags_DF GROUP BY hashtag ORDER BY CNT DESC")
trending.show(10)


# ## 2.3 Visualize tag cloud with Brunel

# Let's create some charts and diagrams with Brunel commands.
# 
# The basic format of each call to Brunel is simple. Whether the command is a single line or a set of lines, the commands are concatenated together and the result interpreted as one command.
# 
# Here are some of the rules for using Brunel that you'll need in this notebook:
# 
# - _DataFrame_: Use the data command to specify the pandas DataFrame.
# - _Chart type_: Use commands like chord and treemap to specify a chart type. If you don't specify a type, the default chart type is a scatterplot.
# - _Chart definition_: Use the x and y commands to specify the data to include on the x-axis and the y-axis.
# - _Styling_: Use commands like color, tooltip, and label to control the styling of the graph.
# - _Size_: Use the width and height key-value pairs to specify the size of the graph. The key-value pairs must be preceded with two colons and separated with a comma, for example: :: width=800, height=300
# 
# See detailed documentation on the Brunel Visualization language at [https://brunel.mybluemix.net/docs](https://brunel.mybluemix.net/docs).

# In[28]:

import brunel
import sys

reload(sys)
sys.setdefaultencoding('utf-8')

trending_pd = trending.toPandas()


# Brunel libraries are able to read data from CSV files only. We will export our Panda DataFrames to CSV first to be able to load them with the Brunel libraries below.

# In[29]:

trending_pd.to_csv('trending_pd.csv')
tweets_state_pd.to_csv('tweets_state_pd.csv')
tweets_gp_pd.to_csv('tweets_gp_pd.csv')


# Top 5 records in every Panda DF.

# In[30]:

trending_pd.head(5)


# The hast tag cloud is visualized using the Brunel cloud graph.

# In[31]:

get_ipython().magic(u"brunel data('trending_pd') cloud color(cnt) size(cnt) label(tag) :: width=900, height=600")


# State and location data can be plotted on a map or a bubble graph representing the number of tweets per state. We will exercise maps later using the GraphX framework.

# In[32]:

tweets_state_pd.head(5)


# In[33]:

get_ipython().magic(u"brunel data('tweets_state_pd') bubble label(state) x(state) color(count) size(count)")


# Brunel graphs are D3 based and interactive. Try using your mouse on the graph for Gender polarity to hover over details and zoom in on the Y axis.

# In[34]:

tweets_gp_pd.head(5)


# In[35]:

get_ipython().magic(u"brunel data('tweets_gp_pd') bar x(polarity) y(male, female) color(male, female) tooltip(#all) legends(none) :: width=800, height=300")


# ## 2.4 Write analysis results back to Cloudant

# Next we are going to persist the hashtags_DF back into a Cloudant database. (Note: The database `hashtags` has to exist in Cloudant. Please create that database first.)

# In[36]:

hashtagsDF.write.format("com.cloudant.spark").    option("cloudant.host",properties['cloudant']['account'].replace('https://','')).    option("cloudant.username", properties['cloudant']['username']).    option("cloudant.password", properties['cloudant']['password']).    option("bulkSize", "2000").save("hashtags")


# # 3. Analysis with Spark GraphX

# Import dependencies from the Pixiedust library loaded in the preperation section. See [https://github.com/ibm-cds-labs/pixiedust](https://github.com/ibm-cds-labs/pixiedust) for details.

# In[37]:

from pixiedust.display import *


# To render a chart you have options to select the columns to display or the aggregation function to apply.

# In[38]:

tweets_state_us = tweets_state.filter(tweets_state.state.isin("Alabama", "Alaska", "Arizona", 
        "Arkansas", "California", "Colorado", "Connecticut", "Delaware", "Florida", 
        "Georgia", "Hawaii", "Idaho", "Illinois Indiana", "Iowa", "Kansas", "Kentucky", 
        "Louisiana", "Maine", "Maryland", "Massachusetts", "Michigan", "Minnesota", 
        "Mississippi", "Missouri", "Montana Nebraska", "Nevada", "New Hampshire", 
        "New Jersey", "New Mexico", "New York", "North Carolina", "North Dakota", 
        "Ohio", "Oklahoma", "Oregon", "Pennsylvania Rhode Island", "South Carolina", 
        "South Dakota", "Tennessee", "Texas","Utah", "Vermont", "Virginia", 
        "Washington", "West Virginia", "Wisconsin", "Wyoming"))


# In[39]:

tweets_state_us.show(5)


# In[ ]:

display(tweets_state_us)


# Use a data set with at least two numeric columns to create scatter plots.

# # 4. Analysis with Spark MLlib
# 
# Here we are going to use KMeans clustering algorithm from Spark MLlib.
# 
# Clustering will let us cluster similar tweets together.
# 
# We will then display clusters using Brunel library.

# In[41]:

# TRAINING by hashtag
from pyspark.mllib.feature import HashingTF
from pyspark.mllib.clustering import KMeans, KMeansModel

# dataframe of tweets' messages and hashtags
mhDF = sqlContext.sql("SELECT message.body as message,                 message.object.twitter_entities.hashtags.text as tags                 FROM tweets_DF                 WHERE message.object.twitter_entities.hashtags.text IS NOT NULL")
mhDF.show()
# create an RDD of hashtags
hashtagsRDD = mhDF.rdd.map(lambda h: h.tags)

# create Feature verctor for every tweet's hastags
# each hashtag represents feature
# a function calculates how many time hashtag is in a tweet
htf = HashingTF(100)
vectors = hashtagsRDD.map(lambda hs: htf.transform(hs)).cache()
print(vectors.take(2))

# Build the model (cluster the data)
numClusters = 10 # number of clusters
model = KMeans.train(vectors, numClusters, maxIterations=10, initializationMode="random")


# In[42]:

from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType, StringType

def predict(tags):
    vector = htf.transform(tags)
    return model.predict(vector)
# Creates a Column expression representing a user defined function
udfPredict = udf(predict, IntegerType())

def formatstr(message):
    lines = message.splitlines()
    return " ".join(lines)
udfFormatstr = udf(formatstr, StringType())

# transform mhDF into cmhDF, a dataframe containing formatted messages, 
# hashtabs and cluster
mhDF2 = mhDF.withColumn("message", udfFormatstr(mhDF.message))
cmhDF = mhDF2.withColumn("cluster", udfPredict(mhDF2.tags))
cmhDF.show()


# In[43]:

import sys
reload(sys)
sys.setdefaultencoding('utf-8')


# In[44]:

# visualizing clusters
import brunel

cmh_pd = cmhDF.toPandas()
cmh_pd.to_csv('cmh_pd.csv')
get_ipython().magic(u"brunel data('cmh_pd') bubble x(cluster) color(#all) size(#count) tooltip(message, tags) legends(none)")

