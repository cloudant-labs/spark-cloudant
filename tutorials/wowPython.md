
# 0. Preparation and setup

One Python library that makes GraphX support available to our Jupyter notebooks is not yet bound to the runtime by default. 

To get it added to the Spark context you have to use the `!pip` magic cell command `install` first to bind the library to the existing runtime.

The `pixiedust` library is implemented and loaded from [https://github.com/ibm-cds-labs/pixiedust](https://github.com/ibm-cds-labs/pixiedust). See the project documentation for details.


```python
!pip install --user --upgrade --no-deps pixiedust
```

    Requirement already up-to-date: pixiedust in /gpfs/global_fs01/sym_shared/YPProdSpark/user/s0af-f7d42ca46ba458-463367a6cbb4/.local/lib/python2.7/site-packages


Pixiedust provides a nice visualization plugin for d3 style plots. Have a look at [https://d3js.org/](https://d3js.org/) if you are not yet familiar with d3. 

Having non-ascii characters in some of your tweets requires the Python interpreter to be set to support UTF-8. Reload your Python sys settings with UTF-8 encoding.


```python
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
```

When the library has been loaded successfully you have access to the PackageManager. Use the PackageManager to install a package to supply GraphFrames. Those are needed later in the notebook to complete the instructions for Spark GraphX.


```python
from pixiedust.packageManager import PackageManager
pkg=PackageManager()
pkg.installPackage("graphframes:graphframes:0")
```




    <maven.artifact.Artifact at 0x7f7764a3cd90>



At this point you are being asked to _Please restart Kernel to complete installation of the new package_. Use the Restart Kernel dialog from the menu to do that. Once completed, you can start the analysis and resume with the next section.

**************************************************
**Please restart your Kernel before you proceed!**
**************************************************

# 1. Load data from Twitter to Cloudant

Following the lab instructions you should at this point have:

- a Cloudant account
- an empty database in your Cloudant account
- an IBM Insights for Twitter service instance

Provide the details for both into the global variables section below, including

*Twitter*:
- _restAPI_ - the API endpoint we use to query the Twitter API with. Use the URL for your IBM Insights for Twitter service and add `/api/v1/messages/search` as path (for example `https://cdeservice.stage1.mybluemix.net/api/v1/messages/search`)
- _username_ - the username for your IBM Insights for Twitter service instance
- _password_ - the password for your IBM Insights for Twitter service instance

*Cloudant*:
- _account_ - the fully qualified account https URL (for example `https://testy-dynamite-001.cloudant.com`)
- _username_ - the Cloudant account username
- _password_ - the Cloudant account password
- _database_ - the database name you want your tweets to be loaded into (Note: the database will NOT get created by the script below. Please create the database manually into your Cloudant account first.)


```python
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
```

Import all required Python libraries.


```python
import requests
import json

from requests.auth import HTTPBasicAuth

import http.client
```

Define a class with helper functions to query the Twitter service API and load documents in the Cloudant database using the bulk load API. (Note: no code is being executed yet and you don't expect any output for these declarations.)


```python
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

```

Finally we make the call the load our Cloudant database with tweets. To do that, we require two parameters:

- _query_ - the query string to pass to the Twitter API. Use **#election2016** as default or experiment with your own.
- _count_ - the number of tweets to process. Use **200** as a good start or scale up if you want. (Note: Execution time depends on ....)


```python
query = "#election2016"
count = 1000
```


```python
TtC = TwitterToCloudant()
TtC.count = count
    
TtC.query_twitter(properties, None, query, 0)
```

    ('https://5e2d04c1-cbcd-4159-901d-229e5a8d7054:JoOpVsIDMq@cdeservice.mybluemix.net/api/v1/messages/search', '#election2016')
    GET: Tweets from https://5e2d04c1-cbcd-4159-901d-229e5a8d7054:JoOpVsIDMq@cdeservice.mybluemix.net/api/v1/messages/search 
    Got 200 response 
    POST: Docs to https://873e246f-497b-4c1d-a7ef-b0a81dc5850a-bluemix:3091a8a0833332fa9505a411884ce5405e0d666a1e6fdb25b29a8abb9c44ae80@873e246f-497b-4c1d-a7ef-b0a81dc5850a-bluemix.cloudant.com/election2016/_bulk_docs
    Got 201 response 
    https://cdeservice.mybluemix.net:443/api/v1/messages/search?q=%23election2016&from=100&size=100
    GET: Tweets from https://cdeservice.mybluemix.net:443/api/v1/messages/search?q=%23election2016&from=100&size=100 
    Got 200 response 
    POST: Docs to https://873e246f-497b-4c1d-a7ef-b0a81dc5850a-bluemix:3091a8a0833332fa9505a411884ce5405e0d666a1e6fdb25b29a8abb9c44ae80@873e246f-497b-4c1d-a7ef-b0a81dc5850a-bluemix.cloudant.com/election2016/_bulk_docs
    Got 201 response 
    https://cdeservice.mybluemix.net:443/api/v1/messages/search?q=%23election2016&from=200&size=100
    GET: Tweets from https://cdeservice.mybluemix.net:443/api/v1/messages/search?q=%23election2016&from=200&size=100 
    Got 200 response 
    POST: Docs to https://873e246f-497b-4c1d-a7ef-b0a81dc5850a-bluemix:3091a8a0833332fa9505a411884ce5405e0d666a1e6fdb25b29a8abb9c44ae80@873e246f-497b-4c1d-a7ef-b0a81dc5850a-bluemix.cloudant.com/election2016/_bulk_docs
    Got 201 response 
    https://cdeservice.mybluemix.net:443/api/v1/messages/search?q=%23election2016&from=300&size=100
    GET: Tweets from https://cdeservice.mybluemix.net:443/api/v1/messages/search?q=%23election2016&from=300&size=100 
    Got 200 response 
    POST: Docs to https://873e246f-497b-4c1d-a7ef-b0a81dc5850a-bluemix:3091a8a0833332fa9505a411884ce5405e0d666a1e6fdb25b29a8abb9c44ae80@873e246f-497b-4c1d-a7ef-b0a81dc5850a-bluemix.cloudant.com/election2016/_bulk_docs
    Got 201 response 
    https://cdeservice.mybluemix.net:443/api/v1/messages/search?q=%23election2016&from=400&size=100
    GET: Tweets from https://cdeservice.mybluemix.net:443/api/v1/messages/search?q=%23election2016&from=400&size=100 
    Got 200 response 
    POST: Docs to https://873e246f-497b-4c1d-a7ef-b0a81dc5850a-bluemix:3091a8a0833332fa9505a411884ce5405e0d666a1e6fdb25b29a8abb9c44ae80@873e246f-497b-4c1d-a7ef-b0a81dc5850a-bluemix.cloudant.com/election2016/_bulk_docs
    Got 201 response 
    https://cdeservice.mybluemix.net:443/api/v1/messages/search?q=%23election2016&from=500&size=100
    GET: Tweets from https://cdeservice.mybluemix.net:443/api/v1/messages/search?q=%23election2016&from=500&size=100 
    Got 200 response 
    POST: Docs to https://873e246f-497b-4c1d-a7ef-b0a81dc5850a-bluemix:3091a8a0833332fa9505a411884ce5405e0d666a1e6fdb25b29a8abb9c44ae80@873e246f-497b-4c1d-a7ef-b0a81dc5850a-bluemix.cloudant.com/election2016/_bulk_docs
    Got 201 response 
    https://cdeservice.mybluemix.net:443/api/v1/messages/search?q=%23election2016&from=600&size=100
    GET: Tweets from https://cdeservice.mybluemix.net:443/api/v1/messages/search?q=%23election2016&from=600&size=100 
    Got 200 response 
    POST: Docs to https://873e246f-497b-4c1d-a7ef-b0a81dc5850a-bluemix:3091a8a0833332fa9505a411884ce5405e0d666a1e6fdb25b29a8abb9c44ae80@873e246f-497b-4c1d-a7ef-b0a81dc5850a-bluemix.cloudant.com/election2016/_bulk_docs
    Got 201 response 
    https://cdeservice.mybluemix.net:443/api/v1/messages/search?q=%23election2016&from=700&size=100
    GET: Tweets from https://cdeservice.mybluemix.net:443/api/v1/messages/search?q=%23election2016&from=700&size=100 
    Got 200 response 
    POST: Docs to https://873e246f-497b-4c1d-a7ef-b0a81dc5850a-bluemix:3091a8a0833332fa9505a411884ce5405e0d666a1e6fdb25b29a8abb9c44ae80@873e246f-497b-4c1d-a7ef-b0a81dc5850a-bluemix.cloudant.com/election2016/_bulk_docs
    Got 201 response 
    https://cdeservice.mybluemix.net:443/api/v1/messages/search?q=%23election2016&from=800&size=100
    GET: Tweets from https://cdeservice.mybluemix.net:443/api/v1/messages/search?q=%23election2016&from=800&size=100 
    Got 200 response 
    POST: Docs to https://873e246f-497b-4c1d-a7ef-b0a81dc5850a-bluemix:3091a8a0833332fa9505a411884ce5405e0d666a1e6fdb25b29a8abb9c44ae80@873e246f-497b-4c1d-a7ef-b0a81dc5850a-bluemix.cloudant.com/election2016/_bulk_docs
    Got 201 response 
    https://cdeservice.mybluemix.net:443/api/v1/messages/search?q=%23election2016&from=900&size=100
    GET: Tweets from https://cdeservice.mybluemix.net:443/api/v1/messages/search?q=%23election2016&from=900&size=100 
    Got 200 response 
    POST: Docs to https://873e246f-497b-4c1d-a7ef-b0a81dc5850a-bluemix:3091a8a0833332fa9505a411884ce5405e0d666a1e6fdb25b29a8abb9c44ae80@873e246f-497b-4c1d-a7ef-b0a81dc5850a-bluemix.cloudant.com/election2016/_bulk_docs
    Got 201 response 


At this point you should see a number of debug messages with response codes 200 and 201. As a result your database is loaded with the number of tweets you provided in _count_ variable above.

If there are response codes like 401 (unauthorized) or 404 (not found) please check your credentails and URLs provided in the _properties_ above. Changes you make to these settings are applied when you execute the cell again. There is no need to execute other cells (that have not been changed) and you can immediately come back here to re-run your TwitterToCloudant functions.

Should there be any severe problems that can not be resolved, we made a database called `tweets` already avaialable in your Cloudant account. You can continue to work through the following instructions using the `tweets` database instead.

# 2. Analyze tweets with Spark SQL

In this section your are going to explore the tweets loaded into your Cloudant database using Spark SQL queries. The Cloudant Spark connector library available at [https://github.com/cloudant-labs/spark-cloudant](https://github.com/cloudant-labs/spark-cloudant) is already linked with the Spark deployment underneath this notebook. All you have to do at this point is to read your Cloudant documents into a DataFrame.

First, this notebook runs on a shared Spark cluster but obtains a dedicated Spark context for isolated binding. The Spark context (sc) is made available automatically when the notebook is launched and should be started at this point. With a few statements you can inspect the Spark version and resources allocated for this context.

_Note: If there is ever a problem with the running Spark context, you can submit sc.stop() and sc.start() to recycle it_


```python
sc.version
```




    u'1.6.0'




```python
sc._conf.getAll()
```




    [(u'spark.eventLog.enabled', u'true'),
     (u'spark.deploy.resourceScheduler.factory',
      u'org.apache.spark.deploy.master.EGOResourceSchedulerFactory'),
     (u'spark.ui.retainedJobs', u'0'),
     (u'spark.eventLog.dir',
      u'/gpfs/fs01/user/s0af-f7d42ca46ba458-463367a6cbb4/events'),
     (u'spark.master', u'spark://yp-spark-dal09-env5-0046:7082'),
     (u'spark.shuffle.service.enabled', u'true'),
     (u'spark.executor.extraJavaOptions',
      u'-Djava.security.egd=file:/dev/./urandom'),
     (u'spark.port.maxRetries', u'512'),
     (u'spark.sql.tungsten.enabled', u'false'),
     (u'spark.logConf', u'true'),
     (u'spark.app.name', u'PySparkShell'),
     (u'spark.executor.memory', u'6G'),
     (u'spark.history.fs.logDirectory',
      u'/gpfs/fs01/user/s0af-f7d42ca46ba458-463367a6cbb4/events'),
     (u'spark.rdd.compress', u'True'),
     (u'spark.ui.enabled', u'false'),
     (u'spark.task.maxFailures', u'10'),
     (u'spark.driver.memory', u'1512M'),
     (u'spark.serializer.objectStreamReset', u'100'),
     (u'spark.sql.unsafe.enabled', u'false'),
     (u'spark.r.command',
      u'/usr/local/src/bluemix_jupyter_bundle.v20/R/bin/Rscript'),
     (u'spark.submit.deployMode', u'client'),
     (u'spark.ui.retainedStages', u'0'),
     (u'spark.worker.ui.retainedExecutors', u'0'),
     (u'spark.shuffle.service.port', u'7340'),
     (u'spark.master.rest.port', u'6070')]



Now you want to create a Spark SQL context object off the given Spark context.


```python
sqlContext = SQLContext(sc)
```

The Spark SQL context (sqlContext) is used to read data from the Cloudant database. We use a schema sample size and specified number of partitions to load the data with. For details on these parameters check [https://github.com/cloudant-labs/spark-cloudant#configuration-on-sparkconf](https://github.com/cloudant-labs/spark-cloudant#configuration-on-sparkconf)


```python
tweetsDF = sqlContext.read.format("com.cloudant.spark").\
    option("cloudant.host",properties['cloudant']['account'].replace('https://','')).\
    option("cloudant.username", properties['cloudant']['username']).\
    option("cloudant.password", properties['cloudant']['password']).\
    option("schemaSampleSize", "-1").\
    option("jsonstore.rdd.partitions", "5").\
    load(properties['cloudant']['database'])
```


```python
tweetsDF.show(5)
```

    +--------------------+--------------------+--------------------+-----------+--------------------+
    |                 _id|                _rev|                 cde|cdeInternal|             message|
    +--------------------+--------------------+--------------------+-----------+--------------------+
    |005cc8619ca9858fc...|1-476d93e03c3d4d2...|[[unknown,[Wichit...|       null|[[Blake Branson,7...|
    |005cc8619ca9858fc...|1-9f5b9ebac16e5af...|[[male,[Houston,U...|       null|[[carlos barrios,...|
    |005cc8619ca9858fc...|1-b82abe965647ae1...|[[male,[null,null...|       null|[[Perez Mariana,4...|
    |005cc8619ca9858fc...|1-678f0f85a17af43...|[[unknown,[null,n...|       null|[[BuzzFeedy,1,245...|
    |005cc8619ca9858fc...|1-ca6804db8db14e2...|[[male,[null,null...|       null|[[ramirez alberto...|
    +--------------------+--------------------+--------------------+-----------+--------------------+
    only showing top 5 rows
    


For performance reasons we will cache the Data Frame to prevent re-loading.


```python
tweetsDF.cache()
```




    DataFrame[_id: string, _rev: string, cde: struct<author:struct<gender:string,location:struct<city:string,country:string,state:string>,maritalStatus:struct<evidence:string,isMarried:string>,parenthood:struct<evidence:string,isParent:string>>,content:struct<sentiment:struct<evidence:array<struct<polarity:string,sentimentTerm:string>>,polarity:string>>>, cdeInternal: struct<compliance:struct<isActive:boolean,userDeleted:boolean,userProtected:boolean,userRetweetedDeleted:boolean,userRetweetedProtected:boolean,userSuspended:boolean>,tracks:array<struct<id:string>>>, message: struct<actor:struct<displayName:string,favoritesCount:bigint,followersCount:bigint,friendsCount:bigint,id:string,image:string,languages:array<string>,link:string,links:array<struct<href:string,rel:string>>,listedCount:bigint,location:struct<displayName:string,objectType:string>,objectType:string,postedTime:string,preferredUsername:string,statusesCount:bigint,summary:string,twitterTimeZone:string,utcOffset:string,verified:boolean>,body:string,favoritesCount:bigint,generator:struct<displayName:string,link:string>,geo:struct<coordinates:array<double>,type:string>,gnip:struct<language:struct<value:string>,profileLocations:array<struct<address:struct<country:string,countryCode:string,locality:string,region:string,subRegion:string>,displayName:string,geo:struct<coordinates:array<double>,type:string>,objectType:string>>,urls:array<struct<expanded_status:bigint,expanded_url:string,url:string>>>,id:string,inReplyTo:struct<link:string>,link:string,location:struct<country_code:string,displayName:string,geo:struct<coordinates:array<string>,type:string>,link:string,name:string,objectType:string,twitter_country_code:string,twitter_place_type:string>,object:struct<actor:struct<displayName:string,favoritesCount:bigint,followersCount:bigint,friendsCount:bigint,id:string,image:string,languages:array<string>,link:string,links:array<struct<href:string,rel:string>>,listedCount:bigint,location:struct<displayName:string,objectType:string>,objectType:string,postedTime:string,preferredUsername:string,statusesCount:bigint,summary:string,twitterTimeZone:string,utcOffset:string,verified:boolean>,body:string,favoritesCount:bigint,generator:struct<displayName:string,link:string>,geo:struct<coordinates:array<double>,type:string>,id:string,inReplyTo:struct<link:string>,link:string,location:struct<country_code:string,displayName:string,geo:struct<coordinates:array<array<array<double>>>,type:string>,link:string,name:string,objectType:string,twitter_country_code:string,twitter_place_type:string>,object:struct<id:string,link:string,objectType:string,postedTime:string,summary:string>,objectType:string,postedTime:string,provider:struct<displayName:string,link:string,objectType:string>,retweetCount:bigint,summary:string,twitter_entities:struct<hashtags:array<struct<indices:array<bigint>,text:string>>,media:array<struct<display_url:string,expanded_url:string,id:bigint,id_str:string,indices:array<bigint>,media_url:string,media_url_https:string,sizes:struct<large:struct<h:bigint,resize:string,w:bigint>,medium:struct<h:bigint,resize:string,w:bigint>,small:struct<h:bigint,resize:string,w:bigint>,thumb:struct<h:bigint,resize:string,w:bigint>>,source_status_id:bigint,source_status_id_str:string,type:string,url:string>>,symbols:array<string>,trends:array<string>,urls:array<struct<display_url:string,expanded_url:string,indices:array<bigint>,url:string>>,user_mentions:array<struct<id:bigint,id_str:string,indices:array<bigint>,name:string,screen_name:string>>>,twitter_extended_entities:struct<media:array<struct<display_url:string,expanded_url:string,id:bigint,id_str:string,indices:array<bigint>,media_url:string,media_url_https:string,sizes:struct<large:struct<h:bigint,resize:string,w:bigint>,medium:struct<h:bigint,resize:string,w:bigint>,small:struct<h:bigint,resize:string,w:bigint>,thumb:struct<h:bigint,resize:string,w:bigint>>,source_status_id:bigint,source_status_id_str:string,type:string,url:string,video_info:struct<aspect_ratio:array<bigint>,variants:array<struct<bitrate:bigint,content_type:string,url:string>>>>>>,twitter_filter_level:string,twitter_lang:string,verb:string>,objectType:string,postedTime:string,provider:struct<displayName:string,link:string,objectType:string>,retweetCount:bigint,twitter_entities:struct<hashtags:array<struct<indices:array<bigint>,text:string>>,media:array<struct<display_url:string,expanded_url:string,id:bigint,id_str:string,indices:array<bigint>,media_url:string,media_url_https:string,sizes:struct<large:struct<h:bigint,resize:string,w:bigint>,medium:struct<h:bigint,resize:string,w:bigint>,small:struct<h:bigint,resize:string,w:bigint>,thumb:struct<h:bigint,resize:string,w:bigint>>,source_status_id:bigint,source_status_id_str:string,type:string,url:string>>,symbols:array<string>,trends:array<string>,urls:array<struct<display_url:string,expanded_url:string,indices:array<bigint>,url:string>>,user_mentions:array<struct<id:bigint,id_str:string,indices:array<bigint>,name:string,screen_name:string>>>,twitter_extended_entities:struct<media:array<struct<display_url:string,expanded_url:string,id:bigint,id_str:string,indices:array<bigint>,media_url:string,media_url_https:string,sizes:struct<large:struct<h:bigint,resize:string,w:bigint>,medium:struct<h:bigint,resize:string,w:bigint>,small:struct<h:bigint,resize:string,w:bigint>,thumb:struct<h:bigint,resize:string,w:bigint>>,source_status_id:bigint,source_status_id_str:string,type:string,url:string,video_info:struct<aspect_ratio:array<bigint>,variants:array<struct<bitrate:bigint,content_type:string,url:string>>>>>>,twitter_filter_level:string,twitter_lang:string,verb:string>]



The schema of a Data Frame reveals the structure of all JSON documents loaded from your Cloudant database. Depending on the setting for the parameter `schemaSampleSize` the created RDD contains attributes for the first document only, for the first N documents, or for all documents. Please have a look at [https://github.com/cloudant-labs/spark-cloudant#schema-variance](https://github.com/cloudant-labs/spark-cloudant#schema-variance) for details on schema computation. 


```python
tweetsDF.printSchema()
```

    root
     |-- _id: string (nullable = true)
     |-- _rev: string (nullable = true)
     |-- cde: struct (nullable = true)
     |    |-- author: struct (nullable = true)
     |    |    |-- gender: string (nullable = true)
     |    |    |-- location: struct (nullable = true)
     |    |    |    |-- city: string (nullable = true)
     |    |    |    |-- country: string (nullable = true)
     |    |    |    |-- state: string (nullable = true)
     |    |    |-- maritalStatus: struct (nullable = true)
     |    |    |    |-- evidence: string (nullable = true)
     |    |    |    |-- isMarried: string (nullable = true)
     |    |    |-- parenthood: struct (nullable = true)
     |    |    |    |-- evidence: string (nullable = true)
     |    |    |    |-- isParent: string (nullable = true)
     |    |-- content: struct (nullable = true)
     |    |    |-- sentiment: struct (nullable = true)
     |    |    |    |-- evidence: array (nullable = true)
     |    |    |    |    |-- element: struct (containsNull = true)
     |    |    |    |    |    |-- polarity: string (nullable = true)
     |    |    |    |    |    |-- sentimentTerm: string (nullable = true)
     |    |    |    |-- polarity: string (nullable = true)
     |-- cdeInternal: struct (nullable = true)
     |    |-- compliance: struct (nullable = true)
     |    |    |-- isActive: boolean (nullable = true)
     |    |    |-- userDeleted: boolean (nullable = true)
     |    |    |-- userProtected: boolean (nullable = true)
     |    |    |-- userRetweetedDeleted: boolean (nullable = true)
     |    |    |-- userRetweetedProtected: boolean (nullable = true)
     |    |    |-- userSuspended: boolean (nullable = true)
     |    |-- tracks: array (nullable = true)
     |    |    |-- element: struct (containsNull = true)
     |    |    |    |-- id: string (nullable = true)
     |-- message: struct (nullable = true)
     |    |-- actor: struct (nullable = true)
     |    |    |-- displayName: string (nullable = true)
     |    |    |-- favoritesCount: long (nullable = true)
     |    |    |-- followersCount: long (nullable = true)
     |    |    |-- friendsCount: long (nullable = true)
     |    |    |-- id: string (nullable = true)
     |    |    |-- image: string (nullable = true)
     |    |    |-- languages: array (nullable = true)
     |    |    |    |-- element: string (containsNull = true)
     |    |    |-- link: string (nullable = true)
     |    |    |-- links: array (nullable = true)
     |    |    |    |-- element: struct (containsNull = true)
     |    |    |    |    |-- href: string (nullable = true)
     |    |    |    |    |-- rel: string (nullable = true)
     |    |    |-- listedCount: long (nullable = true)
     |    |    |-- location: struct (nullable = true)
     |    |    |    |-- displayName: string (nullable = true)
     |    |    |    |-- objectType: string (nullable = true)
     |    |    |-- objectType: string (nullable = true)
     |    |    |-- postedTime: string (nullable = true)
     |    |    |-- preferredUsername: string (nullable = true)
     |    |    |-- statusesCount: long (nullable = true)
     |    |    |-- summary: string (nullable = true)
     |    |    |-- twitterTimeZone: string (nullable = true)
     |    |    |-- utcOffset: string (nullable = true)
     |    |    |-- verified: boolean (nullable = true)
     |    |-- body: string (nullable = true)
     |    |-- favoritesCount: long (nullable = true)
     |    |-- generator: struct (nullable = true)
     |    |    |-- displayName: string (nullable = true)
     |    |    |-- link: string (nullable = true)
     |    |-- geo: struct (nullable = true)
     |    |    |-- coordinates: array (nullable = true)
     |    |    |    |-- element: double (containsNull = true)
     |    |    |-- type: string (nullable = true)
     |    |-- gnip: struct (nullable = true)
     |    |    |-- language: struct (nullable = true)
     |    |    |    |-- value: string (nullable = true)
     |    |    |-- profileLocations: array (nullable = true)
     |    |    |    |-- element: struct (containsNull = true)
     |    |    |    |    |-- address: struct (nullable = true)
     |    |    |    |    |    |-- country: string (nullable = true)
     |    |    |    |    |    |-- countryCode: string (nullable = true)
     |    |    |    |    |    |-- locality: string (nullable = true)
     |    |    |    |    |    |-- region: string (nullable = true)
     |    |    |    |    |    |-- subRegion: string (nullable = true)
     |    |    |    |    |-- displayName: string (nullable = true)
     |    |    |    |    |-- geo: struct (nullable = true)
     |    |    |    |    |    |-- coordinates: array (nullable = true)
     |    |    |    |    |    |    |-- element: double (containsNull = true)
     |    |    |    |    |    |-- type: string (nullable = true)
     |    |    |    |    |-- objectType: string (nullable = true)
     |    |    |-- urls: array (nullable = true)
     |    |    |    |-- element: struct (containsNull = true)
     |    |    |    |    |-- expanded_status: long (nullable = true)
     |    |    |    |    |-- expanded_url: string (nullable = true)
     |    |    |    |    |-- url: string (nullable = true)
     |    |-- id: string (nullable = true)
     |    |-- inReplyTo: struct (nullable = true)
     |    |    |-- link: string (nullable = true)
     |    |-- link: string (nullable = true)
     |    |-- location: struct (nullable = true)
     |    |    |-- country_code: string (nullable = true)
     |    |    |-- displayName: string (nullable = true)
     |    |    |-- geo: struct (nullable = true)
     |    |    |    |-- coordinates: array (nullable = true)
     |    |    |    |    |-- element: string (containsNull = true)
     |    |    |    |-- type: string (nullable = true)
     |    |    |-- link: string (nullable = true)
     |    |    |-- name: string (nullable = true)
     |    |    |-- objectType: string (nullable = true)
     |    |    |-- twitter_country_code: string (nullable = true)
     |    |    |-- twitter_place_type: string (nullable = true)
     |    |-- object: struct (nullable = true)
     |    |    |-- actor: struct (nullable = true)
     |    |    |    |-- displayName: string (nullable = true)
     |    |    |    |-- favoritesCount: long (nullable = true)
     |    |    |    |-- followersCount: long (nullable = true)
     |    |    |    |-- friendsCount: long (nullable = true)
     |    |    |    |-- id: string (nullable = true)
     |    |    |    |-- image: string (nullable = true)
     |    |    |    |-- languages: array (nullable = true)
     |    |    |    |    |-- element: string (containsNull = true)
     |    |    |    |-- link: string (nullable = true)
     |    |    |    |-- links: array (nullable = true)
     |    |    |    |    |-- element: struct (containsNull = true)
     |    |    |    |    |    |-- href: string (nullable = true)
     |    |    |    |    |    |-- rel: string (nullable = true)
     |    |    |    |-- listedCount: long (nullable = true)
     |    |    |    |-- location: struct (nullable = true)
     |    |    |    |    |-- displayName: string (nullable = true)
     |    |    |    |    |-- objectType: string (nullable = true)
     |    |    |    |-- objectType: string (nullable = true)
     |    |    |    |-- postedTime: string (nullable = true)
     |    |    |    |-- preferredUsername: string (nullable = true)
     |    |    |    |-- statusesCount: long (nullable = true)
     |    |    |    |-- summary: string (nullable = true)
     |    |    |    |-- twitterTimeZone: string (nullable = true)
     |    |    |    |-- utcOffset: string (nullable = true)
     |    |    |    |-- verified: boolean (nullable = true)
     |    |    |-- body: string (nullable = true)
     |    |    |-- favoritesCount: long (nullable = true)
     |    |    |-- generator: struct (nullable = true)
     |    |    |    |-- displayName: string (nullable = true)
     |    |    |    |-- link: string (nullable = true)
     |    |    |-- geo: struct (nullable = true)
     |    |    |    |-- coordinates: array (nullable = true)
     |    |    |    |    |-- element: double (containsNull = true)
     |    |    |    |-- type: string (nullable = true)
     |    |    |-- id: string (nullable = true)
     |    |    |-- inReplyTo: struct (nullable = true)
     |    |    |    |-- link: string (nullable = true)
     |    |    |-- link: string (nullable = true)
     |    |    |-- location: struct (nullable = true)
     |    |    |    |-- country_code: string (nullable = true)
     |    |    |    |-- displayName: string (nullable = true)
     |    |    |    |-- geo: struct (nullable = true)
     |    |    |    |    |-- coordinates: array (nullable = true)
     |    |    |    |    |    |-- element: array (containsNull = true)
     |    |    |    |    |    |    |-- element: array (containsNull = true)
     |    |    |    |    |    |    |    |-- element: double (containsNull = true)
     |    |    |    |    |-- type: string (nullable = true)
     |    |    |    |-- link: string (nullable = true)
     |    |    |    |-- name: string (nullable = true)
     |    |    |    |-- objectType: string (nullable = true)
     |    |    |    |-- twitter_country_code: string (nullable = true)
     |    |    |    |-- twitter_place_type: string (nullable = true)
     |    |    |-- object: struct (nullable = true)
     |    |    |    |-- id: string (nullable = true)
     |    |    |    |-- link: string (nullable = true)
     |    |    |    |-- objectType: string (nullable = true)
     |    |    |    |-- postedTime: string (nullable = true)
     |    |    |    |-- summary: string (nullable = true)
     |    |    |-- objectType: string (nullable = true)
     |    |    |-- postedTime: string (nullable = true)
     |    |    |-- provider: struct (nullable = true)
     |    |    |    |-- displayName: string (nullable = true)
     |    |    |    |-- link: string (nullable = true)
     |    |    |    |-- objectType: string (nullable = true)
     |    |    |-- retweetCount: long (nullable = true)
     |    |    |-- summary: string (nullable = true)
     |    |    |-- twitter_entities: struct (nullable = true)
     |    |    |    |-- hashtags: array (nullable = true)
     |    |    |    |    |-- element: struct (containsNull = true)
     |    |    |    |    |    |-- indices: array (nullable = true)
     |    |    |    |    |    |    |-- element: long (containsNull = true)
     |    |    |    |    |    |-- text: string (nullable = true)
     |    |    |    |-- media: array (nullable = true)
     |    |    |    |    |-- element: struct (containsNull = true)
     |    |    |    |    |    |-- display_url: string (nullable = true)
     |    |    |    |    |    |-- expanded_url: string (nullable = true)
     |    |    |    |    |    |-- id: long (nullable = true)
     |    |    |    |    |    |-- id_str: string (nullable = true)
     |    |    |    |    |    |-- indices: array (nullable = true)
     |    |    |    |    |    |    |-- element: long (containsNull = true)
     |    |    |    |    |    |-- media_url: string (nullable = true)
     |    |    |    |    |    |-- media_url_https: string (nullable = true)
     |    |    |    |    |    |-- sizes: struct (nullable = true)
     |    |    |    |    |    |    |-- large: struct (nullable = true)
     |    |    |    |    |    |    |    |-- h: long (nullable = true)
     |    |    |    |    |    |    |    |-- resize: string (nullable = true)
     |    |    |    |    |    |    |    |-- w: long (nullable = true)
     |    |    |    |    |    |    |-- medium: struct (nullable = true)
     |    |    |    |    |    |    |    |-- h: long (nullable = true)
     |    |    |    |    |    |    |    |-- resize: string (nullable = true)
     |    |    |    |    |    |    |    |-- w: long (nullable = true)
     |    |    |    |    |    |    |-- small: struct (nullable = true)
     |    |    |    |    |    |    |    |-- h: long (nullable = true)
     |    |    |    |    |    |    |    |-- resize: string (nullable = true)
     |    |    |    |    |    |    |    |-- w: long (nullable = true)
     |    |    |    |    |    |    |-- thumb: struct (nullable = true)
     |    |    |    |    |    |    |    |-- h: long (nullable = true)
     |    |    |    |    |    |    |    |-- resize: string (nullable = true)
     |    |    |    |    |    |    |    |-- w: long (nullable = true)
     |    |    |    |    |    |-- source_status_id: long (nullable = true)
     |    |    |    |    |    |-- source_status_id_str: string (nullable = true)
     |    |    |    |    |    |-- type: string (nullable = true)
     |    |    |    |    |    |-- url: string (nullable = true)
     |    |    |    |-- symbols: array (nullable = true)
     |    |    |    |    |-- element: string (containsNull = true)
     |    |    |    |-- trends: array (nullable = true)
     |    |    |    |    |-- element: string (containsNull = true)
     |    |    |    |-- urls: array (nullable = true)
     |    |    |    |    |-- element: struct (containsNull = true)
     |    |    |    |    |    |-- display_url: string (nullable = true)
     |    |    |    |    |    |-- expanded_url: string (nullable = true)
     |    |    |    |    |    |-- indices: array (nullable = true)
     |    |    |    |    |    |    |-- element: long (containsNull = true)
     |    |    |    |    |    |-- url: string (nullable = true)
     |    |    |    |-- user_mentions: array (nullable = true)
     |    |    |    |    |-- element: struct (containsNull = true)
     |    |    |    |    |    |-- id: long (nullable = true)
     |    |    |    |    |    |-- id_str: string (nullable = true)
     |    |    |    |    |    |-- indices: array (nullable = true)
     |    |    |    |    |    |    |-- element: long (containsNull = true)
     |    |    |    |    |    |-- name: string (nullable = true)
     |    |    |    |    |    |-- screen_name: string (nullable = true)
     |    |    |-- twitter_extended_entities: struct (nullable = true)
     |    |    |    |-- media: array (nullable = true)
     |    |    |    |    |-- element: struct (containsNull = true)
     |    |    |    |    |    |-- display_url: string (nullable = true)
     |    |    |    |    |    |-- expanded_url: string (nullable = true)
     |    |    |    |    |    |-- id: long (nullable = true)
     |    |    |    |    |    |-- id_str: string (nullable = true)
     |    |    |    |    |    |-- indices: array (nullable = true)
     |    |    |    |    |    |    |-- element: long (containsNull = true)
     |    |    |    |    |    |-- media_url: string (nullable = true)
     |    |    |    |    |    |-- media_url_https: string (nullable = true)
     |    |    |    |    |    |-- sizes: struct (nullable = true)
     |    |    |    |    |    |    |-- large: struct (nullable = true)
     |    |    |    |    |    |    |    |-- h: long (nullable = true)
     |    |    |    |    |    |    |    |-- resize: string (nullable = true)
     |    |    |    |    |    |    |    |-- w: long (nullable = true)
     |    |    |    |    |    |    |-- medium: struct (nullable = true)
     |    |    |    |    |    |    |    |-- h: long (nullable = true)
     |    |    |    |    |    |    |    |-- resize: string (nullable = true)
     |    |    |    |    |    |    |    |-- w: long (nullable = true)
     |    |    |    |    |    |    |-- small: struct (nullable = true)
     |    |    |    |    |    |    |    |-- h: long (nullable = true)
     |    |    |    |    |    |    |    |-- resize: string (nullable = true)
     |    |    |    |    |    |    |    |-- w: long (nullable = true)
     |    |    |    |    |    |    |-- thumb: struct (nullable = true)
     |    |    |    |    |    |    |    |-- h: long (nullable = true)
     |    |    |    |    |    |    |    |-- resize: string (nullable = true)
     |    |    |    |    |    |    |    |-- w: long (nullable = true)
     |    |    |    |    |    |-- source_status_id: long (nullable = true)
     |    |    |    |    |    |-- source_status_id_str: string (nullable = true)
     |    |    |    |    |    |-- type: string (nullable = true)
     |    |    |    |    |    |-- url: string (nullable = true)
     |    |    |    |    |    |-- video_info: struct (nullable = true)
     |    |    |    |    |    |    |-- aspect_ratio: array (nullable = true)
     |    |    |    |    |    |    |    |-- element: long (containsNull = true)
     |    |    |    |    |    |    |-- variants: array (nullable = true)
     |    |    |    |    |    |    |    |-- element: struct (containsNull = true)
     |    |    |    |    |    |    |    |    |-- bitrate: long (nullable = true)
     |    |    |    |    |    |    |    |    |-- content_type: string (nullable = true)
     |    |    |    |    |    |    |    |    |-- url: string (nullable = true)
     |    |    |-- twitter_filter_level: string (nullable = true)
     |    |    |-- twitter_lang: string (nullable = true)
     |    |    |-- verb: string (nullable = true)
     |    |-- objectType: string (nullable = true)
     |    |-- postedTime: string (nullable = true)
     |    |-- provider: struct (nullable = true)
     |    |    |-- displayName: string (nullable = true)
     |    |    |-- link: string (nullable = true)
     |    |    |-- objectType: string (nullable = true)
     |    |-- retweetCount: long (nullable = true)
     |    |-- twitter_entities: struct (nullable = true)
     |    |    |-- hashtags: array (nullable = true)
     |    |    |    |-- element: struct (containsNull = true)
     |    |    |    |    |-- indices: array (nullable = true)
     |    |    |    |    |    |-- element: long (containsNull = true)
     |    |    |    |    |-- text: string (nullable = true)
     |    |    |-- media: array (nullable = true)
     |    |    |    |-- element: struct (containsNull = true)
     |    |    |    |    |-- display_url: string (nullable = true)
     |    |    |    |    |-- expanded_url: string (nullable = true)
     |    |    |    |    |-- id: long (nullable = true)
     |    |    |    |    |-- id_str: string (nullable = true)
     |    |    |    |    |-- indices: array (nullable = true)
     |    |    |    |    |    |-- element: long (containsNull = true)
     |    |    |    |    |-- media_url: string (nullable = true)
     |    |    |    |    |-- media_url_https: string (nullable = true)
     |    |    |    |    |-- sizes: struct (nullable = true)
     |    |    |    |    |    |-- large: struct (nullable = true)
     |    |    |    |    |    |    |-- h: long (nullable = true)
     |    |    |    |    |    |    |-- resize: string (nullable = true)
     |    |    |    |    |    |    |-- w: long (nullable = true)
     |    |    |    |    |    |-- medium: struct (nullable = true)
     |    |    |    |    |    |    |-- h: long (nullable = true)
     |    |    |    |    |    |    |-- resize: string (nullable = true)
     |    |    |    |    |    |    |-- w: long (nullable = true)
     |    |    |    |    |    |-- small: struct (nullable = true)
     |    |    |    |    |    |    |-- h: long (nullable = true)
     |    |    |    |    |    |    |-- resize: string (nullable = true)
     |    |    |    |    |    |    |-- w: long (nullable = true)
     |    |    |    |    |    |-- thumb: struct (nullable = true)
     |    |    |    |    |    |    |-- h: long (nullable = true)
     |    |    |    |    |    |    |-- resize: string (nullable = true)
     |    |    |    |    |    |    |-- w: long (nullable = true)
     |    |    |    |    |-- source_status_id: long (nullable = true)
     |    |    |    |    |-- source_status_id_str: string (nullable = true)
     |    |    |    |    |-- type: string (nullable = true)
     |    |    |    |    |-- url: string (nullable = true)
     |    |    |-- symbols: array (nullable = true)
     |    |    |    |-- element: string (containsNull = true)
     |    |    |-- trends: array (nullable = true)
     |    |    |    |-- element: string (containsNull = true)
     |    |    |-- urls: array (nullable = true)
     |    |    |    |-- element: struct (containsNull = true)
     |    |    |    |    |-- display_url: string (nullable = true)
     |    |    |    |    |-- expanded_url: string (nullable = true)
     |    |    |    |    |-- indices: array (nullable = true)
     |    |    |    |    |    |-- element: long (containsNull = true)
     |    |    |    |    |-- url: string (nullable = true)
     |    |    |-- user_mentions: array (nullable = true)
     |    |    |    |-- element: struct (containsNull = true)
     |    |    |    |    |-- id: long (nullable = true)
     |    |    |    |    |-- id_str: string (nullable = true)
     |    |    |    |    |-- indices: array (nullable = true)
     |    |    |    |    |    |-- element: long (containsNull = true)
     |    |    |    |    |-- name: string (nullable = true)
     |    |    |    |    |-- screen_name: string (nullable = true)
     |    |-- twitter_extended_entities: struct (nullable = true)
     |    |    |-- media: array (nullable = true)
     |    |    |    |-- element: struct (containsNull = true)
     |    |    |    |    |-- display_url: string (nullable = true)
     |    |    |    |    |-- expanded_url: string (nullable = true)
     |    |    |    |    |-- id: long (nullable = true)
     |    |    |    |    |-- id_str: string (nullable = true)
     |    |    |    |    |-- indices: array (nullable = true)
     |    |    |    |    |    |-- element: long (containsNull = true)
     |    |    |    |    |-- media_url: string (nullable = true)
     |    |    |    |    |-- media_url_https: string (nullable = true)
     |    |    |    |    |-- sizes: struct (nullable = true)
     |    |    |    |    |    |-- large: struct (nullable = true)
     |    |    |    |    |    |    |-- h: long (nullable = true)
     |    |    |    |    |    |    |-- resize: string (nullable = true)
     |    |    |    |    |    |    |-- w: long (nullable = true)
     |    |    |    |    |    |-- medium: struct (nullable = true)
     |    |    |    |    |    |    |-- h: long (nullable = true)
     |    |    |    |    |    |    |-- resize: string (nullable = true)
     |    |    |    |    |    |    |-- w: long (nullable = true)
     |    |    |    |    |    |-- small: struct (nullable = true)
     |    |    |    |    |    |    |-- h: long (nullable = true)
     |    |    |    |    |    |    |-- resize: string (nullable = true)
     |    |    |    |    |    |    |-- w: long (nullable = true)
     |    |    |    |    |    |-- thumb: struct (nullable = true)
     |    |    |    |    |    |    |-- h: long (nullable = true)
     |    |    |    |    |    |    |-- resize: string (nullable = true)
     |    |    |    |    |    |    |-- w: long (nullable = true)
     |    |    |    |    |-- source_status_id: long (nullable = true)
     |    |    |    |    |-- source_status_id_str: string (nullable = true)
     |    |    |    |    |-- type: string (nullable = true)
     |    |    |    |    |-- url: string (nullable = true)
     |    |    |    |    |-- video_info: struct (nullable = true)
     |    |    |    |    |    |-- aspect_ratio: array (nullable = true)
     |    |    |    |    |    |    |-- element: long (containsNull = true)
     |    |    |    |    |    |-- variants: array (nullable = true)
     |    |    |    |    |    |    |-- element: struct (containsNull = true)
     |    |    |    |    |    |    |    |-- bitrate: long (nullable = true)
     |    |    |    |    |    |    |    |-- content_type: string (nullable = true)
     |    |    |    |    |    |    |    |-- url: string (nullable = true)
     |    |-- twitter_filter_level: string (nullable = true)
     |    |-- twitter_lang: string (nullable = true)
     |    |-- verb: string (nullable = true)
    


With the use of the IBM Insights for Twitter API all tweets are enriched with metadata. For example, the gender of the Twitter user or the state of his account location are added in clear text. Sentiment analysis is also done at the time the tweets are loaded from the original Twitter API. This allows us to group tweets according to their positive, neutral, or negative sentiment.

In a first example you can extract the gender, state, and polarity details from the DataFrame (or use any other field available in the schema output above). 

_Note: To extract a nested field you have to use the full attribute path, for example cde.author.gender or cde.content.sentiment.polarity. The alias() function is available to simplify the name in the resulting DataFrame._


```python
tweetsDF2 = tweetsDF.select(tweetsDF.cde.author.gender.alias("gender"), 
                 tweetsDF.cde.author.location.state.alias("state"),
                 tweetsDF.cde.content.sentiment.polarity.alias("polarity"))
```

The above statement executes extremely fast because no actual function or transformation was computed yet. Spark uses a lazy approach to compute functions only when they are actually needed. The following function is used to show the output of the Data Frame. At that point only do you see a longer runtime to compute `tweetsDF2`. 


```python
tweetsDF2.count()
```




    6600




```python
tweetsDF2.printSchema()
```

    root
     |-- gender: string (nullable = true)
     |-- state: string (nullable = true)
     |-- polarity: string (nullable = true)
    


Work with other Spark SQL functions to do things like counting, grouping etc.


```python
# count tweets by state
tweets_state = tweetsDF2.groupBy(tweetsDF2.state).count()
tweets_state.show(100)

# count by gender & polarity
tweets_gp0 = tweetsDF2.groupBy(tweetsDF2.gender, tweetsDF2.polarity).count()
tweets_gp0.show(100)

tweets_gp= tweetsDF2.where(tweetsDF2.polarity.isNotNull()).groupBy("polarity").pivot("gender").count()
tweets_gp.show(100)
```

    +--------------------+-----+
    |               state|count|
    +--------------------+-----+
    |            Ar Riyāḑ|    8|
    |            Michigan|   24|
    |            Kinshasa|    8|
    |               Texas|  286|
    |           Louisiana|   18|
    |            Victoria|   12|
    |The Federal District|   16|
    |District of Columbia|  142|
    |         Connecticut|   12|
    |             Florida|  162|
    |            Arkansas|   12|
    |             Montana|   14|
    |            Tasmania|    8|
    |              Kansas|   12|
    |           Wisconsin|   14|
    |             Wyoming|    8|
    |        Pennsylvania|   28|
    |              Hawaii|    6|
    |          Washington|   38|
    |            Missouri|   20|
    |           Tennessee|    6|
    |            New York|  150|
    |             England|   12|
    |            Colorado|   66|
    |          Calabarzon|    8|
    |         Mississippi|    8|
    |                Ohio|   90|
    |                Iowa|   34|
    |              Alaska|   22|
    |      North Carolina|  124|
    |    British Columbia|    6|
    |            Scotland|    6|
    |              Oregon|    6|
    |              Nevada|   12|
    |          California|  356|
    |             Georgia|   46|
    |            Oklahoma|   18|
    |                Utah|   22|
    |             Arizona|   80|
    |      South Carolina|   42|
    |        South Dakota|    6|
    |       Krasnodarskiy|    8|
    |            Virginia|   54|
    |       Île-de-France|    6|
    |              Guayas|    6|
    |National Capital ...|   14|
    |             Ontario|   18|
    |                null| 3370|
    |                    |  700|
    |             Indiana|   62|
    |       New Hampshire|   30|
    |            Maryland|   46|
    |            Nebraska|    8|
    |               Maine|   12|
    |            Illinois|   78|
    |             Alabama|   12|
    |          New Jersey|   64|
    |     South Australia|    8|
    |       Massachusetts|   76|
    |            Kentucky|   12|
    |           Minnesota|   42|
    |        Western Cape|    6|
    +--------------------+-----+
    
    +-------+----------+-----+
    | gender|  polarity|count|
    +-------+----------+-----+
    |   male|  POSITIVE|  280|
    |unknown|AMBIVALENT|   32|
    |   male|  NEGATIVE|  326|
    |unknown|  POSITIVE|  196|
    | female|   NEUTRAL| 1244|
    |   null|      null|  520|
    |unknown|   NEUTRAL| 1820|
    | female|  POSITIVE|  150|
    |   male|   NEUTRAL| 1446|
    | female|  NEGATIVE|  224|
    |   male|AMBIVALENT|   26|
    | female|AMBIVALENT|   42|
    |unknown|  NEGATIVE|  294|
    +-------+----------+-----+
    
    +----------+------+----+-------+
    |  polarity|female|male|unknown|
    +----------+------+----+-------+
    |   NEUTRAL|  1244|1446|   1820|
    |AMBIVALENT|    42|  26|     32|
    |  POSITIVE|   150| 280|    196|
    |  NEGATIVE|   224| 326|    294|
    +----------+------+----+-------+
    


## 2.1 Plot results using matplotlib

In Python you can use simple libraries to plot your DataFrames directly in diagrams. However, the use of matplotlib is not trivial and once the data is rendered in the diagram it is static. For more comprehensive graphing Spark provides the GraphX extension. Here the data is transformed into a directed multigraph model (similar to those used in GraphDBs) called GraphFrames. 

You will explore GraphFrames later in this lab. Let's first have a look at simply plotting your DataFrames using matplotlib.  


```python
import pandas as pd
%matplotlib inline
import matplotlib.pyplot as plt
import numpy as np
```

Plot the number of tweets per state. Notice again how Spark computes the result lazily. In no previous output did we require the full DataFrame and it did not have to get fully computed until now.


```python
tweets_state_pd = tweets_state.toPandas()
values = tweets_state_pd['count']
labels = tweets_state_pd['state']

plt.gcf().set_size_inches(16, 12, forward=True)
plt.title('Number of tweets by state')

plt.barh(range(len(values)), values)
plt.yticks(range(len(values)), labels)

plt.show()
```


![png](output_43_0.png)


More plots to group data by gender and polarity.


```python
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
```


![png](output_45_0.png)


## 2.2 Create SQL temporary tables

With Spark SQL you can create in-memory tables and query your Spark RDDs in tables using SQL syntax. This is just an alternative represenation of your RDD where SQL functions (like filters or projections) are converted into Spark functions. For the user it mostly provides a SQL wrapper over Spark and a familiar way to query data.


```python
tweetsDF.registerTempTable("tweets_DF")
```

Run SQL statements using the sqlContext.sql() function and render output with show(). The result of a SQL function could again be mapped to a data frame.


```python
sqlContext.sql("SELECT count(*) AS cnt FROM tweets_DF").show()
```

    +----+
    | cnt|
    +----+
    |6600|
    +----+
    



```python
sqlContext.sql("SELECT message.actor.displayName AS author, count(*) as cnt FROM tweets_DF GROUP BY message.actor.displayName ORDER BY cnt DESC").show(10)
```

    +--------------------+---+
    |              author|cnt|
    +--------------------+---+
    |             A.T.O.M|100|
    |Nation of Immigrants| 86|
    |               hugoe| 52|
    |    Mormon Democrats| 50|
    |        Chuck Nellis| 50|
    |Nacion De Inmigrants| 48|
    |       nelsy Benitez| 46|
    |  Halli Casser-Jayne| 46|
    |    angel villanueva| 46|
    |    Nelson B Umanzor| 46|
    +--------------------+---+
    only showing top 10 rows
    


With multiple temporary tables (potentially from different databases) you can execute JOIN and UNION queries to analyze the database in combination.

In the next query we will return all hashtags used in our body of tweets. 


```python
hashtags = sqlContext.sql("SELECT message.object.twitter_entities.hashtags.text as tags \
                FROM tweets_DF \
                WHERE message.object.twitter_entities.hashtags.text IS NOT NULL")
```

The hashtags are in lists, one per tweet. We flat map this list to a large list and then store it back into a temporary table. The temporary table can be used to define a hashtag cloud to understand which hashtag has been used how many times.


```python
l = hashtags.map(lambda x: x.tags).collect()
tagCloud = [item for sublist in l for item in sublist]
```

Create a DataFrame from the Python dictionary we used to flatten our hashtags into. The DataFrame has a simple schema with just a single column called `hastag`.


```python
from pyspark.sql import Row

tagCloudDF = sc.parallelize(tagCloud)
row = Row("hashtag")
hashtagsDF = tagCloudDF.map(row).toDF()
```

Register a new temp table for hashtags. Group and count tags to get a sense of trending issues.


```python
hashtagsDF.registerTempTable("hashtags_DF")
```


```python
trending = sqlContext.sql("SELECT count(hashtag) as CNT, hashtag as TAG FROM hashtags_DF GROUP BY hashtag ORDER BY CNT DESC")
trending.show(10)
```

    +----+-----------------+
    | CNT|              TAG|
    +----+-----------------+
    |3494|     Election2016|
    |1594|       TNTweeters|
    | 980|             AINF|
    | 900|              CIR|
    | 744|              GOP|
    | 640|       Immigrants|
    | 594|     election2016|
    | 456|ImmigrationReform|
    | 306|      MiddleClass|
    | 306|       PoorPeople|
    +----+-----------------+
    only showing top 10 rows
    


## 2.3 Visualize tag cloud with Brunel

Let's create some charts and diagrams with Brunel commands.

The basic format of each call to Brunel is simple. Whether the command is a single line or a set of lines, the commands are concatenated together and the result interpreted as one command.

Here are some of the rules for using Brunel that you'll need in this notebook:

- _DataFrame_: Use the data command to specify the pandas DataFrame.
- _Chart type_: Use commands like chord and treemap to specify a chart type. If you don't specify a type, the default chart type is a scatterplot.
- _Chart definition_: Use the x and y commands to specify the data to include on the x-axis and the y-axis.
- _Styling_: Use commands like color, tooltip, and label to control the styling of the graph.
- _Size_: Use the width and height key-value pairs to specify the size of the graph. The key-value pairs must be preceded with two colons and separated with a comma, for example: :: width=800, height=300

See detailed documentation on the Brunel Visualization language at [https://brunel.mybluemix.net/docs](https://brunel.mybluemix.net/docs).


```python
import brunel
import sys

reload(sys)
sys.setdefaultencoding('utf-8')

trending_pd = trending.toPandas()
```

Brunel libraries are able to read data from CSV files only. We will export our Panda DataFrames to CSV first to be able to load them with the Brunel libraries below.


```python
trending_pd.to_csv('trending_pd.csv')
tweets_state_pd.to_csv('tweets_state_pd.csv')
tweets_gp_pd.to_csv('tweets_gp_pd.csv')
```

Top 5 records in every Panda DF.


```python
trending_pd.head(5)
```




<div>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>CNT</th>
      <th>TAG</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>3494</td>
      <td>Election2016</td>
    </tr>
    <tr>
      <th>1</th>
      <td>1594</td>
      <td>TNTweeters</td>
    </tr>
    <tr>
      <th>2</th>
      <td>980</td>
      <td>AINF</td>
    </tr>
    <tr>
      <th>3</th>
      <td>900</td>
      <td>CIR</td>
    </tr>
    <tr>
      <th>4</th>
      <td>744</td>
      <td>GOP</td>
    </tr>
  </tbody>
</table>
</div>



The hast tag cloud is visualized using the Brunel cloud graph.


```python
%brunel data('trending_pd') cloud color(cnt) size(cnt) label(tag) :: width=900, height=600
```


<!--
  ~ Copyright (c) 2015 IBM Corporation and others.
  ~
  ~ Licensed under the Apache License, Version 2.0 (the "License");
  ~ You may not use this file except in compliance with the License.
  ~ You may obtain a copy of the License at
  ~
  ~     http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing, software
  ~ distributed under the License is distributed on an "AS IS" BASIS,
  ~ WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  ~ See the License for the specific language governing permissions and
  ~ limitations under the License.
  -->


<link rel="stylesheet" type="text/css" href="/data/jupyter2/4974bb7a-e2a6-4424-80af-f7d42ca46ba4/nbextensions/brunel_ext/Brunel.css">
<link rel="stylesheet" type="text/css" href="/data/jupyter2/4974bb7a-e2a6-4424-80af-f7d42ca46ba4/nbextensions/brunel_ext/sumoselect/sumoselect.css">

<style>
    
</style>

<svg id="visid62f5ed84-9693-11e6-9984-002590fb6500" width="900" height="600"></svg>





    <IPython.core.display.Javascript object>



State and location data can be plotted on a map or a bubble graph representing the number of tweets per state. We will exercise maps later using the GraphX framework.


```python
tweets_state_pd.head(5)
```




<div>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>state</th>
      <th>count</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>Ar Riyāḑ</td>
      <td>8</td>
    </tr>
    <tr>
      <th>1</th>
      <td>Michigan</td>
      <td>24</td>
    </tr>
    <tr>
      <th>2</th>
      <td>Kinshasa</td>
      <td>8</td>
    </tr>
    <tr>
      <th>3</th>
      <td>Texas</td>
      <td>286</td>
    </tr>
    <tr>
      <th>4</th>
      <td>Louisiana</td>
      <td>18</td>
    </tr>
  </tbody>
</table>
</div>




```python
%brunel data('tweets_state_pd') bubble label(state) x(state) color(count) size(count)
```


<!--
  ~ Copyright (c) 2015 IBM Corporation and others.
  ~
  ~ Licensed under the Apache License, Version 2.0 (the "License");
  ~ You may not use this file except in compliance with the License.
  ~ You may obtain a copy of the License at
  ~
  ~     http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing, software
  ~ distributed under the License is distributed on an "AS IS" BASIS,
  ~ WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  ~ See the License for the specific language governing permissions and
  ~ limitations under the License.
  -->


<link rel="stylesheet" type="text/css" href="/data/jupyter2/4974bb7a-e2a6-4424-80af-f7d42ca46ba4/nbextensions/brunel_ext/Brunel.css">
<link rel="stylesheet" type="text/css" href="/data/jupyter2/4974bb7a-e2a6-4424-80af-f7d42ca46ba4/nbextensions/brunel_ext/sumoselect/sumoselect.css">

<style>
    
</style>

<svg id="visid631bc9f0-9693-11e6-9984-002590fb6500" width="500" height="400"></svg>





    <IPython.core.display.Javascript object>



Brunel graphs are D3 based and interactive. Try using your mouse on the graph for Gender polarity to hover over details and zoom in on the Y axis.


```python
tweets_gp_pd.head(5)
```




<div>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>polarity</th>
      <th>female</th>
      <th>male</th>
      <th>unknown</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>NEUTRAL</td>
      <td>1244</td>
      <td>1446</td>
      <td>1820</td>
    </tr>
    <tr>
      <th>1</th>
      <td>AMBIVALENT</td>
      <td>42</td>
      <td>26</td>
      <td>32</td>
    </tr>
    <tr>
      <th>2</th>
      <td>POSITIVE</td>
      <td>150</td>
      <td>280</td>
      <td>196</td>
    </tr>
    <tr>
      <th>3</th>
      <td>NEGATIVE</td>
      <td>224</td>
      <td>326</td>
      <td>294</td>
    </tr>
  </tbody>
</table>
</div>




```python
%brunel data('tweets_gp_pd') bar x(polarity) y(male, female) color(male, female) tooltip(#all) legends(none) :: width=800, height=300
```


<!--
  ~ Copyright (c) 2015 IBM Corporation and others.
  ~
  ~ Licensed under the Apache License, Version 2.0 (the "License");
  ~ You may not use this file except in compliance with the License.
  ~ You may obtain a copy of the License at
  ~
  ~     http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing, software
  ~ distributed under the License is distributed on an "AS IS" BASIS,
  ~ WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  ~ See the License for the specific language governing permissions and
  ~ limitations under the License.
  -->


<link rel="stylesheet" type="text/css" href="/data/jupyter2/4974bb7a-e2a6-4424-80af-f7d42ca46ba4/nbextensions/brunel_ext/Brunel.css">
<link rel="stylesheet" type="text/css" href="/data/jupyter2/4974bb7a-e2a6-4424-80af-f7d42ca46ba4/nbextensions/brunel_ext/sumoselect/sumoselect.css">

<style>
    
</style>

<svg id="visid63258436-9693-11e6-9984-002590fb6500" width="800" height="300"></svg>





    <IPython.core.display.Javascript object>



## 2.4 Write analysis results back to Cloudant

Next we are going to persist the hashtags_DF back into a Cloudant database. (Note: The database `hashtags` has to exist in Cloudant. Please create that database first.)


```python
hashtagsDF.write.format("com.cloudant.spark").\
    option("cloudant.host",properties['cloudant']['account'].replace('https://','')).\
    option("cloudant.username", properties['cloudant']['username']).\
    option("cloudant.password", properties['cloudant']['password']).\
    option("bulkSize", "2000").\
save("hashtags")
```

# 3. Analysis with Spark GraphX

Import dependencies from the Pixiedust library loaded in the preperation section. See [https://github.com/ibm-cds-labs/pixiedust](https://github.com/ibm-cds-labs/pixiedust) for details.


```python
from pixiedust.display import *
```

To render a chart you have options to select the columns to display or the aggregation function to apply.


```python
tweets_state_us = tweets_state.filter(tweets_state.state.isin("Alabama", "Alaska", "Arizona", 
        "Arkansas", "California", "Colorado", "Connecticut", "Delaware", "Florida", 
        "Georgia", "Hawaii", "Idaho", "Illinois Indiana", "Iowa", "Kansas", "Kentucky", 
        "Louisiana", "Maine", "Maryland", "Massachusetts", "Michigan", "Minnesota", 
        "Mississippi", "Missouri", "Montana Nebraska", "Nevada", "New Hampshire", 
        "New Jersey", "New Mexico", "New York", "North Carolina", "North Dakota", 
        "Ohio", "Oklahoma", "Oregon", "Pennsylvania Rhode Island", "South Carolina", 
        "South Dakota", "Tennessee", "Texas","Utah", "Vermont", "Virginia", 
        "Washington", "West Virginia", "Wisconsin", "Wyoming"))
```


```python
tweets_state_us.show(5)
```


```python
display(tweets_state_us)
```


<style type="text/css">
  .output_subarea.rendered_html {
    max-width: 100%;
    padding: 0;
  }
  .pixiedust {
    border: 1px solid #ececec;
    border-radius: 3px;
    margin: 10px 0;
    padding: 10px;
    
    color: #333333;
    font-size: small;
    font-weight: 300;
    letter-spacing: 0.5px;
    line-height: normal;
  }
  .pixiedust .btn-group {
    border-right: 0;
  }
  .pixiedust .display-type-button:not(:first-child) {
    margin-left: 10px !important;
  }
  .pixiedust .btn-default {
    border-radius: 0;
  }
  .pixiedust-toolbar .btn-default.focus,
  .pixiedust-toolbar .btn-default:focus,
  .pixiedust-toolbar .btn-default:hover {
    background-color: #337ab7;
    border-color: #2e6da4;
    color: #ffffff;
  }
  .pixiedust .dropdown-menu {
    background-color: #ffffff;
    border-color: rgba(0, 0, 0, 0.15);
    padding: 0;
  }
  .pixiedust .dropdown-menu ul {
    list-style: outside none none;
    margin: 0;
    padding: 0;
  }
  .pixiedust .dropdown-menu li {
    cursor: pointer;
    padding: 8px 12px;
  }
  .pixiedust .dropdown-menu li:hover {
    background-color: #337ab7;
    color: #ffffff;
    cursor: pointer;
  }
  .pixiedust .dropdown-menu li > span {
    display: inline-block;
    margin-left: 10px;
  }
  .pixiedust .btn .fa-chevron-down {
    margin-left: 25px;
  }
  .pixiedust .btn-group.open .dropdown-toggle {
    background-color: #337ab7;
    border-color: #2e6da4;
    color: #ffffff;
  }
  .pixiedust .modal-dialog .btn-default:focus,
  .pixiedust .modal-dialog .btn-default:hover {
    background-color: #e6e6e6;
    border-color: #adadad;
  }
  .pixiedust .modal-dialog .btn-primary {
    background-color: #337ab7;
    color: #fff;
    transition: background-color 0.15s ease-out 0s;
  }
  .pixiedust .modal-dialog .btn-primary:focus,
  .pixiedust .modal-dialog .btn-primary:hover {
    background-color: #286090;
    border-color: #204d74;
  }
  .pixiedust .modal-header {
    background-color: #337ab7;
    color: #ffffff;
  }
  .pixiedust .modal-header .close {
    color: #fff;
    opacity: 1;
    text-shadow: none;
  }
  .pixiedust .modal-header .close:hover {
    opacity: 0.5;
  }
  .pixiedust .modal-body {
    padding: 25px;
  }
  .pixiedust .modal-footer .btn {
    min-width: 75px;
  }

  .pixiedust .executionTime {
    color: gray;
  }

  .pixiedust .expandButton {
    background-color: transparent;
    border: 0 none;
    font-size: 15px;
    padding: 0;
    position: relative;
    right: 0;
    text-align: right;
    top: 0;
    width: 100%;
  }
  .pixiedust .expandButton:hover,
  .pixiedust .expandButton:active,
  .pixiedust .expandButton:focus {
    background-color: transparent;
    color: #337ab7;
  }
  .pixiedust .expandButton:not(.collapsed) {
    float: right;
    width: initial;
  }
  .pixiedust .expandButton > i.fa:before {
    content: "\f065";
  }
  .pixiedust .expandButton.collapsed > i.fa:before {
    content: "\f066";
  }
  .pixiedust .expandButton.collapsed:before {
    color: #999999;
    content: "Pixiedust output minimized";
    float: left;
    font-size: 14px;
    font-style: italic;
    font-weight: 300;
  }
</style>

<div class="pixiedust">
  <button class="btn btn-default btn-sm expandButton" data-toggle="collapse"
          data-target="#pixiedust-output-wrapper-16cc7553" title="Expand/Collapse Pixiedust output">
    <i class="fa"></i>
  </button>
  <div id="pixiedust-output-wrapper-16cc7553" class="collapse in">
    
      <div class="pixiedust-toolbar btn-group" role="group" style="margin-bottom:15px">  
      
        
          <a class="btn btn-small btn-default display-type-button" id="menu16cc7553-dataframe" title="DataFrame Table">
            <i class="fa fa-table"></i>
          </a>
          
            <script>
                $('#menu16cc7553-dataframe').on('click', 

function() {
    cellId = typeof cellId === "undefined" ? "" : cellId;
    var curCell=IPython.notebook.get_cells().filter(function(cell){
        return cell.cell_id=="9C2BFB65DD1F456D889381A547DA85CD".replace("cellId",cellId);
    });
    curCell=curCell.length>0?curCell[0]:null;
    console.log("curCell",curCell);
    var startWallToWall;
    //Resend the display command
    var callbacks = {
        iopub:{
            output:function(msg){
                console.log("msg", msg);
                if (false){
                    return curCell.output_area.handle_output.apply(curCell.output_area, arguments);
                }
                var msg_type=msg.header.msg_type;
                var content = msg.content;
                var executionTime = $("#execution16cc7553");
                if(msg_type==="stream"){
                    $('#wrapperHTML16cc7553').html(content.text);
                }else if (msg_type==="display_data" || msg_type==="execute_result"){
                    var html=null;
                    if (!!content.data["text/html"]){
                        html=content.data["text/html"];
                    }else if (!!content.data["image/png"]){
                        html=html||"";
                        html+="<img src='data:image/png;base64," +content.data["image/png"]+"'></img>";
                    }
                                                    
                    if (!!content.data["application/javascript"]){
                        try{
                            $('#wrapperJS16cc7553').html("<script type=\\\"text/javascript\\\">"+content.data["application/javascript"]+"</s" + "cript>");
                        }catch(e){
                            console.log("Invalid javascript output",e, content.data);
                        }
                    }
                    
                    if (html){
                        if(curCell&&curCell.output_area&&curCell.output_area.outputs){
                            var data = JSON.parse(JSON.stringify(content.data));
                            if(!!data["text/html"])data["text/html"]=html;
                            curCell.output_area.outputs.push({"data": data,"metadata":content.metadata,"output_type":msg_type});
                        }
                        try{
                            $('#wrapperHTML16cc7553').html(html);
                        }catch(e){
                            console.log("Invalid html output", e, html);
                            $('#wrapperHTML16cc7553').html( "Invalid html output. <pre>" 
                                + html.replace(/>/g,'&gt;').replace(/</g,'&lt;').replace(/"/g,'&quot;') + "<pre>");
                        }
                    }
                }else if (msg_type === "error") {
                    require(['base/js/utils'], function(utils) {
                        var tb = content.traceback;
                        console.log("tb",tb);
                        if (tb && tb.length>0){
                            var data = tb.reduce(function(res, frame){return res+frame+'\\n';},"");
                            console.log("data",data);
                            data = utils.fixConsole(data);
                            data = utils.fixCarriageReturn(data);
                            data = utils.autoLinkUrls(data);
                            $('#wrapperHTML16cc7553').html("<pre>" + data +"</pre>");
                        }
                    });
                }

                //Append profiling info
                if (executionTime.length > 0 && $("#execution16cc7553").length == 0 ){
                    $('#wrapperHTML16cc7553').append(executionTime);
                }else if (startWallToWall && $("#execution16cc7553").length > 0 ){
                    $("#execution16cc7553").append($("<div/>").text("Wall to Wall time: " + ( (new Date().getTime() - startWallToWall)/1000 ) + "s"));
                }
            }
        }
    }
    
    if (IPython && IPython.notebook && IPython.notebook.session && IPython.notebook.session.kernel){
        var command = "display(tweets_state_us,cell_id='9C2BFB65DD1F456D889381A547DA85CD',handlerId='dataframe',mapDisplayMode='markers',valueFields='count',keyFields='state',aggregation='MAX',mapRegion='US',prefix='16cc7553')".replace("cellId",cellId);
        function addOptions(options){
            function getStringRep(v) {
                if (!isNaN(parseFloat(v)) && isFinite(v)){
                    return v.toString();
                }
                return "'" + v + "'";
            }
            for (var key in options){
                var value = options[key];
                var hasValue = value != null && typeof value !== 'undefined' && value !== '';
                var replaceValue = hasValue ? (key+"=" + getStringRep(value) ) : "";
                var pattern = (hasValue?"":",")+"\\s*" + key + "\\s*=\\s*'(\\\\'|[^'])*'";
                var rpattern=new RegExp(pattern);
                var n = command.search(rpattern);
                if ( n >= 0 ){
                    command = command.replace(rpattern, replaceValue);
                }else if (hasValue){
                    var n = command.lastIndexOf(")");
                    command = [command.slice(0, n), (command[n-1]=="("? "":",") + replaceValue, command.slice(n)].join('')
                }        
            }
        }
        if(typeof cellMetadata != "undefined" && cellMetadata.displayParams){
            addOptions(cellMetadata.displayParams);
            addOptions({"showchrome":"true"});
        }else if ('False'=='True' && curCell && curCell._metadata.pixiedust ){
            addOptions(curCell._metadata.pixiedust.displayParams || {} );
        }
        addOptions({});
        
        
    

        console.log("Running command2",command);
        var pattern = "\\w*\\s*=\\s*'(\\\\'|[^'])*'";
        var rpattern=new RegExp(pattern,"g");
        var n = command.match(rpattern);
        var displayParams={}
        for (var i = 0; i < n.length; i++){
            var parts=n[i].split("=");
            var key = parts[0].trim();
            var value = parts[1].trim()
            if (!key.startsWith("nostore_") && key != "showchrome" && key != "prefix" && key != "cell_id"){
                displayParams[key] = value.substring(1,value.length-1);
            }
        }
        if(curCell&&curCell.output_area){
            curCell._metadata.pixiedust = curCell._metadata.pixiedust || {}
            curCell._metadata.pixiedust.displayParams=displayParams
            curCell.output_area.outputs=[];
        }
        $('#wrapperJS16cc7553').html("")
        $('#wrapperHTML16cc7553').html('<div style="width:100px;height:60px;left:47%;position:relative"><i class="fa fa-circle-o-notch fa-spin" style="font-size:48px"></i></div>'+
        '<div style="text-align:center">Loading your data. Please wait...</div>');
        startWallToWall = new Date().getTime();
        IPython.notebook.session.kernel.execute(command, callbacks, {silent:false,store_history:false,stop_on_error:true});
    }
}
)
            </script>
        
        
      
        
          <div class="btn-group btn-small display-type-button">
            
            <a class="btn btn-small dropdown-toggle btn-default" data-toggle="dropdown" title="Chart">
              
              <i class="fa fa-line-chart"></i>
              
              <i class="fa fa-chevron-down"></i>
            </a>
            <div class="dropdown-menu" role="menu" style="white-space:nowrap">
              <ul>
              
                <li id="menu16cc7553-barChart">
                  
                  <i class="fa fa-bar-chart"></i>
                  
                  <span>Bar Chart</span>
                  
            <script>
                $('#menu16cc7553-barChart').on('click', 

function() {
    cellId = typeof cellId === "undefined" ? "" : cellId;
    var curCell=IPython.notebook.get_cells().filter(function(cell){
        return cell.cell_id=="9C2BFB65DD1F456D889381A547DA85CD".replace("cellId",cellId);
    });
    curCell=curCell.length>0?curCell[0]:null;
    console.log("curCell",curCell);
    var startWallToWall;
    //Resend the display command
    var callbacks = {
        iopub:{
            output:function(msg){
                console.log("msg", msg);
                if (false){
                    return curCell.output_area.handle_output.apply(curCell.output_area, arguments);
                }
                var msg_type=msg.header.msg_type;
                var content = msg.content;
                var executionTime = $("#execution16cc7553");
                if(msg_type==="stream"){
                    $('#wrapperHTML16cc7553').html(content.text);
                }else if (msg_type==="display_data" || msg_type==="execute_result"){
                    var html=null;
                    if (!!content.data["text/html"]){
                        html=content.data["text/html"];
                    }else if (!!content.data["image/png"]){
                        html=html||"";
                        html+="<img src='data:image/png;base64," +content.data["image/png"]+"'></img>";
                    }
                                                    
                    if (!!content.data["application/javascript"]){
                        try{
                            $('#wrapperJS16cc7553').html("<script type=\\\"text/javascript\\\">"+content.data["application/javascript"]+"</s" + "cript>");
                        }catch(e){
                            console.log("Invalid javascript output",e, content.data);
                        }
                    }
                    
                    if (html){
                        if(curCell&&curCell.output_area&&curCell.output_area.outputs){
                            var data = JSON.parse(JSON.stringify(content.data));
                            if(!!data["text/html"])data["text/html"]=html;
                            curCell.output_area.outputs.push({"data": data,"metadata":content.metadata,"output_type":msg_type});
                        }
                        try{
                            $('#wrapperHTML16cc7553').html(html);
                        }catch(e){
                            console.log("Invalid html output", e, html);
                            $('#wrapperHTML16cc7553').html( "Invalid html output. <pre>" 
                                + html.replace(/>/g,'&gt;').replace(/</g,'&lt;').replace(/"/g,'&quot;') + "<pre>");
                        }
                    }
                }else if (msg_type === "error") {
                    require(['base/js/utils'], function(utils) {
                        var tb = content.traceback;
                        console.log("tb",tb);
                        if (tb && tb.length>0){
                            var data = tb.reduce(function(res, frame){return res+frame+'\\n';},"");
                            console.log("data",data);
                            data = utils.fixConsole(data);
                            data = utils.fixCarriageReturn(data);
                            data = utils.autoLinkUrls(data);
                            $('#wrapperHTML16cc7553').html("<pre>" + data +"</pre>");
                        }
                    });
                }

                //Append profiling info
                if (executionTime.length > 0 && $("#execution16cc7553").length == 0 ){
                    $('#wrapperHTML16cc7553').append(executionTime);
                }else if (startWallToWall && $("#execution16cc7553").length > 0 ){
                    $("#execution16cc7553").append($("<div/>").text("Wall to Wall time: " + ( (new Date().getTime() - startWallToWall)/1000 ) + "s"));
                }
            }
        }
    }
    
    if (IPython && IPython.notebook && IPython.notebook.session && IPython.notebook.session.kernel){
        var command = "display(tweets_state_us,cell_id='9C2BFB65DD1F456D889381A547DA85CD',handlerId='barChart',mapDisplayMode='markers',valueFields='count',keyFields='state',aggregation='MAX',mapRegion='US',prefix='16cc7553')".replace("cellId",cellId);
        function addOptions(options){
            function getStringRep(v) {
                if (!isNaN(parseFloat(v)) && isFinite(v)){
                    return v.toString();
                }
                return "'" + v + "'";
            }
            for (var key in options){
                var value = options[key];
                var hasValue = value != null && typeof value !== 'undefined' && value !== '';
                var replaceValue = hasValue ? (key+"=" + getStringRep(value) ) : "";
                var pattern = (hasValue?"":",")+"\\s*" + key + "\\s*=\\s*'(\\\\'|[^'])*'";
                var rpattern=new RegExp(pattern);
                var n = command.search(rpattern);
                if ( n >= 0 ){
                    command = command.replace(rpattern, replaceValue);
                }else if (hasValue){
                    var n = command.lastIndexOf(")");
                    command = [command.slice(0, n), (command[n-1]=="("? "":",") + replaceValue, command.slice(n)].join('')
                }        
            }
        }
        if(typeof cellMetadata != "undefined" && cellMetadata.displayParams){
            addOptions(cellMetadata.displayParams);
            addOptions({"showchrome":"true"});
        }else if ('False'=='True' && curCell && curCell._metadata.pixiedust ){
            addOptions(curCell._metadata.pixiedust.displayParams || {} );
        }
        addOptions({});
        
        
    

        console.log("Running command2",command);
        var pattern = "\\w*\\s*=\\s*'(\\\\'|[^'])*'";
        var rpattern=new RegExp(pattern,"g");
        var n = command.match(rpattern);
        var displayParams={}
        for (var i = 0; i < n.length; i++){
            var parts=n[i].split("=");
            var key = parts[0].trim();
            var value = parts[1].trim()
            if (!key.startsWith("nostore_") && key != "showchrome" && key != "prefix" && key != "cell_id"){
                displayParams[key] = value.substring(1,value.length-1);
            }
        }
        if(curCell&&curCell.output_area){
            curCell._metadata.pixiedust = curCell._metadata.pixiedust || {}
            curCell._metadata.pixiedust.displayParams=displayParams
            curCell.output_area.outputs=[];
        }
        $('#wrapperJS16cc7553').html("")
        $('#wrapperHTML16cc7553').html('<div style="width:100px;height:60px;left:47%;position:relative"><i class="fa fa-circle-o-notch fa-spin" style="font-size:48px"></i></div>'+
        '<div style="text-align:center">Loading your data. Please wait...</div>');
        startWallToWall = new Date().getTime();
        IPython.notebook.session.kernel.execute(command, callbacks, {silent:false,store_history:false,stop_on_error:true});
    }
}
)
            </script>
        
                </li>
              
                <li id="menu16cc7553-lineChart">
                  
                  <i class="fa fa-line-chart"></i>
                  
                  <span>Line Chart</span>
                  
            <script>
                $('#menu16cc7553-lineChart').on('click', 

function() {
    cellId = typeof cellId === "undefined" ? "" : cellId;
    var curCell=IPython.notebook.get_cells().filter(function(cell){
        return cell.cell_id=="9C2BFB65DD1F456D889381A547DA85CD".replace("cellId",cellId);
    });
    curCell=curCell.length>0?curCell[0]:null;
    console.log("curCell",curCell);
    var startWallToWall;
    //Resend the display command
    var callbacks = {
        iopub:{
            output:function(msg){
                console.log("msg", msg);
                if (false){
                    return curCell.output_area.handle_output.apply(curCell.output_area, arguments);
                }
                var msg_type=msg.header.msg_type;
                var content = msg.content;
                var executionTime = $("#execution16cc7553");
                if(msg_type==="stream"){
                    $('#wrapperHTML16cc7553').html(content.text);
                }else if (msg_type==="display_data" || msg_type==="execute_result"){
                    var html=null;
                    if (!!content.data["text/html"]){
                        html=content.data["text/html"];
                    }else if (!!content.data["image/png"]){
                        html=html||"";
                        html+="<img src='data:image/png;base64," +content.data["image/png"]+"'></img>";
                    }
                                                    
                    if (!!content.data["application/javascript"]){
                        try{
                            $('#wrapperJS16cc7553').html("<script type=\\\"text/javascript\\\">"+content.data["application/javascript"]+"</s" + "cript>");
                        }catch(e){
                            console.log("Invalid javascript output",e, content.data);
                        }
                    }
                    
                    if (html){
                        if(curCell&&curCell.output_area&&curCell.output_area.outputs){
                            var data = JSON.parse(JSON.stringify(content.data));
                            if(!!data["text/html"])data["text/html"]=html;
                            curCell.output_area.outputs.push({"data": data,"metadata":content.metadata,"output_type":msg_type});
                        }
                        try{
                            $('#wrapperHTML16cc7553').html(html);
                        }catch(e){
                            console.log("Invalid html output", e, html);
                            $('#wrapperHTML16cc7553').html( "Invalid html output. <pre>" 
                                + html.replace(/>/g,'&gt;').replace(/</g,'&lt;').replace(/"/g,'&quot;') + "<pre>");
                        }
                    }
                }else if (msg_type === "error") {
                    require(['base/js/utils'], function(utils) {
                        var tb = content.traceback;
                        console.log("tb",tb);
                        if (tb && tb.length>0){
                            var data = tb.reduce(function(res, frame){return res+frame+'\\n';},"");
                            console.log("data",data);
                            data = utils.fixConsole(data);
                            data = utils.fixCarriageReturn(data);
                            data = utils.autoLinkUrls(data);
                            $('#wrapperHTML16cc7553').html("<pre>" + data +"</pre>");
                        }
                    });
                }

                //Append profiling info
                if (executionTime.length > 0 && $("#execution16cc7553").length == 0 ){
                    $('#wrapperHTML16cc7553').append(executionTime);
                }else if (startWallToWall && $("#execution16cc7553").length > 0 ){
                    $("#execution16cc7553").append($("<div/>").text("Wall to Wall time: " + ( (new Date().getTime() - startWallToWall)/1000 ) + "s"));
                }
            }
        }
    }
    
    if (IPython && IPython.notebook && IPython.notebook.session && IPython.notebook.session.kernel){
        var command = "display(tweets_state_us,cell_id='9C2BFB65DD1F456D889381A547DA85CD',handlerId='lineChart',mapDisplayMode='markers',valueFields='count',keyFields='state',aggregation='MAX',mapRegion='US',prefix='16cc7553')".replace("cellId",cellId);
        function addOptions(options){
            function getStringRep(v) {
                if (!isNaN(parseFloat(v)) && isFinite(v)){
                    return v.toString();
                }
                return "'" + v + "'";
            }
            for (var key in options){
                var value = options[key];
                var hasValue = value != null && typeof value !== 'undefined' && value !== '';
                var replaceValue = hasValue ? (key+"=" + getStringRep(value) ) : "";
                var pattern = (hasValue?"":",")+"\\s*" + key + "\\s*=\\s*'(\\\\'|[^'])*'";
                var rpattern=new RegExp(pattern);
                var n = command.search(rpattern);
                if ( n >= 0 ){
                    command = command.replace(rpattern, replaceValue);
                }else if (hasValue){
                    var n = command.lastIndexOf(")");
                    command = [command.slice(0, n), (command[n-1]=="("? "":",") + replaceValue, command.slice(n)].join('')
                }        
            }
        }
        if(typeof cellMetadata != "undefined" && cellMetadata.displayParams){
            addOptions(cellMetadata.displayParams);
            addOptions({"showchrome":"true"});
        }else if ('False'=='True' && curCell && curCell._metadata.pixiedust ){
            addOptions(curCell._metadata.pixiedust.displayParams || {} );
        }
        addOptions({});
        
        
    

        console.log("Running command2",command);
        var pattern = "\\w*\\s*=\\s*'(\\\\'|[^'])*'";
        var rpattern=new RegExp(pattern,"g");
        var n = command.match(rpattern);
        var displayParams={}
        for (var i = 0; i < n.length; i++){
            var parts=n[i].split("=");
            var key = parts[0].trim();
            var value = parts[1].trim()
            if (!key.startsWith("nostore_") && key != "showchrome" && key != "prefix" && key != "cell_id"){
                displayParams[key] = value.substring(1,value.length-1);
            }
        }
        if(curCell&&curCell.output_area){
            curCell._metadata.pixiedust = curCell._metadata.pixiedust || {}
            curCell._metadata.pixiedust.displayParams=displayParams
            curCell.output_area.outputs=[];
        }
        $('#wrapperJS16cc7553').html("")
        $('#wrapperHTML16cc7553').html('<div style="width:100px;height:60px;left:47%;position:relative"><i class="fa fa-circle-o-notch fa-spin" style="font-size:48px"></i></div>'+
        '<div style="text-align:center">Loading your data. Please wait...</div>');
        startWallToWall = new Date().getTime();
        IPython.notebook.session.kernel.execute(command, callbacks, {silent:false,store_history:false,stop_on_error:true});
    }
}
)
            </script>
        
                </li>
              
                <li id="menu16cc7553-scatterPlot">
                  
                  <i class="fa fa-circle"></i>
                  
                  <span>Scatter Plot</span>
                  
            <script>
                $('#menu16cc7553-scatterPlot').on('click', 

function() {
    cellId = typeof cellId === "undefined" ? "" : cellId;
    var curCell=IPython.notebook.get_cells().filter(function(cell){
        return cell.cell_id=="9C2BFB65DD1F456D889381A547DA85CD".replace("cellId",cellId);
    });
    curCell=curCell.length>0?curCell[0]:null;
    console.log("curCell",curCell);
    var startWallToWall;
    //Resend the display command
    var callbacks = {
        iopub:{
            output:function(msg){
                console.log("msg", msg);
                if (false){
                    return curCell.output_area.handle_output.apply(curCell.output_area, arguments);
                }
                var msg_type=msg.header.msg_type;
                var content = msg.content;
                var executionTime = $("#execution16cc7553");
                if(msg_type==="stream"){
                    $('#wrapperHTML16cc7553').html(content.text);
                }else if (msg_type==="display_data" || msg_type==="execute_result"){
                    var html=null;
                    if (!!content.data["text/html"]){
                        html=content.data["text/html"];
                    }else if (!!content.data["image/png"]){
                        html=html||"";
                        html+="<img src='data:image/png;base64," +content.data["image/png"]+"'></img>";
                    }
                                                    
                    if (!!content.data["application/javascript"]){
                        try{
                            $('#wrapperJS16cc7553').html("<script type=\\\"text/javascript\\\">"+content.data["application/javascript"]+"</s" + "cript>");
                        }catch(e){
                            console.log("Invalid javascript output",e, content.data);
                        }
                    }
                    
                    if (html){
                        if(curCell&&curCell.output_area&&curCell.output_area.outputs){
                            var data = JSON.parse(JSON.stringify(content.data));
                            if(!!data["text/html"])data["text/html"]=html;
                            curCell.output_area.outputs.push({"data": data,"metadata":content.metadata,"output_type":msg_type});
                        }
                        try{
                            $('#wrapperHTML16cc7553').html(html);
                        }catch(e){
                            console.log("Invalid html output", e, html);
                            $('#wrapperHTML16cc7553').html( "Invalid html output. <pre>" 
                                + html.replace(/>/g,'&gt;').replace(/</g,'&lt;').replace(/"/g,'&quot;') + "<pre>");
                        }
                    }
                }else if (msg_type === "error") {
                    require(['base/js/utils'], function(utils) {
                        var tb = content.traceback;
                        console.log("tb",tb);
                        if (tb && tb.length>0){
                            var data = tb.reduce(function(res, frame){return res+frame+'\\n';},"");
                            console.log("data",data);
                            data = utils.fixConsole(data);
                            data = utils.fixCarriageReturn(data);
                            data = utils.autoLinkUrls(data);
                            $('#wrapperHTML16cc7553').html("<pre>" + data +"</pre>");
                        }
                    });
                }

                //Append profiling info
                if (executionTime.length > 0 && $("#execution16cc7553").length == 0 ){
                    $('#wrapperHTML16cc7553').append(executionTime);
                }else if (startWallToWall && $("#execution16cc7553").length > 0 ){
                    $("#execution16cc7553").append($("<div/>").text("Wall to Wall time: " + ( (new Date().getTime() - startWallToWall)/1000 ) + "s"));
                }
            }
        }
    }
    
    if (IPython && IPython.notebook && IPython.notebook.session && IPython.notebook.session.kernel){
        var command = "display(tweets_state_us,cell_id='9C2BFB65DD1F456D889381A547DA85CD',handlerId='scatterPlot',mapDisplayMode='markers',valueFields='count',keyFields='state',aggregation='MAX',mapRegion='US',prefix='16cc7553')".replace("cellId",cellId);
        function addOptions(options){
            function getStringRep(v) {
                if (!isNaN(parseFloat(v)) && isFinite(v)){
                    return v.toString();
                }
                return "'" + v + "'";
            }
            for (var key in options){
                var value = options[key];
                var hasValue = value != null && typeof value !== 'undefined' && value !== '';
                var replaceValue = hasValue ? (key+"=" + getStringRep(value) ) : "";
                var pattern = (hasValue?"":",")+"\\s*" + key + "\\s*=\\s*'(\\\\'|[^'])*'";
                var rpattern=new RegExp(pattern);
                var n = command.search(rpattern);
                if ( n >= 0 ){
                    command = command.replace(rpattern, replaceValue);
                }else if (hasValue){
                    var n = command.lastIndexOf(")");
                    command = [command.slice(0, n), (command[n-1]=="("? "":",") + replaceValue, command.slice(n)].join('')
                }        
            }
        }
        if(typeof cellMetadata != "undefined" && cellMetadata.displayParams){
            addOptions(cellMetadata.displayParams);
            addOptions({"showchrome":"true"});
        }else if ('False'=='True' && curCell && curCell._metadata.pixiedust ){
            addOptions(curCell._metadata.pixiedust.displayParams || {} );
        }
        addOptions({});
        
        
    

        console.log("Running command2",command);
        var pattern = "\\w*\\s*=\\s*'(\\\\'|[^'])*'";
        var rpattern=new RegExp(pattern,"g");
        var n = command.match(rpattern);
        var displayParams={}
        for (var i = 0; i < n.length; i++){
            var parts=n[i].split("=");
            var key = parts[0].trim();
            var value = parts[1].trim()
            if (!key.startsWith("nostore_") && key != "showchrome" && key != "prefix" && key != "cell_id"){
                displayParams[key] = value.substring(1,value.length-1);
            }
        }
        if(curCell&&curCell.output_area){
            curCell._metadata.pixiedust = curCell._metadata.pixiedust || {}
            curCell._metadata.pixiedust.displayParams=displayParams
            curCell.output_area.outputs=[];
        }
        $('#wrapperJS16cc7553').html("")
        $('#wrapperHTML16cc7553').html('<div style="width:100px;height:60px;left:47%;position:relative"><i class="fa fa-circle-o-notch fa-spin" style="font-size:48px"></i></div>'+
        '<div style="text-align:center">Loading your data. Please wait...</div>');
        startWallToWall = new Date().getTime();
        IPython.notebook.session.kernel.execute(command, callbacks, {silent:false,store_history:false,stop_on_error:true});
    }
}
)
            </script>
        
                </li>
              
                <li id="menu16cc7553-pieChart">
                  
                  <i class="fa fa-pie-chart"></i>
                  
                  <span>Pie Chart</span>
                  
            <script>
                $('#menu16cc7553-pieChart').on('click', 

function() {
    cellId = typeof cellId === "undefined" ? "" : cellId;
    var curCell=IPython.notebook.get_cells().filter(function(cell){
        return cell.cell_id=="9C2BFB65DD1F456D889381A547DA85CD".replace("cellId",cellId);
    });
    curCell=curCell.length>0?curCell[0]:null;
    console.log("curCell",curCell);
    var startWallToWall;
    //Resend the display command
    var callbacks = {
        iopub:{
            output:function(msg){
                console.log("msg", msg);
                if (false){
                    return curCell.output_area.handle_output.apply(curCell.output_area, arguments);
                }
                var msg_type=msg.header.msg_type;
                var content = msg.content;
                var executionTime = $("#execution16cc7553");
                if(msg_type==="stream"){
                    $('#wrapperHTML16cc7553').html(content.text);
                }else if (msg_type==="display_data" || msg_type==="execute_result"){
                    var html=null;
                    if (!!content.data["text/html"]){
                        html=content.data["text/html"];
                    }else if (!!content.data["image/png"]){
                        html=html||"";
                        html+="<img src='data:image/png;base64," +content.data["image/png"]+"'></img>";
                    }
                                                    
                    if (!!content.data["application/javascript"]){
                        try{
                            $('#wrapperJS16cc7553').html("<script type=\\\"text/javascript\\\">"+content.data["application/javascript"]+"</s" + "cript>");
                        }catch(e){
                            console.log("Invalid javascript output",e, content.data);
                        }
                    }
                    
                    if (html){
                        if(curCell&&curCell.output_area&&curCell.output_area.outputs){
                            var data = JSON.parse(JSON.stringify(content.data));
                            if(!!data["text/html"])data["text/html"]=html;
                            curCell.output_area.outputs.push({"data": data,"metadata":content.metadata,"output_type":msg_type});
                        }
                        try{
                            $('#wrapperHTML16cc7553').html(html);
                        }catch(e){
                            console.log("Invalid html output", e, html);
                            $('#wrapperHTML16cc7553').html( "Invalid html output. <pre>" 
                                + html.replace(/>/g,'&gt;').replace(/</g,'&lt;').replace(/"/g,'&quot;') + "<pre>");
                        }
                    }
                }else if (msg_type === "error") {
                    require(['base/js/utils'], function(utils) {
                        var tb = content.traceback;
                        console.log("tb",tb);
                        if (tb && tb.length>0){
                            var data = tb.reduce(function(res, frame){return res+frame+'\\n';},"");
                            console.log("data",data);
                            data = utils.fixConsole(data);
                            data = utils.fixCarriageReturn(data);
                            data = utils.autoLinkUrls(data);
                            $('#wrapperHTML16cc7553').html("<pre>" + data +"</pre>");
                        }
                    });
                }

                //Append profiling info
                if (executionTime.length > 0 && $("#execution16cc7553").length == 0 ){
                    $('#wrapperHTML16cc7553').append(executionTime);
                }else if (startWallToWall && $("#execution16cc7553").length > 0 ){
                    $("#execution16cc7553").append($("<div/>").text("Wall to Wall time: " + ( (new Date().getTime() - startWallToWall)/1000 ) + "s"));
                }
            }
        }
    }
    
    if (IPython && IPython.notebook && IPython.notebook.session && IPython.notebook.session.kernel){
        var command = "display(tweets_state_us,cell_id='9C2BFB65DD1F456D889381A547DA85CD',handlerId='pieChart',mapDisplayMode='markers',valueFields='count',keyFields='state',aggregation='MAX',mapRegion='US',prefix='16cc7553')".replace("cellId",cellId);
        function addOptions(options){
            function getStringRep(v) {
                if (!isNaN(parseFloat(v)) && isFinite(v)){
                    return v.toString();
                }
                return "'" + v + "'";
            }
            for (var key in options){
                var value = options[key];
                var hasValue = value != null && typeof value !== 'undefined' && value !== '';
                var replaceValue = hasValue ? (key+"=" + getStringRep(value) ) : "";
                var pattern = (hasValue?"":",")+"\\s*" + key + "\\s*=\\s*'(\\\\'|[^'])*'";
                var rpattern=new RegExp(pattern);
                var n = command.search(rpattern);
                if ( n >= 0 ){
                    command = command.replace(rpattern, replaceValue);
                }else if (hasValue){
                    var n = command.lastIndexOf(")");
                    command = [command.slice(0, n), (command[n-1]=="("? "":",") + replaceValue, command.slice(n)].join('')
                }        
            }
        }
        if(typeof cellMetadata != "undefined" && cellMetadata.displayParams){
            addOptions(cellMetadata.displayParams);
            addOptions({"showchrome":"true"});
        }else if ('False'=='True' && curCell && curCell._metadata.pixiedust ){
            addOptions(curCell._metadata.pixiedust.displayParams || {} );
        }
        addOptions({});
        
        
    

        console.log("Running command2",command);
        var pattern = "\\w*\\s*=\\s*'(\\\\'|[^'])*'";
        var rpattern=new RegExp(pattern,"g");
        var n = command.match(rpattern);
        var displayParams={}
        for (var i = 0; i < n.length; i++){
            var parts=n[i].split("=");
            var key = parts[0].trim();
            var value = parts[1].trim()
            if (!key.startsWith("nostore_") && key != "showchrome" && key != "prefix" && key != "cell_id"){
                displayParams[key] = value.substring(1,value.length-1);
            }
        }
        if(curCell&&curCell.output_area){
            curCell._metadata.pixiedust = curCell._metadata.pixiedust || {}
            curCell._metadata.pixiedust.displayParams=displayParams
            curCell.output_area.outputs=[];
        }
        $('#wrapperJS16cc7553').html("")
        $('#wrapperHTML16cc7553').html('<div style="width:100px;height:60px;left:47%;position:relative"><i class="fa fa-circle-o-notch fa-spin" style="font-size:48px"></i></div>'+
        '<div style="text-align:center">Loading your data. Please wait...</div>');
        startWallToWall = new Date().getTime();
        IPython.notebook.session.kernel.execute(command, callbacks, {silent:false,store_history:false,stop_on_error:true});
    }
}
)
            </script>
        
                </li>
              
                <li id="menu16cc7553-mapChart">
                  
                  <i class="fa fa-globe"></i>
                  
                  <span>Map</span>
                  
            <script>
                $('#menu16cc7553-mapChart').on('click', 

function() {
    cellId = typeof cellId === "undefined" ? "" : cellId;
    var curCell=IPython.notebook.get_cells().filter(function(cell){
        return cell.cell_id=="9C2BFB65DD1F456D889381A547DA85CD".replace("cellId",cellId);
    });
    curCell=curCell.length>0?curCell[0]:null;
    console.log("curCell",curCell);
    var startWallToWall;
    //Resend the display command
    var callbacks = {
        iopub:{
            output:function(msg){
                console.log("msg", msg);
                if (false){
                    return curCell.output_area.handle_output.apply(curCell.output_area, arguments);
                }
                var msg_type=msg.header.msg_type;
                var content = msg.content;
                var executionTime = $("#execution16cc7553");
                if(msg_type==="stream"){
                    $('#wrapperHTML16cc7553').html(content.text);
                }else if (msg_type==="display_data" || msg_type==="execute_result"){
                    var html=null;
                    if (!!content.data["text/html"]){
                        html=content.data["text/html"];
                    }else if (!!content.data["image/png"]){
                        html=html||"";
                        html+="<img src='data:image/png;base64," +content.data["image/png"]+"'></img>";
                    }
                                                    
                    if (!!content.data["application/javascript"]){
                        try{
                            $('#wrapperJS16cc7553').html("<script type=\\\"text/javascript\\\">"+content.data["application/javascript"]+"</s" + "cript>");
                        }catch(e){
                            console.log("Invalid javascript output",e, content.data);
                        }
                    }
                    
                    if (html){
                        if(curCell&&curCell.output_area&&curCell.output_area.outputs){
                            var data = JSON.parse(JSON.stringify(content.data));
                            if(!!data["text/html"])data["text/html"]=html;
                            curCell.output_area.outputs.push({"data": data,"metadata":content.metadata,"output_type":msg_type});
                        }
                        try{
                            $('#wrapperHTML16cc7553').html(html);
                        }catch(e){
                            console.log("Invalid html output", e, html);
                            $('#wrapperHTML16cc7553').html( "Invalid html output. <pre>" 
                                + html.replace(/>/g,'&gt;').replace(/</g,'&lt;').replace(/"/g,'&quot;') + "<pre>");
                        }
                    }
                }else if (msg_type === "error") {
                    require(['base/js/utils'], function(utils) {
                        var tb = content.traceback;
                        console.log("tb",tb);
                        if (tb && tb.length>0){
                            var data = tb.reduce(function(res, frame){return res+frame+'\\n';},"");
                            console.log("data",data);
                            data = utils.fixConsole(data);
                            data = utils.fixCarriageReturn(data);
                            data = utils.autoLinkUrls(data);
                            $('#wrapperHTML16cc7553').html("<pre>" + data +"</pre>");
                        }
                    });
                }

                //Append profiling info
                if (executionTime.length > 0 && $("#execution16cc7553").length == 0 ){
                    $('#wrapperHTML16cc7553').append(executionTime);
                }else if (startWallToWall && $("#execution16cc7553").length > 0 ){
                    $("#execution16cc7553").append($("<div/>").text("Wall to Wall time: " + ( (new Date().getTime() - startWallToWall)/1000 ) + "s"));
                }
            }
        }
    }
    
    if (IPython && IPython.notebook && IPython.notebook.session && IPython.notebook.session.kernel){
        var command = "display(tweets_state_us,cell_id='9C2BFB65DD1F456D889381A547DA85CD',handlerId='mapChart',mapDisplayMode='markers',valueFields='count',keyFields='state',aggregation='MAX',mapRegion='US',prefix='16cc7553')".replace("cellId",cellId);
        function addOptions(options){
            function getStringRep(v) {
                if (!isNaN(parseFloat(v)) && isFinite(v)){
                    return v.toString();
                }
                return "'" + v + "'";
            }
            for (var key in options){
                var value = options[key];
                var hasValue = value != null && typeof value !== 'undefined' && value !== '';
                var replaceValue = hasValue ? (key+"=" + getStringRep(value) ) : "";
                var pattern = (hasValue?"":",")+"\\s*" + key + "\\s*=\\s*'(\\\\'|[^'])*'";
                var rpattern=new RegExp(pattern);
                var n = command.search(rpattern);
                if ( n >= 0 ){
                    command = command.replace(rpattern, replaceValue);
                }else if (hasValue){
                    var n = command.lastIndexOf(")");
                    command = [command.slice(0, n), (command[n-1]=="("? "":",") + replaceValue, command.slice(n)].join('')
                }        
            }
        }
        if(typeof cellMetadata != "undefined" && cellMetadata.displayParams){
            addOptions(cellMetadata.displayParams);
            addOptions({"showchrome":"true"});
        }else if ('False'=='True' && curCell && curCell._metadata.pixiedust ){
            addOptions(curCell._metadata.pixiedust.displayParams || {} );
        }
        addOptions({});
        
        
    

        console.log("Running command2",command);
        var pattern = "\\w*\\s*=\\s*'(\\\\'|[^'])*'";
        var rpattern=new RegExp(pattern,"g");
        var n = command.match(rpattern);
        var displayParams={}
        for (var i = 0; i < n.length; i++){
            var parts=n[i].split("=");
            var key = parts[0].trim();
            var value = parts[1].trim()
            if (!key.startsWith("nostore_") && key != "showchrome" && key != "prefix" && key != "cell_id"){
                displayParams[key] = value.substring(1,value.length-1);
            }
        }
        if(curCell&&curCell.output_area){
            curCell._metadata.pixiedust = curCell._metadata.pixiedust || {}
            curCell._metadata.pixiedust.displayParams=displayParams
            curCell.output_area.outputs=[];
        }
        $('#wrapperJS16cc7553').html("")
        $('#wrapperHTML16cc7553').html('<div style="width:100px;height:60px;left:47%;position:relative"><i class="fa fa-circle-o-notch fa-spin" style="font-size:48px"></i></div>'+
        '<div style="text-align:center">Loading your data. Please wait...</div>');
        startWallToWall = new Date().getTime();
        IPython.notebook.session.kernel.execute(command, callbacks, {silent:false,store_history:false,stop_on_error:true});
    }
}
)
            </script>
        
                </li>
              
                <li id="menu16cc7553-histogram">
                  
                  <i class="fa fa-table"></i>
                  
                  <span>Histogram</span>
                  
            <script>
                $('#menu16cc7553-histogram').on('click', 

function() {
    cellId = typeof cellId === "undefined" ? "" : cellId;
    var curCell=IPython.notebook.get_cells().filter(function(cell){
        return cell.cell_id=="9C2BFB65DD1F456D889381A547DA85CD".replace("cellId",cellId);
    });
    curCell=curCell.length>0?curCell[0]:null;
    console.log("curCell",curCell);
    var startWallToWall;
    //Resend the display command
    var callbacks = {
        iopub:{
            output:function(msg){
                console.log("msg", msg);
                if (false){
                    return curCell.output_area.handle_output.apply(curCell.output_area, arguments);
                }
                var msg_type=msg.header.msg_type;
                var content = msg.content;
                var executionTime = $("#execution16cc7553");
                if(msg_type==="stream"){
                    $('#wrapperHTML16cc7553').html(content.text);
                }else if (msg_type==="display_data" || msg_type==="execute_result"){
                    var html=null;
                    if (!!content.data["text/html"]){
                        html=content.data["text/html"];
                    }else if (!!content.data["image/png"]){
                        html=html||"";
                        html+="<img src='data:image/png;base64," +content.data["image/png"]+"'></img>";
                    }
                                                    
                    if (!!content.data["application/javascript"]){
                        try{
                            $('#wrapperJS16cc7553').html("<script type=\\\"text/javascript\\\">"+content.data["application/javascript"]+"</s" + "cript>");
                        }catch(e){
                            console.log("Invalid javascript output",e, content.data);
                        }
                    }
                    
                    if (html){
                        if(curCell&&curCell.output_area&&curCell.output_area.outputs){
                            var data = JSON.parse(JSON.stringify(content.data));
                            if(!!data["text/html"])data["text/html"]=html;
                            curCell.output_area.outputs.push({"data": data,"metadata":content.metadata,"output_type":msg_type});
                        }
                        try{
                            $('#wrapperHTML16cc7553').html(html);
                        }catch(e){
                            console.log("Invalid html output", e, html);
                            $('#wrapperHTML16cc7553').html( "Invalid html output. <pre>" 
                                + html.replace(/>/g,'&gt;').replace(/</g,'&lt;').replace(/"/g,'&quot;') + "<pre>");
                        }
                    }
                }else if (msg_type === "error") {
                    require(['base/js/utils'], function(utils) {
                        var tb = content.traceback;
                        console.log("tb",tb);
                        if (tb && tb.length>0){
                            var data = tb.reduce(function(res, frame){return res+frame+'\\n';},"");
                            console.log("data",data);
                            data = utils.fixConsole(data);
                            data = utils.fixCarriageReturn(data);
                            data = utils.autoLinkUrls(data);
                            $('#wrapperHTML16cc7553').html("<pre>" + data +"</pre>");
                        }
                    });
                }

                //Append profiling info
                if (executionTime.length > 0 && $("#execution16cc7553").length == 0 ){
                    $('#wrapperHTML16cc7553').append(executionTime);
                }else if (startWallToWall && $("#execution16cc7553").length > 0 ){
                    $("#execution16cc7553").append($("<div/>").text("Wall to Wall time: " + ( (new Date().getTime() - startWallToWall)/1000 ) + "s"));
                }
            }
        }
    }
    
    if (IPython && IPython.notebook && IPython.notebook.session && IPython.notebook.session.kernel){
        var command = "display(tweets_state_us,cell_id='9C2BFB65DD1F456D889381A547DA85CD',handlerId='histogram',mapDisplayMode='markers',valueFields='count',keyFields='state',aggregation='MAX',mapRegion='US',prefix='16cc7553')".replace("cellId",cellId);
        function addOptions(options){
            function getStringRep(v) {
                if (!isNaN(parseFloat(v)) && isFinite(v)){
                    return v.toString();
                }
                return "'" + v + "'";
            }
            for (var key in options){
                var value = options[key];
                var hasValue = value != null && typeof value !== 'undefined' && value !== '';
                var replaceValue = hasValue ? (key+"=" + getStringRep(value) ) : "";
                var pattern = (hasValue?"":",")+"\\s*" + key + "\\s*=\\s*'(\\\\'|[^'])*'";
                var rpattern=new RegExp(pattern);
                var n = command.search(rpattern);
                if ( n >= 0 ){
                    command = command.replace(rpattern, replaceValue);
                }else if (hasValue){
                    var n = command.lastIndexOf(")");
                    command = [command.slice(0, n), (command[n-1]=="("? "":",") + replaceValue, command.slice(n)].join('')
                }        
            }
        }
        if(typeof cellMetadata != "undefined" && cellMetadata.displayParams){
            addOptions(cellMetadata.displayParams);
            addOptions({"showchrome":"true"});
        }else if ('False'=='True' && curCell && curCell._metadata.pixiedust ){
            addOptions(curCell._metadata.pixiedust.displayParams || {} );
        }
        addOptions({});
        
        
    

        console.log("Running command2",command);
        var pattern = "\\w*\\s*=\\s*'(\\\\'|[^'])*'";
        var rpattern=new RegExp(pattern,"g");
        var n = command.match(rpattern);
        var displayParams={}
        for (var i = 0; i < n.length; i++){
            var parts=n[i].split("=");
            var key = parts[0].trim();
            var value = parts[1].trim()
            if (!key.startsWith("nostore_") && key != "showchrome" && key != "prefix" && key != "cell_id"){
                displayParams[key] = value.substring(1,value.length-1);
            }
        }
        if(curCell&&curCell.output_area){
            curCell._metadata.pixiedust = curCell._metadata.pixiedust || {}
            curCell._metadata.pixiedust.displayParams=displayParams
            curCell.output_area.outputs=[];
        }
        $('#wrapperJS16cc7553').html("")
        $('#wrapperHTML16cc7553').html('<div style="width:100px;height:60px;left:47%;position:relative"><i class="fa fa-circle-o-notch fa-spin" style="font-size:48px"></i></div>'+
        '<div style="text-align:center">Loading your data. Please wait...</div>');
        startWallToWall = new Date().getTime();
        IPython.notebook.session.kernel.execute(command, callbacks, {silent:false,store_history:false,stop_on_error:true});
    }
}
)
            </script>
        
                </li>
              
              </ul>
            </div>
          </div>
        
      
        
      
        
      
        
          <div class="btn-group btn-small display-type-button">
            
            <a class="btn btn-small dropdown-toggle btn-default" data-toggle="dropdown" title="Stash dataset">
              
              <i class="fa fa-cloud-download"></i>
              
              <i class="fa fa-chevron-down"></i>
            </a>
            <div class="dropdown-menu" role="menu" style="white-space:nowrap">
              <ul>
              
                <li id="menu16cc7553-downloadFile">
                  
                  <i class="fa fa-download"></i>
                  
                  <span>Download as File</span>
                  
            <script>
                $('#menu16cc7553-downloadFile').on('click', 

function() {
    cellId = typeof cellId === "undefined" ? "" : cellId;
    var curCell=IPython.notebook.get_cells().filter(function(cell){
        return cell.cell_id=="9C2BFB65DD1F456D889381A547DA85CD".replace("cellId",cellId);
    });
    curCell=curCell.length>0?curCell[0]:null;
    console.log("curCell",curCell);
    var startWallToWall;
    //Resend the display command
    var callbacks = {
        iopub:{
            output:function(msg){
                console.log("msg", msg);
                if (false){
                    return curCell.output_area.handle_output.apply(curCell.output_area, arguments);
                }
                var msg_type=msg.header.msg_type;
                var content = msg.content;
                var executionTime = $("#execution16cc7553");
                if(msg_type==="stream"){
                    $('#wrapperHTML16cc7553').html(content.text);
                }else if (msg_type==="display_data" || msg_type==="execute_result"){
                    var html=null;
                    if (!!content.data["text/html"]){
                        html=content.data["text/html"];
                    }else if (!!content.data["image/png"]){
                        html=html||"";
                        html+="<img src='data:image/png;base64," +content.data["image/png"]+"'></img>";
                    }
                                                    
                    if (!!content.data["application/javascript"]){
                        try{
                            $('#wrapperJS16cc7553').html("<script type=\\\"text/javascript\\\">"+content.data["application/javascript"]+"</s" + "cript>");
                        }catch(e){
                            console.log("Invalid javascript output",e, content.data);
                        }
                    }
                    
                    if (html){
                        if(curCell&&curCell.output_area&&curCell.output_area.outputs){
                            var data = JSON.parse(JSON.stringify(content.data));
                            if(!!data["text/html"])data["text/html"]=html;
                            curCell.output_area.outputs.push({"data": data,"metadata":content.metadata,"output_type":msg_type});
                        }
                        try{
                            $('#wrapperHTML16cc7553').html(html);
                        }catch(e){
                            console.log("Invalid html output", e, html);
                            $('#wrapperHTML16cc7553').html( "Invalid html output. <pre>" 
                                + html.replace(/>/g,'&gt;').replace(/</g,'&lt;').replace(/"/g,'&quot;') + "<pre>");
                        }
                    }
                }else if (msg_type === "error") {
                    require(['base/js/utils'], function(utils) {
                        var tb = content.traceback;
                        console.log("tb",tb);
                        if (tb && tb.length>0){
                            var data = tb.reduce(function(res, frame){return res+frame+'\\n';},"");
                            console.log("data",data);
                            data = utils.fixConsole(data);
                            data = utils.fixCarriageReturn(data);
                            data = utils.autoLinkUrls(data);
                            $('#wrapperHTML16cc7553').html("<pre>" + data +"</pre>");
                        }
                    });
                }

                //Append profiling info
                if (executionTime.length > 0 && $("#execution16cc7553").length == 0 ){
                    $('#wrapperHTML16cc7553').append(executionTime);
                }else if (startWallToWall && $("#execution16cc7553").length > 0 ){
                    $("#execution16cc7553").append($("<div/>").text("Wall to Wall time: " + ( (new Date().getTime() - startWallToWall)/1000 ) + "s"));
                }
            }
        }
    }
    
    if (IPython && IPython.notebook && IPython.notebook.session && IPython.notebook.session.kernel){
        var command = "display(tweets_state_us,cell_id='9C2BFB65DD1F456D889381A547DA85CD',handlerId='downloadFile',mapDisplayMode='markers',valueFields='count',keyFields='state',aggregation='MAX',mapRegion='US',prefix='16cc7553')".replace("cellId",cellId);
        function addOptions(options){
            function getStringRep(v) {
                if (!isNaN(parseFloat(v)) && isFinite(v)){
                    return v.toString();
                }
                return "'" + v + "'";
            }
            for (var key in options){
                var value = options[key];
                var hasValue = value != null && typeof value !== 'undefined' && value !== '';
                var replaceValue = hasValue ? (key+"=" + getStringRep(value) ) : "";
                var pattern = (hasValue?"":",")+"\\s*" + key + "\\s*=\\s*'(\\\\'|[^'])*'";
                var rpattern=new RegExp(pattern);
                var n = command.search(rpattern);
                if ( n >= 0 ){
                    command = command.replace(rpattern, replaceValue);
                }else if (hasValue){
                    var n = command.lastIndexOf(")");
                    command = [command.slice(0, n), (command[n-1]=="("? "":",") + replaceValue, command.slice(n)].join('')
                }        
            }
        }
        if(typeof cellMetadata != "undefined" && cellMetadata.displayParams){
            addOptions(cellMetadata.displayParams);
            addOptions({"showchrome":"true"});
        }else if ('False'=='True' && curCell && curCell._metadata.pixiedust ){
            addOptions(curCell._metadata.pixiedust.displayParams || {} );
        }
        addOptions({});
        
        
    

        console.log("Running command2",command);
        var pattern = "\\w*\\s*=\\s*'(\\\\'|[^'])*'";
        var rpattern=new RegExp(pattern,"g");
        var n = command.match(rpattern);
        var displayParams={}
        for (var i = 0; i < n.length; i++){
            var parts=n[i].split("=");
            var key = parts[0].trim();
            var value = parts[1].trim()
            if (!key.startsWith("nostore_") && key != "showchrome" && key != "prefix" && key != "cell_id"){
                displayParams[key] = value.substring(1,value.length-1);
            }
        }
        if(curCell&&curCell.output_area){
            curCell._metadata.pixiedust = curCell._metadata.pixiedust || {}
            curCell._metadata.pixiedust.displayParams=displayParams
            curCell.output_area.outputs=[];
        }
        $('#wrapperJS16cc7553').html("")
        $('#wrapperHTML16cc7553').html('<div style="width:100px;height:60px;left:47%;position:relative"><i class="fa fa-circle-o-notch fa-spin" style="font-size:48px"></i></div>'+
        '<div style="text-align:center">Loading your data. Please wait...</div>');
        startWallToWall = new Date().getTime();
        IPython.notebook.session.kernel.execute(command, callbacks, {silent:false,store_history:false,stop_on_error:true});
    }
}
)
            </script>
        
                </li>
              
                <li id="menu16cc7553-stashCloudant">
                  
                  <i class="fa fa-cloud"></i>
                  
                  <span>Stash to Cloudant</span>
                  
            <script>
                $('#menu16cc7553-stashCloudant').on('click', 

function() {
    cellId = typeof cellId === "undefined" ? "" : cellId;
    var curCell=IPython.notebook.get_cells().filter(function(cell){
        return cell.cell_id=="9C2BFB65DD1F456D889381A547DA85CD".replace("cellId",cellId);
    });
    curCell=curCell.length>0?curCell[0]:null;
    console.log("curCell",curCell);
    var startWallToWall;
    //Resend the display command
    var callbacks = {
        iopub:{
            output:function(msg){
                console.log("msg", msg);
                if (false){
                    return curCell.output_area.handle_output.apply(curCell.output_area, arguments);
                }
                var msg_type=msg.header.msg_type;
                var content = msg.content;
                var executionTime = $("#execution16cc7553");
                if(msg_type==="stream"){
                    $('#wrapperHTML16cc7553').html(content.text);
                }else if (msg_type==="display_data" || msg_type==="execute_result"){
                    var html=null;
                    if (!!content.data["text/html"]){
                        html=content.data["text/html"];
                    }else if (!!content.data["image/png"]){
                        html=html||"";
                        html+="<img src='data:image/png;base64," +content.data["image/png"]+"'></img>";
                    }
                                                    
                    if (!!content.data["application/javascript"]){
                        try{
                            $('#wrapperJS16cc7553').html("<script type=\\\"text/javascript\\\">"+content.data["application/javascript"]+"</s" + "cript>");
                        }catch(e){
                            console.log("Invalid javascript output",e, content.data);
                        }
                    }
                    
                    if (html){
                        if(curCell&&curCell.output_area&&curCell.output_area.outputs){
                            var data = JSON.parse(JSON.stringify(content.data));
                            if(!!data["text/html"])data["text/html"]=html;
                            curCell.output_area.outputs.push({"data": data,"metadata":content.metadata,"output_type":msg_type});
                        }
                        try{
                            $('#wrapperHTML16cc7553').html(html);
                        }catch(e){
                            console.log("Invalid html output", e, html);
                            $('#wrapperHTML16cc7553').html( "Invalid html output. <pre>" 
                                + html.replace(/>/g,'&gt;').replace(/</g,'&lt;').replace(/"/g,'&quot;') + "<pre>");
                        }
                    }
                }else if (msg_type === "error") {
                    require(['base/js/utils'], function(utils) {
                        var tb = content.traceback;
                        console.log("tb",tb);
                        if (tb && tb.length>0){
                            var data = tb.reduce(function(res, frame){return res+frame+'\\n';},"");
                            console.log("data",data);
                            data = utils.fixConsole(data);
                            data = utils.fixCarriageReturn(data);
                            data = utils.autoLinkUrls(data);
                            $('#wrapperHTML16cc7553').html("<pre>" + data +"</pre>");
                        }
                    });
                }

                //Append profiling info
                if (executionTime.length > 0 && $("#execution16cc7553").length == 0 ){
                    $('#wrapperHTML16cc7553').append(executionTime);
                }else if (startWallToWall && $("#execution16cc7553").length > 0 ){
                    $("#execution16cc7553").append($("<div/>").text("Wall to Wall time: " + ( (new Date().getTime() - startWallToWall)/1000 ) + "s"));
                }
            }
        }
    }
    
    if (IPython && IPython.notebook && IPython.notebook.session && IPython.notebook.session.kernel){
        var command = "display(tweets_state_us,cell_id='9C2BFB65DD1F456D889381A547DA85CD',handlerId='stashCloudant',mapDisplayMode='markers',valueFields='count',keyFields='state',aggregation='MAX',mapRegion='US',prefix='16cc7553')".replace("cellId",cellId);
        function addOptions(options){
            function getStringRep(v) {
                if (!isNaN(parseFloat(v)) && isFinite(v)){
                    return v.toString();
                }
                return "'" + v + "'";
            }
            for (var key in options){
                var value = options[key];
                var hasValue = value != null && typeof value !== 'undefined' && value !== '';
                var replaceValue = hasValue ? (key+"=" + getStringRep(value) ) : "";
                var pattern = (hasValue?"":",")+"\\s*" + key + "\\s*=\\s*'(\\\\'|[^'])*'";
                var rpattern=new RegExp(pattern);
                var n = command.search(rpattern);
                if ( n >= 0 ){
                    command = command.replace(rpattern, replaceValue);
                }else if (hasValue){
                    var n = command.lastIndexOf(")");
                    command = [command.slice(0, n), (command[n-1]=="("? "":",") + replaceValue, command.slice(n)].join('')
                }        
            }
        }
        if(typeof cellMetadata != "undefined" && cellMetadata.displayParams){
            addOptions(cellMetadata.displayParams);
            addOptions({"showchrome":"true"});
        }else if ('False'=='True' && curCell && curCell._metadata.pixiedust ){
            addOptions(curCell._metadata.pixiedust.displayParams || {} );
        }
        addOptions({});
        
        
    

        console.log("Running command2",command);
        var pattern = "\\w*\\s*=\\s*'(\\\\'|[^'])*'";
        var rpattern=new RegExp(pattern,"g");
        var n = command.match(rpattern);
        var displayParams={}
        for (var i = 0; i < n.length; i++){
            var parts=n[i].split("=");
            var key = parts[0].trim();
            var value = parts[1].trim()
            if (!key.startsWith("nostore_") && key != "showchrome" && key != "prefix" && key != "cell_id"){
                displayParams[key] = value.substring(1,value.length-1);
            }
        }
        if(curCell&&curCell.output_area){
            curCell._metadata.pixiedust = curCell._metadata.pixiedust || {}
            curCell._metadata.pixiedust.displayParams=displayParams
            curCell.output_area.outputs=[];
        }
        $('#wrapperJS16cc7553').html("")
        $('#wrapperHTML16cc7553').html('<div style="width:100px;height:60px;left:47%;position:relative"><i class="fa fa-circle-o-notch fa-spin" style="font-size:48px"></i></div>'+
        '<div style="text-align:center">Loading your data. Please wait...</div>');
        startWallToWall = new Date().getTime();
        IPython.notebook.session.kernel.execute(command, callbacks, {silent:false,store_history:false,stop_on_error:true});
    }
}
)
            </script>
        
                </li>
              
                <li id="menu16cc7553-stashSwift">
                  
                  <i class="fa fa-suitcase"></i>
                  
                  <span>Stash to Object Storage</span>
                  
            <script>
                $('#menu16cc7553-stashSwift').on('click', 

function() {
    cellId = typeof cellId === "undefined" ? "" : cellId;
    var curCell=IPython.notebook.get_cells().filter(function(cell){
        return cell.cell_id=="9C2BFB65DD1F456D889381A547DA85CD".replace("cellId",cellId);
    });
    curCell=curCell.length>0?curCell[0]:null;
    console.log("curCell",curCell);
    var startWallToWall;
    //Resend the display command
    var callbacks = {
        iopub:{
            output:function(msg){
                console.log("msg", msg);
                if (false){
                    return curCell.output_area.handle_output.apply(curCell.output_area, arguments);
                }
                var msg_type=msg.header.msg_type;
                var content = msg.content;
                var executionTime = $("#execution16cc7553");
                if(msg_type==="stream"){
                    $('#wrapperHTML16cc7553').html(content.text);
                }else if (msg_type==="display_data" || msg_type==="execute_result"){
                    var html=null;
                    if (!!content.data["text/html"]){
                        html=content.data["text/html"];
                    }else if (!!content.data["image/png"]){
                        html=html||"";
                        html+="<img src='data:image/png;base64," +content.data["image/png"]+"'></img>";
                    }
                                                    
                    if (!!content.data["application/javascript"]){
                        try{
                            $('#wrapperJS16cc7553').html("<script type=\\\"text/javascript\\\">"+content.data["application/javascript"]+"</s" + "cript>");
                        }catch(e){
                            console.log("Invalid javascript output",e, content.data);
                        }
                    }
                    
                    if (html){
                        if(curCell&&curCell.output_area&&curCell.output_area.outputs){
                            var data = JSON.parse(JSON.stringify(content.data));
                            if(!!data["text/html"])data["text/html"]=html;
                            curCell.output_area.outputs.push({"data": data,"metadata":content.metadata,"output_type":msg_type});
                        }
                        try{
                            $('#wrapperHTML16cc7553').html(html);
                        }catch(e){
                            console.log("Invalid html output", e, html);
                            $('#wrapperHTML16cc7553').html( "Invalid html output. <pre>" 
                                + html.replace(/>/g,'&gt;').replace(/</g,'&lt;').replace(/"/g,'&quot;') + "<pre>");
                        }
                    }
                }else if (msg_type === "error") {
                    require(['base/js/utils'], function(utils) {
                        var tb = content.traceback;
                        console.log("tb",tb);
                        if (tb && tb.length>0){
                            var data = tb.reduce(function(res, frame){return res+frame+'\\n';},"");
                            console.log("data",data);
                            data = utils.fixConsole(data);
                            data = utils.fixCarriageReturn(data);
                            data = utils.autoLinkUrls(data);
                            $('#wrapperHTML16cc7553').html("<pre>" + data +"</pre>");
                        }
                    });
                }

                //Append profiling info
                if (executionTime.length > 0 && $("#execution16cc7553").length == 0 ){
                    $('#wrapperHTML16cc7553').append(executionTime);
                }else if (startWallToWall && $("#execution16cc7553").length > 0 ){
                    $("#execution16cc7553").append($("<div/>").text("Wall to Wall time: " + ( (new Date().getTime() - startWallToWall)/1000 ) + "s"));
                }
            }
        }
    }
    
    if (IPython && IPython.notebook && IPython.notebook.session && IPython.notebook.session.kernel){
        var command = "display(tweets_state_us,cell_id='9C2BFB65DD1F456D889381A547DA85CD',handlerId='stashSwift',mapDisplayMode='markers',valueFields='count',keyFields='state',aggregation='MAX',mapRegion='US',prefix='16cc7553')".replace("cellId",cellId);
        function addOptions(options){
            function getStringRep(v) {
                if (!isNaN(parseFloat(v)) && isFinite(v)){
                    return v.toString();
                }
                return "'" + v + "'";
            }
            for (var key in options){
                var value = options[key];
                var hasValue = value != null && typeof value !== 'undefined' && value !== '';
                var replaceValue = hasValue ? (key+"=" + getStringRep(value) ) : "";
                var pattern = (hasValue?"":",")+"\\s*" + key + "\\s*=\\s*'(\\\\'|[^'])*'";
                var rpattern=new RegExp(pattern);
                var n = command.search(rpattern);
                if ( n >= 0 ){
                    command = command.replace(rpattern, replaceValue);
                }else if (hasValue){
                    var n = command.lastIndexOf(")");
                    command = [command.slice(0, n), (command[n-1]=="("? "":",") + replaceValue, command.slice(n)].join('')
                }        
            }
        }
        if(typeof cellMetadata != "undefined" && cellMetadata.displayParams){
            addOptions(cellMetadata.displayParams);
            addOptions({"showchrome":"true"});
        }else if ('False'=='True' && curCell && curCell._metadata.pixiedust ){
            addOptions(curCell._metadata.pixiedust.displayParams || {} );
        }
        addOptions({});
        
        
    

        console.log("Running command2",command);
        var pattern = "\\w*\\s*=\\s*'(\\\\'|[^'])*'";
        var rpattern=new RegExp(pattern,"g");
        var n = command.match(rpattern);
        var displayParams={}
        for (var i = 0; i < n.length; i++){
            var parts=n[i].split("=");
            var key = parts[0].trim();
            var value = parts[1].trim()
            if (!key.startsWith("nostore_") && key != "showchrome" && key != "prefix" && key != "cell_id"){
                displayParams[key] = value.substring(1,value.length-1);
            }
        }
        if(curCell&&curCell.output_area){
            curCell._metadata.pixiedust = curCell._metadata.pixiedust || {}
            curCell._metadata.pixiedust.displayParams=displayParams
            curCell.output_area.outputs=[];
        }
        $('#wrapperJS16cc7553').html("")
        $('#wrapperHTML16cc7553').html('<div style="width:100px;height:60px;left:47%;position:relative"><i class="fa fa-circle-o-notch fa-spin" style="font-size:48px"></i></div>'+
        '<div style="text-align:center">Loading your data. Please wait...</div>');
        startWallToWall = new Date().getTime();
        IPython.notebook.session.kernel.execute(command, callbacks, {silent:false,store_history:false,stop_on_error:true});
    }
}
)
            </script>
        
                </li>
              
              </ul>
            </div>
          </div>
        
      
      </div>
    
    <div id="wrapperJS16cc7553"></div>
    <div id="wrapperHTML16cc7553" style="min-height:100px"></div>
  </div>
</div>
                    <script>
                    (

function() {
    cellId = typeof cellId === "undefined" ? "" : cellId;
    var curCell=IPython.notebook.get_cells().filter(function(cell){
        return cell.cell_id=="9C2BFB65DD1F456D889381A547DA85CD".replace("cellId",cellId);
    });
    curCell=curCell.length>0?curCell[0]:null;
    console.log("curCell",curCell);
    var startWallToWall;
    //Resend the display command
    var callbacks = {
        iopub:{
            output:function(msg){
                console.log("msg", msg);
                if (false){
                    return curCell.output_area.handle_output.apply(curCell.output_area, arguments);
                }
                var msg_type=msg.header.msg_type;
                var content = msg.content;
                var executionTime = $("#execution16cc7553");
                if(msg_type==="stream"){
                    $('#wrapperHTML16cc7553').html(content.text);
                }else if (msg_type==="display_data" || msg_type==="execute_result"){
                    var html=null;
                    if (!!content.data["text/html"]){
                        html=content.data["text/html"];
                    }else if (!!content.data["image/png"]){
                        html=html||"";
                        html+="<img src='data:image/png;base64," +content.data["image/png"]+"'></img>";
                    }
                                                    
                    if (!!content.data["application/javascript"]){
                        try{
                            $('#wrapperJS16cc7553').html("<script type=\\\"text/javascript\\\">"+content.data["application/javascript"]+"</s" + "cript>");
                        }catch(e){
                            console.log("Invalid javascript output",e, content.data);
                        }
                    }
                    
                    if (html){
                        if(curCell&&curCell.output_area&&curCell.output_area.outputs){
                            var data = JSON.parse(JSON.stringify(content.data));
                            if(!!data["text/html"])data["text/html"]=html;
                            curCell.output_area.outputs.push({"data": data,"metadata":content.metadata,"output_type":msg_type});
                        }
                        try{
                            $('#wrapperHTML16cc7553').html(html);
                        }catch(e){
                            console.log("Invalid html output", e, html);
                            $('#wrapperHTML16cc7553').html( "Invalid html output. <pre>" 
                                + html.replace(/>/g,'&gt;').replace(/</g,'&lt;').replace(/"/g,'&quot;') + "<pre>");
                        }
                    }
                }else if (msg_type === "error") {
                    require(['base/js/utils'], function(utils) {
                        var tb = content.traceback;
                        console.log("tb",tb);
                        if (tb && tb.length>0){
                            var data = tb.reduce(function(res, frame){return res+frame+'\\n';},"");
                            console.log("data",data);
                            data = utils.fixConsole(data);
                            data = utils.fixCarriageReturn(data);
                            data = utils.autoLinkUrls(data);
                            $('#wrapperHTML16cc7553').html("<pre>" + data +"</pre>");
                        }
                    });
                }

                //Append profiling info
                if (executionTime.length > 0 && $("#execution16cc7553").length == 0 ){
                    $('#wrapperHTML16cc7553').append(executionTime);
                }else if (startWallToWall && $("#execution16cc7553").length > 0 ){
                    $("#execution16cc7553").append($("<div/>").text("Wall to Wall time: " + ( (new Date().getTime() - startWallToWall)/1000 ) + "s"));
                }
            }
        }
    }
    
    if (IPython && IPython.notebook && IPython.notebook.session && IPython.notebook.session.kernel){
        var command = "display(tweets_state_us,cell_id='9C2BFB65DD1F456D889381A547DA85CD',handlerId='mapChart',mapDisplayMode='markers',valueFields='count',keyFields='state',aggregation='MAX',mapRegion='US',prefix='16cc7553')".replace("cellId",cellId);
        function addOptions(options){
            function getStringRep(v) {
                if (!isNaN(parseFloat(v)) && isFinite(v)){
                    return v.toString();
                }
                return "'" + v + "'";
            }
            for (var key in options){
                var value = options[key];
                var hasValue = value != null && typeof value !== 'undefined' && value !== '';
                var replaceValue = hasValue ? (key+"=" + getStringRep(value) ) : "";
                var pattern = (hasValue?"":",")+"\\s*" + key + "\\s*=\\s*'(\\\\'|[^'])*'";
                var rpattern=new RegExp(pattern);
                var n = command.search(rpattern);
                if ( n >= 0 ){
                    command = command.replace(rpattern, replaceValue);
                }else if (hasValue){
                    var n = command.lastIndexOf(")");
                    command = [command.slice(0, n), (command[n-1]=="("? "":",") + replaceValue, command.slice(n)].join('')
                }        
            }
        }
        if(typeof cellMetadata != "undefined" && cellMetadata.displayParams){
            addOptions(cellMetadata.displayParams);
            addOptions({"showchrome":"true"});
        }else if ('False'=='True' && curCell && curCell._metadata.pixiedust ){
            addOptions(curCell._metadata.pixiedust.displayParams || {} );
        }
        addOptions({});
        
        
    

        console.log("Running command2",command);
        var pattern = "\\w*\\s*=\\s*'(\\\\'|[^'])*'";
        var rpattern=new RegExp(pattern,"g");
        var n = command.match(rpattern);
        var displayParams={}
        for (var i = 0; i < n.length; i++){
            var parts=n[i].split("=");
            var key = parts[0].trim();
            var value = parts[1].trim()
            if (!key.startsWith("nostore_") && key != "showchrome" && key != "prefix" && key != "cell_id"){
                displayParams[key] = value.substring(1,value.length-1);
            }
        }
        if(curCell&&curCell.output_area){
            curCell._metadata.pixiedust = curCell._metadata.pixiedust || {}
            curCell._metadata.pixiedust.displayParams=displayParams
            curCell.output_area.outputs=[];
        }
        $('#wrapperJS16cc7553').html("")
        $('#wrapperHTML16cc7553').html('<div style="width:100px;height:60px;left:47%;position:relative"><i class="fa fa-circle-o-notch fa-spin" style="font-size:48px"></i></div>'+
        '<div style="text-align:center">Loading your data. Please wait...</div>');
        startWallToWall = new Date().getTime();
        IPython.notebook.session.kernel.execute(command, callbacks, {silent:false,store_history:false,stop_on_error:true});
    }
}
)();
                    </script>
                </div>



<script>
$("#mapOptions16cc7553").click(function(){
    
    var globalNested={}
require(['base/js/dialog'],function(dialog){
        var modal = dialog.modal;
        var options = {
            title: "Pixiedust: Chart Options",
            body: '<form id="chartOptions16cc7553"><style type="text/css">	.field-container {	}	.field-container ul {		overflow-y: auto;		border: 1px solid #ccc;		padding: 5px 0 0 0;		list-style-type: none;	}	.field-container label {		color: #008571;		font-weight: bold;	}	.field-container li {		margin: 0px 5px 5px 5px;		padding: 5px;		border: 1px solid #ccc;		border-radius: 5px;		background-color: #daffff;		width: auto;		min-width: 150px;	}	.field-remove {		color: #777;		font-size: 14px;		float: right;		margin: 0px;		cursor: pointer;	}	.field-remove:after {		content: "x";	}</style><div class="container col-sm-12">	<div class="row">		<div class="form-group col-sm-10" style="padding-right:10px;">			<label for="title16cc7553">Chart Title:</label>			<input type="text" class="form-control" id="title16cc7553" name="title" value="">		</div>			</div>	<div class="field-container row">		<div class="col-sm-4" style="padding: 0px 5px 0px 0px;">			<label>Fields:</label>			<ul id="allFields16cc7553" style="height: 215px;">								<li id="field16cc7553-f1" class="field16cc7553">state<a class="field16cc7553-remove"></a></li>								<li id="field16cc7553-f2" class="field16cc7553">count<a class="field16cc7553-remove"></a></li>							</ul>		</div>		<div class="col-sm-8" style="padding: 0px 0px 0px 5px;">						<label>Keys:</label>			<ul id="keyFields16cc7553" style="height: 90px;">																			<li id="field16cc7553-f1-keyFields16cc7553">state<a class="field16cc7553-remove field-remove"></a></li>																</ul>						<label>Values:</label>						<ul id="valueFields16cc7553" style="height: 90px;">																						<li id="field16cc7553-f2-valueFields16cc7553">count<a class="field16cc7553-remove field-remove"></a></li>																</ul>		</div>	</div>	<div class="row">						<div class="col-sm-4" style="padding: 0px 0px 0px 5px;">			<div class="form-group">				<label class="field">Aggregation:</label>				<select class="form-control" name="aggregation" style="margin-left: 0px;">										<option value="SUM">SUM</option>										<option value="AVG">AVG</option>										<option value="MIN">MIN</option>										<option value="MAX" selected>MAX</option>										<option value="COUNT">COUNT</option>									</select>			</div>		</div>						<div class="col-sm-4" style="padding: 0px 5px 0px 5px;">			<div class="form-group">				<label class="field"># of Rows to Display:</label>				<input class="form-control" name="rowCount" type="number" min="1" max="1000" value="100">			</div>		</div>				<div class="col-sm-4" style="padding: 0px 0px 0px 5px;">	<div class="form-group">		<label class="field">Display Mode</label>		<select class="form-control" name="mapDisplayMode" style="margin-left: 0px;">			<option value="region">Region</option>			<option value="markers" selected>Markers</option>			<option value="text">Text</option>		</select>	</div></div><div class="col-sm-4" style="padding: 0px 0px 0px 5px;">	<div class="form-group">		<label class="field">Region</label>		<select class="form-control" name="mapRegion" style="margin-left: 0px;">			<option value="world">World</option>			<option value="US" selected>US</option>		</select>	</div></div>	</div></div></form><div class="bootbox-body row"></div>',
            sanitize:false,
            notebook: IPython.notebook,
            buttons: {
                
                OK: {
                    class : "btn-primary btn-ok",
                    click: function() {
                        
(

function() {
    cellId = typeof cellId === "undefined" ? "" : cellId;
    var curCell=IPython.notebook.get_cells().filter(function(cell){
        return cell.cell_id=="9C2BFB65DD1F456D889381A547DA85CD".replace("cellId",cellId);
    });
    curCell=curCell.length>0?curCell[0]:null;
    console.log("curCell",curCell);
    var startWallToWall;
    //Resend the display command
    var callbacks = {
        iopub:{
            output:function(msg){
                console.log("msg", msg);
                if (false){
                    return curCell.output_area.handle_output.apply(curCell.output_area, arguments);
                }
                var msg_type=msg.header.msg_type;
                var content = msg.content;
                var executionTime = $("#execution16cc7553");
                if(msg_type==="stream"){
                    $('#wrapperHTML16cc7553').html(content.text);
                }else if (msg_type==="display_data" || msg_type==="execute_result"){
                    var html=null;
                    if (!!content.data["text/html"]){
                        html=content.data["text/html"];
                    }else if (!!content.data["image/png"]){
                        html=html||"";
                        html+="<img src='data:image/png;base64," +content.data["image/png"]+"'></img>";
                    }
                                                    
                    if (!!content.data["application/javascript"]){
                        try{
                            $('#wrapperJS16cc7553').html("<script type=\\\"text/javascript\\\">"+content.data["application/javascript"]+"</s" + "cript>");
                        }catch(e){
                            console.log("Invalid javascript output",e, content.data);
                        }
                    }
                    
                    if (html){
                        if(curCell&&curCell.output_area&&curCell.output_area.outputs){
                            var data = JSON.parse(JSON.stringify(content.data));
                            if(!!data["text/html"])data["text/html"]=html;
                            curCell.output_area.outputs.push({"data": data,"metadata":content.metadata,"output_type":msg_type});
                        }
                        try{
                            $('#wrapperHTML16cc7553').html(html);
                        }catch(e){
                            console.log("Invalid html output", e, html);
                            $('#wrapperHTML16cc7553').html( "Invalid html output. <pre>" 
                                + html.replace(/>/g,'&gt;').replace(/</g,'&lt;').replace(/"/g,'&quot;') + "<pre>");
                        }
                    }
                }else if (msg_type === "error") {
                    require(['base/js/utils'], function(utils) {
                        var tb = content.traceback;
                        console.log("tb",tb);
                        if (tb && tb.length>0){
                            var data = tb.reduce(function(res, frame){return res+frame+'\\n';},"");
                            console.log("data",data);
                            data = utils.fixConsole(data);
                            data = utils.fixCarriageReturn(data);
                            data = utils.autoLinkUrls(data);
                            $('#wrapperHTML16cc7553').html("<pre>" + data +"</pre>");
                        }
                    });
                }

                //Append profiling info
                if (executionTime.length > 0 && $("#execution16cc7553").length == 0 ){
                    $('#wrapperHTML16cc7553').append(executionTime);
                }else if (startWallToWall && $("#execution16cc7553").length > 0 ){
                    $("#execution16cc7553").append($("<div/>").text("Wall to Wall time: " + ( (new Date().getTime() - startWallToWall)/1000 ) + "s"));
                }
            }
        }
    }
    
    if (IPython && IPython.notebook && IPython.notebook.session && IPython.notebook.session.kernel){
        var command = "display(tweets_state_us,cell_id='9C2BFB65DD1F456D889381A547DA85CD',handlerId='mapChart',mapDisplayMode='markers',valueFields='count',keyFields='state',aggregation='MAX',mapRegion='US',prefix='16cc7553')".replace("cellId",cellId);
        function addOptions(options){
            function getStringRep(v) {
                if (!isNaN(parseFloat(v)) && isFinite(v)){
                    return v.toString();
                }
                return "'" + v + "'";
            }
            for (var key in options){
                var value = options[key];
                var hasValue = value != null && typeof value !== 'undefined' && value !== '';
                var replaceValue = hasValue ? (key+"=" + getStringRep(value) ) : "";
                var pattern = (hasValue?"":",")+"\\s*" + key + "\\s*=\\s*'(\\\\'|[^'])*'";
                var rpattern=new RegExp(pattern);
                var n = command.search(rpattern);
                if ( n >= 0 ){
                    command = command.replace(rpattern, replaceValue);
                }else if (hasValue){
                    var n = command.lastIndexOf(")");
                    command = [command.slice(0, n), (command[n-1]=="("? "":",") + replaceValue, command.slice(n)].join('')
                }        
            }
        }
        if(typeof cellMetadata != "undefined" && cellMetadata.displayParams){
            addOptions(cellMetadata.displayParams);
            addOptions({"showchrome":"true"});
        }else if ('False'=='True' && curCell && curCell._metadata.pixiedust ){
            addOptions(curCell._metadata.pixiedust.displayParams || {} );
        }
        addOptions({});
        
        
    
var addValueToCommand = function(name, value) {
    if (value) {
        var startIndex, endIndex;
        startIndex = command.indexOf(","+name+"=");
        if (startIndex >= 0) {
            commaIndex = command.indexOf(",", startIndex+1);
            quoteIndex = command.indexOf("'", startIndex+1);
            if (quoteIndex >=0 && quoteIndex < commaIndex) {
                // value is enclosed in quotes - end of value will be second quote
                endIndex = command.indexOf("'", quoteIndex+1) + 1;
            }
            else if (commaIndex >= 0) {
                // end of value is the comma
                endIndex = commaIndex;
            }
            else {
                // no quote or comma found - end of value is at the very end
                endIndex = command.indexOf(")", startIndex+1);
            }
        }
        else {
            startIndex = endIndex = command.lastIndexOf(")");
        }
        var start = command.substring(0,startIndex);
        var end = command.substring(endIndex);
        command = start + "," + name +"='" + value + "'" + end;
    }
    else {
        var startIndex, endIndex;
        startIndex = command.indexOf(","+name+"=");
        if (startIndex >= 0) {
            commaIndex = command.indexOf(",", startIndex+1);
            quoteIndex = command.indexOf("'", startIndex+1);
            if (quoteIndex >=0 && quoteIndex < commaIndex) {
                // value is enclosed in quotes - end of value will be second quote
                endIndex = command.indexOf("'", quoteIndex+1) + 1;
            }
            else if (commaIndex >= 0) {
                // end of value is the comma
                endIndex = commaIndex;
            }
            else {
                // no quote or comma found - end of value is at the very end
                endIndex = command.indexOf(")", startIndex+1);
            }
            var start = command.substring(0,startIndex);
            var end = command.substring(endIndex);
            command = start + end;
        }
    }
};
var getListValues = function(listId) {
    var value = '';
    $(listId + ' li').each(function(idx, li) {
        if (value.length != 0) {
            value += ',';
        }
        value += $(li).text();
    });
    return value;
};
addValueToCommand('keyFields',getListValues('#keyFields16cc7553'));
addValueToCommand('valueFields',getListValues('#valueFields16cc7553'));
$('#chartOptions16cc7553 *').filter(':input').each(function(){
    if ($(this).is(':checkbox')) {
        addValueToCommand($(this).attr('name'),$(this).is(':checked')+'');	
    }
    else {
        addValueToCommand($(this).attr('name'),$(this).val());
    }
});
$('#chartOptions16cc7553 *').filter('select').each(function(){
    addValueToCommand($(this).attr('name'),$(this).val());
});


        console.log("Running command2",command);
        var pattern = "\\w*\\s*=\\s*'(\\\\'|[^'])*'";
        var rpattern=new RegExp(pattern,"g");
        var n = command.match(rpattern);
        var displayParams={}
        for (var i = 0; i < n.length; i++){
            var parts=n[i].split("=");
            var key = parts[0].trim();
            var value = parts[1].trim()
            if (!key.startsWith("nostore_") && key != "showchrome" && key != "prefix" && key != "cell_id"){
                displayParams[key] = value.substring(1,value.length-1);
            }
        }
        if(curCell&&curCell.output_area){
            curCell._metadata.pixiedust = curCell._metadata.pixiedust || {}
            curCell._metadata.pixiedust.displayParams=displayParams
            curCell.output_area.outputs=[];
        }
        $('#wrapperJS16cc7553').html("")
        $('#wrapperHTML16cc7553').html('<div style="width:100px;height:60px;left:47%;position:relative"><i class="fa fa-circle-o-notch fa-spin" style="font-size:48px"></i></div>'+
        '<div style="text-align:center">Loading your data. Please wait...</div>');
        startWallToWall = new Date().getTime();
        IPython.notebook.session.kernel.execute(command, callbacks, {silent:false,store_history:false,stop_on_error:true});
    }
}
)()

                    }
                },
                Cancel: {
                    class : "btn-cancel",
                    click: function(){
                        
                    }
                }
                
            }
        };
        var modal_obj = modal(options);
        modal_obj.addClass('pixiedust');
        modal_obj.on('shown.bs.modal', function(){
            
$('#chartOptions16cc7553 *').filter(':input').each(function(){
	IPython.keyboard_manager.register_events($(this));
});
$('.field16cc7553').draggable({
	helper:"clone",
	containment:"document"
});
$('.field16cc7553-remove').click(function(e) {
	e.preventDefault();
	$(this).parent().remove();
	return false;
});
$('#keyFields16cc7553, #valueFields16cc7553').droppable({
	drop:function(event, ui) {
		var newId = ui.draggable.attr('id') + '-' + $(this).attr('id');
		if ( $('#'+newId).length == 0) { 
			var el = ui.draggable.clone(true, true);
			el.attr("id",newId);
			el.find("a").addClass("field-remove");
			el.appendTo($(this));
		}
	}
});

        });

        })

});
</script>
<button id="mapOptions16cc7553">Options</button>
<div id="map16cc7553"/></div><div class="executionTime" id="execution16cc7553">Execution time: 0.12s</div>


Use a data set with at least two numeric columns to create scatter plots.

# 4. Analysis with Spark MLlib

Here we are going to use KMeans clustering algorithm from Spark MLlib.

Clustering will let us cluster similar tweets together.

We will then display clusters using Brunel library.


```python
# TRAINING by hashtag
from pyspark.mllib.feature import HashingTF
from pyspark.mllib.clustering import KMeans, KMeansModel

# dataframe of tweets' messages and hashtags
mhDF = sqlContext.sql("SELECT message.body as message, \
                message.object.twitter_entities.hashtags.text as tags \
                FROM tweets_DF \
                WHERE message.object.twitter_entities.hashtags.text IS NOT NULL")
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
```


```python
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
```


```python
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
```


```python
# visualizing clusters
import brunel

cmh_pd = cmhDF.toPandas()
cmh_pd.to_csv('cmh_pd.csv')
%brunel data('cmh_pd') bubble x(cluster) color(#all) size(#count) tooltip(message, tags) legends(none)
```


<!--
  ~ Copyright (c) 2015 IBM Corporation and others.
  ~
  ~ Licensed under the Apache License, Version 2.0 (the "License");
  ~ You may not use this file except in compliance with the License.
  ~ You may obtain a copy of the License at
  ~
  ~     http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing, software
  ~ distributed under the License is distributed on an "AS IS" BASIS,
  ~ WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  ~ See the License for the specific language governing permissions and
  ~ limitations under the License.
  -->


<link rel="stylesheet" type="text/css" href="/data/jupyter2/4974bb7a-e2a6-4424-80af-f7d42ca46ba4/nbextensions/brunel_ext/Brunel.css">
<link rel="stylesheet" type="text/css" href="/data/jupyter2/4974bb7a-e2a6-4424-80af-f7d42ca46ba4/nbextensions/brunel_ext/sumoselect/sumoselect.css">

<style>
    
</style>

<svg id="visid76ccc68e-9693-11e6-9984-002590fb6500" width="500" height="400"></svg>





    <IPython.core.display.Javascript object>


