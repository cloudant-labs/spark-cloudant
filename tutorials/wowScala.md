
# 0. Introduction

In this tutorial, we will use Spark Streaming to stream data from a Cloudant database, and process this continously received stream of data using Spark SQL. 

Our processing pipeline goes through three stages:
1. *_changes feed* is streamed from the given Cloudant database using `CloudantReceiver`. `CloudantReceiver` will receive _changes feed of the database, extract individual JSON documents from the feed, and store these documents in Spark's memory for processing by Spark Streaming. 
2. Spark Streaming will break up this continous stream of documents into batches. Each batch is a separate RDD, and in our case represents a set of documents collected within 10 secs window. This sequence of batches, or sequence of RDDs is what is called a discretized stream or DStream. 
3. Each RDD of the DStream is processed using Spark SQL.

```
|                1                  | -> |            2               |    |          3             |
|_changes feed ->... doc3 doc2 doc1 | -> |... [doc4 doc3] [doc2 doc1] | -> |... [pBatch2] [pBatch1] |
|      CloudantReceiver             | -> | Spark Streaming: DStream   | -> |      Spark SQL         |
```

In the steps below, we:
1. Initialize StreamingContext and DStream
2. Define processing of DStream using Spark SQL
3. Actually start processing, and stop it after some time.

# 1. Initializing DStream

## 1.1 Provide the details of your cloudant account in *properties* map:

- *cloudant.host* - the fully qualified account https URL
- *cloudant.username* - the Cloudant account username
- *cloudant.password* - the Cloudant account password
- *database* - database which changes you want to recieve 


```scala
import org.apache.spark.streaming.{ Seconds, StreamingContext, Time }
import org.apache.spark.{ SparkContext, SparkConf}
import org.apache.spark.sql.SQLContext
import org.apache.spark.rdd.RDD
import java.util.concurrent.atomic.AtomicLong

import com.cloudant.spark.CloudantReceiver


val properties = Map(
    "cloudant.host"-> "xxxx.cloudant.com", 
    "cloudant.username"-> "xxxx",
    "cloudant.password"-> "xxxx",
    "database"-> "yyyy"
)
```

## 1.2 Initialize StreamingContext and DStream

- Initialize a StreamingContext with a 10 seconds batch size
- Create a DStream of database changes using CloudantReceiver


```scala
val ssc = new StreamingContext(sc, Seconds(10))
val changesDStream = ssc.receiverStream(new CloudantReceiver(properties))
```

## 2. Define processing of DStreams

- Get SQLContext
- For every batch:
  - Create a dataframe `tweetsDF`
  - Create `tweetsDF2` dataframe with fields `gender`, `state`, and `polarity` 
  - Calculate and display the cumulative count of tweets
  


```scala
val sqlContext = new SQLContext(sc)
import sqlContext.implicits._

val curTotalCount = new AtomicLong(0)
changesDStream.foreachRDD((rdd: RDD[String], time: Time) => {
    println(s"========= $time =========")
    val rawTweetsDF = sqlContext.read.json(rdd)
    
    if (!rawTweetsDF.schema.isEmpty) {
        rawTweetsDF.show(10)
        
        val tweetsDF = rawTweetsDF.select($"cde.author.gender", 
                $"cde.author.location.state",
                $"cde.content.sentiment.polarity")
        tweetsDF.show(10)
        
        curTotalCount.getAndAdd(tweetsDF.count())
        println("Current total count:" + curTotalCount)
    }
})
```

# 3. Start receiving and processing of data

- Start StreamingContext
- Allow processing to run for 300 secs
- Manually stop processing 

All previous instructions were just initilizations and definions, and nothing will happen until we start StreamingContext. After the start, the data will be received and processed. Since, DStream is continous,  it will not stop until we manually stop the processing.

While this processing is running for 300 secs, we can go back to the Python notebook, and load more data to the database. These new changes will be picked up by Spark Streaming, proccessed and displayed below.
Thus, the steps for demonstrating dynamic nature of Spark Streaming are following:

1. Run the cell below of the current **Scala notebook**
2. After that go back to the **Python notebook**, and run the following two cells of the Python notebook:
Run:
```python
query = "#election2016"
count = 300
```
ant then run the following cell:
```python
TtC = TwitterToCloudant()
TtC.count = count
TtC.query_twitter(properties, None, query, 0)
```

Keep iterating through the load of tweets into the database and see how the stream below picks up the changes. Every 10 sec we fetch the next set of tweets available at that point. The analysis results will change appropriately and you can see for example different states showing up at the top of the list.


```scala
ssc.start()
Thread.sleep(300000L)
ssc.stop(true)
```

    ========= 1476947750000 ms =========
    ========= 1476947760000 ms =========
    +--------------------+--------------------+--------------------+--------------------+--------------------+
    |                 _id|                _rev|                 cde|         cdeInternal|             message|
    +--------------------+--------------------+--------------------+--------------------+--------------------+
    |d044c4d0b0551cc93...|1-9c2f0a4b09ea675...|[[null,[,United S...|[null,WrappedArra...|[[AZ After Party,...|
    |d044c4d0b0551cc93...|1-bb2f38a4ced7969...|[[unknown,[,Unite...|[null,WrappedArra...|[[utahpolitics,1,...|
    |d044c4d0b0551cc93...|1-7669e192db75074...|[[unknown,[NATION...|[null,WrappedArra...|[[Not On This Wat...|
    |d044c4d0b0551cc93...|1-643e24d6fbda555...|[[male,[BELLE,Uni...|[null,WrappedArra...|[[DR.BROWN-DEAN,3...|
    |d044c4d0b0551cc93...|1-59f8bbeec7c0283...|[[null,[null,null...|                null|[[Arturo Quintero...|
    |d044c4d0b0551cc93...|1-a7e8585adf332e3...|[[female,[null,nu...|                null|[[Gloria Marcelin...|
    |d044c4d0b0551cc93...|1-d8d702846ed578c...|[[male,[,,],[,unk...|[null,WrappedArra...|[[Mormon Democrat...|
    |d044c4d0b0551cc93...|1-682d3282aa29a80...|[[unknown,[Accra,...|[null,WrappedArra...|[[Afoobu,218,621,...|
    |d044c4d0b0551cc93...|1-5aadc76dc336ea6...|[[unknown,[SAN FR...|[[true,null,false...|[[jess,873,74,266...|
    |d044c4d0b0551cc93...|1-e01013f3b419d3c...|[[unknown,[null,n...|[null,WrappedArra...|[[995mu,1041,94,5...|
    +--------------------+--------------------+--------------------+--------------------+--------------------+
    only showing top 10 rows
    
    +-------+----------+--------+
    | gender|     state|polarity|
    +-------+----------+--------+
    |   null|   Arizona|    null|
    |unknown|      Utah| NEUTRAL|
    |unknown|  Maryland|NEGATIVE|
    |   male|  Missouri|NEGATIVE|
    |   null|      null|    null|
    | female|      null| NEUTRAL|
    |   male|          | NEUTRAL|
    |unknown|          | NEUTRAL|
    |unknown|California| NEUTRAL|
    |unknown|      null| NEUTRAL|
    +-------+----------+--------+
    only showing top 10 rows
    
    Current total count:5096
    ========= 1476947770000 ms =========
    ========= 1476947780000 ms =========
    ========= 1476947790000 ms =========
    +--------------------+--------------------+--------------------+--------------------+--------------------+
    |                 _id|                _rev|                 cde|         cdeInternal|             message|
    +--------------------+--------------------+--------------------+--------------------+--------------------+
    |bacf66c4896a92d59...|1-e01013f3b419d3c...|[[unknown,[null,n...|[null,WrappedArra...|[[995mu,1041,94,5...|
    |bacf66c4896a92d59...|1-020f864315fe3d6...|[[male,[Elizabeth...|[[true,null,false...|[[Mr. Huesken,164...|
    |bacf66c4896a92d59...|1-faa818605292480...|[[male,[Salt Lake...|[null,WrappedArra...|[[Daniel Burton,3...|
    |bacf66c4896a92d59...|1-e681a6374442ba0...|[[unknown,[Housto...|                null|[[Donirah Andrade...|
    |bacf66c4896a92d59...|1-643e24d6fbda555...|[[male,[BELLE,Uni...|[null,WrappedArra...|[[DR.BROWN-DEAN,3...|
    |bacf66c4896a92d59...|1-9f0eb0d3e8309ac...|[[unknown,[null,n...|[null,WrappedArra...|[[ekajoyce,179,25...|
    |bacf66c4896a92d59...|1-b4839ef8229e31a...|[[male,[,United S...|                null|[[Michael D. Thom...|
    |bacf66c4896a92d59...|1-c33e21907705394...|[[male,[null,null...|                null|[[Nelson B Umanzo...|
    |bacf66c4896a92d59...|1-ac848cef20b071c...|[[null,[null,null...|                null|[[nelsy Benitez,4...|
    |bacf66c4896a92d59...|1-2e527f88c6d9fc9...|[[male,[Chicago,U...|                null|[[Mark Toland,577...|
    +--------------------+--------------------+--------------------+--------------------+--------------------+
    only showing top 10 rows
    
    +-------+------------+--------+
    | gender|       state|polarity|
    +-------+------------+--------+
    |unknown|        null| NEUTRAL|
    |   male|Pennsylvania| NEUTRAL|
    |   male|        Utah| NEUTRAL|
    |unknown|       Texas| NEUTRAL|
    |   male|    Missouri|NEGATIVE|
    |unknown|        null| NEUTRAL|
    |   male|    Michigan|POSITIVE|
    |   male|        null| NEUTRAL|
    |   null|        null|    null|
    |   male|    Illinois|NEGATIVE|
    +-------+------------+--------+
    only showing top 10 rows
    
    Current total count:5227
    ========= 1476947800000 ms =========
    ========= 1476947810000 ms =========
    +--------------------+--------------------+--------------------+--------------------+--------------------+
    |                 _id|                _rev|                 cde|         cdeInternal|             message|
    +--------------------+--------------------+--------------------+--------------------+--------------------+
    |bacf66c4896a92d59...|1-e01013f3b419d3c...|[[unknown,[null,n...|[null,WrappedArra...|[[995mu,1041,94,5...|
    |bacf66c4896a92d59...|1-6d81205ffdb5aeb...|[[male,[WASHINGTO...|                null|[[The Washington ...|
    |bacf66c4896a92d59...|1-d8d702846ed578c...|[[male,[,,],[,unk...|[null,WrappedArra...|[[Mormon Democrat...|
    |bacf66c4896a92d59...|1-bb2f38a4ced7969...|[[unknown,[,Unite...|[null,WrappedArra...|[[utahpolitics,1,...|
    |bacf66c4896a92d59...|1-9f0eb0d3e8309ac...|[[unknown,[null,n...|[null,WrappedArra...|[[ekajoyce,179,25...|
    |bacf66c4896a92d59...|1-79c68d84451c1e5...|[[female,[,United...|                null|[[Sharon Jones,16...|
    |bacf66c4896a92d59...|1-10c4d0c52932bee...|[[male,[,United S...|[[true,null,null,...|[[Chuck Nellis,33...|
    |bacf66c4896a92d59...|1-ef96819a7a98d1b...|[[male,[Monticell...|[null,WrappedArra...|[[Steven Kurlande...|
    |bacf66c4896a92d59...|1-682d3282aa29a80...|[[unknown,[Accra,...|[null,WrappedArra...|[[Afoobu,218,621,...|
    |bacf66c4896a92d59...|1-59f8bbeec7c0283...|[[null,[null,null...|                null|[[Arturo Quintero...|
    +--------------------+--------------------+--------------------+--------------------+--------------------+
    only showing top 10 rows
    
    +-------+--------------------+--------+
    | gender|               state|polarity|
    +-------+--------------------+--------+
    |unknown|                null| NEUTRAL|
    |   male|District of Columbia|NEGATIVE|
    |   male|                    | NEUTRAL|
    |unknown|                Utah| NEUTRAL|
    |unknown|                null| NEUTRAL|
    | female|             Georgia|POSITIVE|
    |   male|      North Carolina| NEUTRAL|
    |   male|            New York|POSITIVE|
    |unknown|                    | NEUTRAL|
    |   null|                null|    null|
    +-------+--------------------+--------+
    only showing top 10 rows
    
    Current total count:5374
    ========= 1476947820000 ms =========
    ========= 1476947830000 ms =========
    ========= 1476947840000 ms =========
    ========= 1476947850000 ms =========
    +--------------------+--------------------+--------------------+--------------------+--------------------+
    |                 _id|                _rev|                 cde|         cdeInternal|             message|
    +--------------------+--------------------+--------------------+--------------------+--------------------+
    |c0de2a963229e6554...|1-e01013f3b419d3c...|[[unknown,[null,n...|[null,WrappedArra...|[[995mu,1041,94,5...|
    |c0de2a963229e6554...|1-ef96819a7a98d1b...|[[male,[Monticell...|[null,WrappedArra...|[[Steven Kurlande...|
    |c0de2a963229e6554...|1-9c2f0a4b09ea675...|[[null,[,United S...|[null,WrappedArra...|[[AZ After Party,...|
    |c0de2a963229e6554...|1-6d81205ffdb5aeb...|[[male,[WASHINGTO...|                null|[[The Washington ...|
    |c0de2a963229e6554...|1-10c4d0c52932bee...|[[male,[,United S...|[[true,null,null,...|[[Chuck Nellis,33...|
    |c0de2a963229e6554...|1-682d3282aa29a80...|[[unknown,[Accra,...|[null,WrappedArra...|[[Afoobu,218,621,...|
    |c0de2a963229e6554...|1-643e24d6fbda555...|[[male,[BELLE,Uni...|[null,WrappedArra...|[[DR.BROWN-DEAN,3...|
    |c0de2a963229e6554...|1-2017265ebeb6938...|[[unknown,[York,U...|                null|[[CJ,54,11,90,id:...|
    |c0de2a963229e6554...|1-9f59e2b572a70a0...|[[unknown,[Denver...|                null|[[PeePartyexpress...|
    |c0de2a963229e6554...|1-020f864315fe3d6...|[[male,[Elizabeth...|[[true,false,null...|[[Mr. Huesken,164...|
    +--------------------+--------------------+--------------------+--------------------+--------------------+
    only showing top 10 rows
    
    +-------+--------------------+--------+
    | gender|               state|polarity|
    +-------+--------------------+--------+
    |unknown|                null| NEUTRAL|
    |   male|            New York|POSITIVE|
    |   null|             Arizona|    null|
    |   male|District of Columbia|NEGATIVE|
    |   male|      North Carolina| NEUTRAL|
    |unknown|                    | NEUTRAL|
    |   male|            Missouri|NEGATIVE|
    |unknown|                    | NEUTRAL|
    |unknown|            Colorado|NEGATIVE|
    |   male|        Pennsylvania| NEUTRAL|
    +-------+--------------------+--------+
    only showing top 10 rows
    
    Current total count:5518
    ========= 1476947860000 ms =========
    ========= 1476947870000 ms =========
    ========= 1476947880000 ms =========
    ========= 1476947890000 ms =========
    ========= 1476947900000 ms =========
    ========= 1476947910000 ms =========
    ========= 1476947920000 ms =========
    ========= 1476947930000 ms =========
    ========= 1476947940000 ms =========
    ========= 1476947950000 ms =========
    ========= 1476947960000 ms =========
    ========= 1476947970000 ms =========
    ========= 1476947980000 ms =========
    ========= 1476947990000 ms =========
    ========= 1476948000000 ms =========
    ========= 1476948010000 ms =========
    ========= 1476948020000 ms =========
    ========= 1476948030000 ms =========
    ========= 1476948040000 ms =========



```scala

```
