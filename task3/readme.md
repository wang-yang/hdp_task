# Task 3 – Option 1: Streaming Architecture with Hive

## Hive

Store the twitter data in the attached file called sample_twitter_data in HDFS. The data is in json format and should not be altered.

Address: `/task3/sample_twitter_data.txt`

Once the data is in HDFS, create an hcat/hive schema to be able to answer the following question: What are all the tweets by the twitter user "Aimee_Cottle"? You will need to provide the query that answers this question.

Hint: there are multiple ways to do this, the preferred method involves org.apache.hcatalog.data.JsonSerDe - if that does nott work search for Json serde in the www - there are some you can compile from source to get it to work

### Some note:

The CR character (0x0d) is displayed as ^M.  
Apple Macintosh     [CR]      (#x000D)            \r    Carriage Return  
UNIX Based Systems  [LF]      (#x000A)            \n    line-feed  
DOS Based Systems   [CR][LF]  (#x000D)(#x000A)    \r\n  carriage-return/line-feed  

#### Json Sample:

```json
{"user":{
    "userlocation":"Cinderford, Gloucestershire",
    "id":230231618,
    "name":"Aimee",
    "screenname":"Aimee_Cottle",
    "geoenabled":true},
 "tweetmessage":"Gastroenteritis has pretty much killed me this week :( off work for a few days whilst I recover!",
 "createddate":"2013-06-20T12:08:14",
 "geolocation":null
}
```

#### SQL Solution:

```sql
set hive.support.sql11.reserved.keywords=false;
create external table tweets_json
  (tweetmessage string,
   createddate string,
   geolocation string,
   `user` struct<userlocation:string, id:int, name:string, screenname:string, geoenabled:boolean>)
row format serde 'org.apache.hive.hcatalog.data.JsonSerDe' location '/task3/';

set hive.support.sql11.reserved.keywords=false;
select distinct user.screenname, createddate, tweetmessage  
from tweets_json where user.screenname='Aimee_Cottle' 
order by createddate;
```

#### Result:

```
Aimee_Cottle    2013-06-20T12:08:14     Gastroenteritis has pretty much killed me this week :( off work for a few days whilst I recover!
```

## Streaming

Implement a storm topology that streams in tweets (https://dev.twitter.com/streaming/overview), does some interesting analytics in real-time on the tweets, and then persists into HDFS.

### Get code & compile

```bash
git clone https://github.com/wang-yang/hdp_task.git
cd hdp_task/task3/project
mvn compile
mvn assembly:assembly
```

### Submit topology to Storm cluster

```bash
storm jar storm-twitter-xxx-ranking-jar-with-dependencies.jar com.willwy.hdp.topology.Topology twitter_test
```

### Check topology status

Storm UI: http://54.238.237.32:8744/index.html


## Ref

1. https://moises-trelles.github.io/post.html#
2. http://tecadmin.net/install-apache-maven-on-centos/
3. https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DDL
4. https://community.hortonworks.com/questions/28684/creating-a-hive-table-with-orgapachehcatalogdatajs.html
5. http://hortonworks.com/tutorials/ 
