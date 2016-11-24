# Task 3 – Option 1: Streaming Architecture with Hive

## Hive

1. Store the twitter data in the attached file called sample_twitter_data in HDFS. The data is in json format and should not be altered.

Address: `/task3/sample_twitter_data.txt`

2. Once the data is in HDFS, create an hcat/hive schema to be able to answer the following question: What are all the tweets by the twitter user "Aimee_Cottle"? You will need to provide the query that answers this question.

Hint: there are multiple ways to do this, the preferred method involves org.apache.hcatalog.data.JsonSerDe - if that does nott work search for Json serde in the www - there are some you can compile from source to get it to work

### Some note:

The CR character (0x0d) is displayed as ^M.
Apple Macintosh     [CR]      (#x000D)            \r    Carriage Return
UNIX Based Systems  [LF]      (#x000A)            \n    line-feed
DOS Based Systems   [CR][LF]  (#x000D)(#x000A)    \r\n  carriage-return/line-feed

```sql
CREATE EXTERNAL TABLE tweets (  createddate string,  geolocation string,  tweetmessage string,  `user` struct<geoenabled:boolean, id:int, name:string, screenname:string, userlocation:string>)
  ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe' LOCATION '/user/root/';
   
   SELECT DISTINCT tweetmessage, user.name, createddate 
     FROM tweets WHERE user.name = 'Hortonworks'
         ORDER BY createddate;
```


## Streaming

Implement a storm topology that streams in tweets (https://dev.twitter.com/streaming/overview), does some interesting analytics in real-time on the tweets, and then persists into HDFS.

Storm UI: http://54.238.237.32:8744/index.html
