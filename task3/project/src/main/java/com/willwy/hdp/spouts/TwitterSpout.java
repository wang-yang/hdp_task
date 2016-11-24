package com.willwy.hdp.spouts;

import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Reads Twitter's sample feed using the twitter4j library.
 * @author willwy 
 */

public class TwitterSpout extends BaseRichSpout {
  private static final long serialVersionUID = 1L;
  private SpoutOutputCollector collector;
  private LinkedBlockingQueue<Status> queue;
  private TwitterStream twitterStream;

  @Override
  public void open(@SuppressWarnings("rawtypes")Map conf, 
                   TopologyContext context, 
                   SpoutOutputCollector collector) {

    queue = new LinkedBlockingQueue<Status>(1000);
    this.collector = collector;

    //Configuring Twitter login information
    ConfigurationBuilder configurationBuilder = new ConfigurationBuilder();
    configurationBuilder.setDebugEnabled(true)
    .setOAuthConsumerKey("BnUMG3e9dE7E1iSv6LpN39EvO")
    .setOAuthConsumerSecret("OWnXC06jExeRcKFlGK0xVqM8GB302hV4yKKnFCvoZiriRSoUh7")
    .setOAuthAccessToken("1156536030-QspRp7wMme6xN5KqMBgBq8IEWzzSuZRrbFYEAaE")
    .setOAuthAccessTokenSecret("bF9bpiLfuIkHsBbVM8v5K2MAZAIQLiVEYfKtPNNMKMfZt");
    //.setJSONStoreEnabled(true);

    StatusListener listener = new StatusListener() {
      @Override
      public void onStatus(Status status) {
        queue.offer(status);
      }
      @Override
      public void onDeletionNotice(StatusDeletionNotice sdn) {
      }
      @Override
      public void onTrackLimitationNotice(int i) {
      }
      @Override
      public void onScrubGeo(long l, long l1) {
      }
      @Override
      public void onStallWarning(StallWarning stallWarning) {
      }
      @Override
      public void onException(Exception e) {
      }
    };

    TwitterStreamFactory factory = new TwitterStreamFactory(configurationBuilder.build());
    twitterStream = factory.getInstance();
    twitterStream.addListener(listener);
    twitterStream.sample();
  }
  @Override
  public void nextTuple() {
    Status status = queue.poll();
    if (status == null) {
      Utils.sleep(50);
    } else {
      collector.emit(new Values(status));
    }
  }
  @Override
  public void close() {
    twitterStream.shutdown();
  }
  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("tweet"));
  }
  @Override
  public Map<String, Object> getComponentConfiguration() {
    Config config = new Config();
    config.setMaxTaskParallelism(1);
    return config;
  }
  @Override
  public void ack(Object id) {
  }
  @Override
  public void fail(Object id) {
  }
}
