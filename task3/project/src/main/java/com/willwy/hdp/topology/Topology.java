package com.willwy.hdp.topology;

import java.io.IOException;

import org.apache.storm.hdfs.bolt.HdfsBolt;
import org.apache.storm.hdfs.bolt.format.DefaultFileNameFormat;
import org.apache.storm.hdfs.bolt.format.DelimitedRecordFormat;
import org.apache.storm.hdfs.bolt.format.FileNameFormat;
import org.apache.storm.hdfs.bolt.format.RecordFormat;
import org.apache.storm.hdfs.bolt.rotation.FileRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.TimedRotationPolicy;
import org.apache.storm.hdfs.bolt.sync.CountSyncPolicy;
import org.apache.storm.hdfs.bolt.sync.SyncPolicy;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.StormSubmitter;

import com.willwy.hdp.spouts.TwitterSpout;
import com.willwy.hdp.bolts.FilterWordsBolt;
import com.willwy.hdp.bolts.WordCounterBolt;
import com.willwy.hdp.bolts.WordSplitterBolt;

/**
 * Topology class that sets up the Storm topology .
 */
public class Topology {
  static final String TOPOLOGY_NAME = "storm-twitter-xxx-topology";

  public static void main(String[] args) throws Exception {
    Config config = new Config();
    config.setMessageTimeoutSecs(120);
    config.setMaxTaskParallelism(3);
    config.setDebug(true);
    
    // sync the filesystem after every 1k tuples
    SyncPolicy syncPolicy = new CountSyncPolicy(1000);

    // rotate files when they reach 5MB
    FileRotationPolicy rotationPolicy = new TimedRotationPolicy(1.0f, 
                                         TimedRotationPolicy.TimeUnit.MINUTES);

    FileNameFormat fileNameFormat = new DefaultFileNameFormat()
                  .withPath("/task3/storm_twitter_out/").withExtension(".txt");

    // use "|" instead of "," for field delimiter
    RecordFormat format = new DelimitedRecordFormat().withFieldDelimiter("|");
  
    HdfsBolt hdfsBolt = new HdfsBolt()
      .withFsUrl("hdfs://ip-172-31-22-86.ap-northeast-1.compute.internal:8020")
      .withFileNameFormat(fileNameFormat)
      .withRecordFormat(format)
      .withRotationPolicy(rotationPolicy)
      .withSyncPolicy(syncPolicy);

    TopologyBuilder builder = new TopologyBuilder();
    builder.setSpout("TwitterSpout", new TwitterSpout());
    builder.setBolt("WordSplitterBolt", new WordSplitterBolt(5))
           .shuffleGrouping("TwitterSpout");
    builder.setBolt("FilterWordsBolt", new FilterWordsBolt())
           .shuffleGrouping("WordSplitterBolt");
    builder.setBolt("WordsCounterBolt", new WordCounterBolt(60, 5 * 60, 30))
           .shuffleGrouping("FilterWordsBolt");
    builder.setBolt("HdfsBoltWriter", hdfsBolt)
           .shuffleGrouping("WordsCounterBolt");

    Config conf = new Config();
    conf.setDebug(false);
    //conf.put("topology.acker.executors", 0);
    //conf.put("topology.workers", 5);
    if(args != null && args.length > 0) {
      StormSubmitter.submitTopology(args[0],
                                    conf,
                                    builder.createTopology());
    }
  }
}
