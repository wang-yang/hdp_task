package com.willwy.hdp.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import twitter4j.Status;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Receives tweets and emits its words over a certain length.
 */
public class WordSplitterBolt extends BaseRichBolt {

  private static final long serialVersionUID = 5151173513759399636L;
  private final int minWordLength;
  private OutputCollector collector;

  private Set<String> XXX_LIST = new HashSet<String>(Arrays.asList(new String[] {
      "tensorflow", "Tensorflow", "TensowFlow", "Google", "google", "facebook", 
      "AI", "Theano", "Caffe", "caffe" }));

  public WordSplitterBolt(int minWordLength) {
    this.minWordLength = minWordLength;
  }
  @Override
  public void prepare(@SuppressWarnings("rawtypes") Map map, 
                      TopologyContext topologyContext, 
                      OutputCollector collector) {
    this.collector = collector;
  }
  @Override
  public void execute(Tuple input) {
    Status tweet = (Status) input.getValueByField("tweet");
    String lang = tweet.getUser().getLang();

    /* 
     \p{Punct} --> Punctuation: One of !"#$%&'()*+,-./:;<=>?@[\]^_`{|}~ 
     \n        --> The newline (line feed) character ('\u000A')
     \r        --> The carriage-return character ('\u000D')
    */

    String text = tweet.getText().replaceAll("\\p{Punct}", " ")
                       .replaceAll("\\r|\\n", "").toLowerCase();

    /*Filter tweets talks about XXXX*/
    for(String keyword : XXX_LIST){
      if (text.contains(keyword)) {

        String[] words = text.split(" ");
        for (String word : words) {
          if (word.length() >= minWordLength) {
            collector.emit(new Values(lang, word));
          }
        }
      }
    }

  }
  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("lang", "word"));
  }
}
