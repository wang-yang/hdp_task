package com.willwy.hdp.bolts;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Keeps stats on word count, calculates and logs top words about the XXXX 
 * every X second to stdout and top list every Y seconds.
 * @author willwy 
 */
public class WordCounterBolt extends BaseRichBolt {
  private static final long serialVersionUID = 2706047697068872387L;
  private static final Logger logger = LoggerFactory.getLogger(WordCounterBolt.class);
    
  /** Number of seconds before the top list will be logged to stdout. */
  private final long logIntervalSec;
    
  /** Number of seconds before the top list will be cleared. */
  private final long clearIntervalSec;
    
  /** Number of top words to store in stats. */
  private final int topListSize;

  private Map<String, Long> counter;
  private long lastLogTime;
  private long lastClearTime;
    
  private OutputCollector collector;

  public WordCounterBolt(long logIntervalSec, 
                         long clearIntervalSec, 
                         int topListSize) {
    this.logIntervalSec = logIntervalSec;
    this.clearIntervalSec = clearIntervalSec;
    this.topListSize = topListSize;
  }

  @Override
  public void prepare(@SuppressWarnings("rawtypes") Map map, 
                      TopologyContext topologyContext, 
                      OutputCollector collector) {
    counter = new HashMap<String, Long>();
    lastLogTime = System.currentTimeMillis();
    lastClearTime = System.currentTimeMillis();
    this.collector = collector;
  }

  @Override
  public void execute(Tuple input) {
    String word = (String) input.getValueByField("word");
    Long count = counter.get(word);
    String ranking_disciplines = "";
        
    count = count == null ? 1L : count + 1;
    counter.put(word, count);

    long now = System.currentTimeMillis();
    long logPeriodSec = (now - lastLogTime) / 1000;
    if (logPeriodSec > logIntervalSec) {
      logger.info("\n\n");
      logger.info("Word count: "+counter.size());

      // calculate top list:
      SortedMap<Long, String> top = new TreeMap<Long, String>();
      for (Map.Entry<String, Long> entry : counter.entrySet()) {
        long discipline_count = entry.getValue();
        String discipline_word = entry.getKey();

        top.put(discipline_count, discipline_word);
        if (top.size() > topListSize) {
          top.remove(top.firstKey());
        }
      }

      String ranking = "";
      int ranking_position = 1;
            
      // Output top list:
      for (Map.Entry<Long, String> entry : top.entrySet()) {
        ranking = ranking + ranking_position + "|" + entry.getValue() + 
                  "|" + entry.getKey() + " - " ;
        ranking_position++;
      }
      ranking_disciplines="Top xxx: " + ranking;
      // Clear top list
      long now_clear = System.currentTimeMillis();
      if (now_clear - lastClearTime > clearIntervalSec * 1000) {
        counter.clear();
        lastClearTime = now_clear;
      }
      collector.emit(new Values(ranking_disciplines));
            
    }
  }
  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("ranking_disciplines"));
  }
}
