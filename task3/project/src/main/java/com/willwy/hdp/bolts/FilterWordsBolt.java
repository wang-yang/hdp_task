package com.willwy.hdp.bolts;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Bolt filters out a predefined set of words.
 * @author willwy 
 */
public class FilterWordsBolt extends BaseRichBolt {
  
  private static final long serialVersionUID = 6069146554651714100L;
  
  private Set<String> XXX_LIST = new HashSet<String>(Arrays.asList(new String[] {
            "tensorflow", "cnn", "rnn", "lstm", "google", "facebook", 
            "caffe","Theano","theano","Tensorflow","CNN","RNN",
            "LSTM", "Google", "Facebook"
    }));
  
    private OutputCollector collector;

    @Override
    public void prepare(@SuppressWarnings("rawtypes") Map map, 
                        TopologyContext topologyContext, 
                        OutputCollector collector) {
        this.collector = collector;
    }
    @Override
    public void execute(Tuple input) {
        String lang = (String) input.getValueByField("lang");
        String word = (String) input.getValueByField("word");
        if (XXX_LIST.contains(word)) {
            collector.emit(new Values(lang, word));
        }
    }
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("lang", "word"));
    }
}
