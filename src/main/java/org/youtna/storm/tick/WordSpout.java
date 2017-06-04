package org.youtna.storm.tick;

import org.apache.storm.spout.ISpout;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Created by v_wbytna on 2017/6/2.
 */
public class WordSpout implements IRichSpout{
    private static final Logger logger = LoggerFactory.getLogger(WordSpout.class);
    private SpoutOutputCollector collector;

    String[] words = {"word sdff sdfsf"
            ,"sdfsf sdfefe seffg"
            ,"sdfs etkplk joixoncv"};

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("WORDS"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void close() {

    }

    @Override
    public void activate() {

    }

    @Override
    public void deactivate() {

    }

    @Override
    public void nextTuple() {
        int length = words.length;
        int t = (int)(Math.random()*10)%length;
        collector.emit(new Values(words[t]));
        try {
            Thread.sleep(500);
        }catch (Exception e){
            logger.error("error");
        }
    }

    @Override
    public void ack(Object msgId) {

    }

    @Override
    public void fail(Object msgId) {

    }
}
