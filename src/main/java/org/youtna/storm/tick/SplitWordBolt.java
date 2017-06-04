package org.youtna.storm.tick;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.IBasicBolt;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Collection;
import java.util.Map;

/**
 * Created by v_wbytna on 2017/6/2.
 */
public class SplitWordBolt implements IBasicBolt {
    @Override
    public void prepare(Map stormConf, TopologyContext context) {

    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        String words = input.getString(0);
        String[] tt = words.split(" ");
        for (int i = 0 ;i < tt.length ;i++){
            collector.emit(new Values(tt[i]));
        }
    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("Word"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
