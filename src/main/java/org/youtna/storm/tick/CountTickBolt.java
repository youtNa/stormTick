package org.youtna.storm.tick;

import org.apache.storm.Config;
import org.apache.storm.Constants;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by v_wbytna on 2017/6/2.
 */
public class CountTickBolt extends BaseBasicBolt {
    private static final Logger logger = LoggerFactory.getLogger(CountTickBolt.class);
    private Map<String, Integer> count;
    private Long time;

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Config conf = new Config();
        conf.put(conf.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 5);
        return conf;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        super.prepare(stormConf, context);
        count = new HashMap<String, Integer>();
        time = System.currentTimeMillis();
    }

    @Override
    public void cleanup() {
        super.cleanup();
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        if (input.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID) &&
                input.getSourceStreamId().equals(Constants.SYSTEM_TICK_STREAM_ID)){
            Long nowTime = System.currentTimeMillis();
            Long useTime = nowTime - time;
            StringBuffer sb = new StringBuffer();
            sb.append("======== Use Time :" + useTime + "========\r\n");
            for (Map.Entry wordCount : count.entrySet()){
                sb.append(wordCount.getKey() + "------>" + wordCount.getValue() + "\r\n");
            }
            Long nnTime = System.currentTimeMillis();
            logger.info(sb.toString() + (nnTime - nowTime) );
            time = nnTime;
        }else {
            String word = input.getString(0);
            if (count.containsKey(word)){
                int thisWordCont = count.get(word);
                count.put(word, ++thisWordCont);
            }else {
                count.put(word,1);
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
}
