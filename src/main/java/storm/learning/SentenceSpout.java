package storm.learning;

import java.util.Map;
import java.util.Random;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.*;

@SuppressWarnings("serial")
public class SentenceSpout extends BaseRichSpout{
    private SpoutOutputCollector collector;
    private String[] sentences = {
        "my dog has fleas",
        "i like cold beverages",
        "the dog ate my homework"
    };

    private int index = 0;

    public void declareOutputFields(OutputFieldsDeclarer declarer){
        declarer.declare(new Fields("sentence"));
    }

    public void open(Map config, TopologyContext context, SpoutOutputCollector collector){
        this.collector = collector;
    }

    public void nextTuple(){
        try {
            this.collector.emit(new Values(sentences[index]));
            index++;
            if (index == sentences.length){
                index = 0;
            }

            // 模拟等待100ms
            Thread.sleep(100);
        //Utils.waitForMillis(1);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}