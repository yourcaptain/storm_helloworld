package storm.learning;

import backtype.storm.topology.TopologyBuilder;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.tuple.Fields;

/**
 * Hello world!
 *
 */
public class App 
{
    private static final String SETENCE_SPOUT_ID = "sentence-spout";
    private static final String SPLIT_BOLT_ID = "split-bolt";
    private static final String COUNT_BOLT_ID = "count-spout";
    private static final String REPORT_BOLT_ID = "report-spout";
    private static final String TOPOLOGY_NAME = "word-count-topology";

    public static void main( String[] args )
    {
        try
        {
            SentenceSpout spout = new SentenceSpout();
            SplitSentenceBolt splitBolt = new SplitSentenceBolt();
            WordCountBolt countBolt = new WordCountBolt();
            ReportBolt reportBolt = new ReportBolt();

            TopologyBuilder builder = new TopologyBuilder();

            builder.setSpout(SETENCE_SPOUT_ID, spout);
            builder.setBolt(SPLIT_BOLT_ID, splitBolt).shuffleGrouping(SETENCE_SPOUT_ID);
            builder.setBolt(COUNT_BOLT_ID, countBolt).fieldsGrouping(SPLIT_BOLT_ID, new Fields("word"));
            builder.setBolt(REPORT_BOLT_ID, reportBolt).globalGrouping(COUNT_BOLT_ID);

            Config config = new Config();

            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology(TOPOLOGY_NAME, config, builder.createTopology());

            Thread.sleep(10000);
            cluster.killTopology(TOPOLOGY_NAME);
            cluster.shutdown();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }    
}
