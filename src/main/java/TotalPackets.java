import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import java.util.Stack;

import java.util.Map;

public class TotalPackets implements IRichBolt {
    private OutputCollector collector;
    Stack<Integer> number_of_packets = new Stack<Integer>();
    Integer final_result=0;
    @Override
    public void prepare(Map stormConf, TopologyContext context,
                        OutputCollector collector) {
        this.collector = collector;

    }


    @Override
    public void execute(Tuple input) {
        String sentence = input.getString(0);
        Integer packet_length=0;
        String[]  words;
        sentence = sentence.trim();
        System.out.println(sentence);
        System.out.println("sentence above >>>>>>>>>>>>>>>>>>>>>>>>>>>");

        Boolean result = sentence.startsWith("version");

        if(!number_of_packets.isEmpty())
        {
            System.out.println(number_of_packets.peek());
            System.out.println("!!!!!!!!!!!!!!");
        }

        if(result==Boolean.FALSE && sentence.endsWith("OpenFlow"))
        {

            String[] segments = sentence.split(":");
            Integer segments_length = segments.length;
            words = segments[segments_length-2].trim().split(",");
            System.out.println("openflow line containing length segment "+words);

        }
        else
        {
            words=sentence.split(",");
        }

        //extracting length value from field
        for(String word:words)
        {   word = word.trim();
            if(!word.isEmpty())
            {
                System.out.println(word);
                if(word.startsWith("length"))
                {
                    String[] length_field = word.split(" ");
                    packet_length = Integer.parseInt(length_field[1]);
                    System.out.println("extracting packet length here : "+packet_length);


                }
            }
        }
        //for new stream, push the length on stack
        //for packets of the same flow(starts with version), push current length on stack

        if(result==Boolean.TRUE)
        {
            Integer tos = number_of_packets.peek();
            System.out.println("TOS "+tos);

            Integer current_length = tos - packet_length ;
            System.out.println("current length "+current_length);
            number_of_packets.push(current_length);
            if(current_length==0)
            {
                final_result = (number_of_packets.size()-1);
                System.out.println("here "+final_result);
                collector.emit(new Values(final_result));
                number_of_packets.clear();

            }

        }
        else
        {
            if(packet_length!=0)
            {
                System.out.println("packet length in a flow : "+packet_length);

                number_of_packets.push(packet_length);
            }
            else
            {
                collector.emit(new Values(packet_length));
            }

        }

            collector.ack(input);

    }


    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

        declarer.declare(new Fields("nop"));
    }

    @Override
    public void cleanup() {
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

}