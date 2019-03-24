import org.apache.storm.shade.org.joda.time.LocalTime;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Map;
import java.util.Stack;

import java.sql.Time;
import java.time.*;

public class Average_Inflow_Packet_Length implements IRichBolt {
    private OutputCollector collector;
    ArrayList<Integer> timestamp= new ArrayList<Integer>();
    ArrayList<Integer> packet_lengths = new ArrayList<Integer>();
    Integer sum_packet_lengths=0;
    //Stack<Integer> number_of_packets = new Stack<Integer>();
    Integer final_result=0;
    @Override
    public void prepare(Map stormConf, TopologyContext context,
                        OutputCollector collector) {
        this.collector = collector;

    }


    @Override
    public void execute(Tuple input) {
        //to get sentence
        String sentence = input.getString(0);
        /*
        sentence = sentence.trim();
        is the sentence flow summary or packets of flow
        */

        Boolean result = sentence.contains("version");
        Boolean result_OpenFlow = sentence.contains("OpenFlow");
        //obtain words of the sentence
        String[] words;

        //Datetime object
        LocalTime formatter = new LocalTime();
        LocalTime time_value;
        if(!result && result_OpenFlow)
        {
            words = sentence.split(" ");
            System.out.println("splitting sentences into words here ");
            System.out.println(words[0]);
            time_value = new LocalTime(formatter.parse(words[0]));
            System.out.println("extracting the time from the words "+time_value);
            Integer result_minute;
            System.out.println("length of array "+packet_lengths.size());
            //appending to session
            if(packet_lengths.size()!=0)
            {
                //comparing if time session has expired or not. if result_minute =0 then session continues
                result_minute=timestamp.get(timestamp.size()-1).compareTo(time_value.getMinuteOfHour());
                if(result_minute==0)
                {
                    //extract packet length
                    Integer packet_length = Integer.parseInt(words[words.length-2].split(":")[0]);
                    sum_packet_lengths += packet_length;
                    System.out.println("So far the sum is " + sum_packet_lengths);
                    packet_lengths.add(sum_packet_lengths);
                    timestamp.add(time_value.getMinuteOfHour());
                }
                else
                {
                    //start new session. clear old session details.
                    collector.emit(new Values(sum_packet_lengths/(packet_lengths.size())));
                    System.out.println("the sum emittted is "+sum_packet_lengths);
                    sum_packet_lengths=0;
                    packet_lengths.clear();
                    //starting new session
                    Integer packet_length = Integer.parseInt(words[words.length-2].split(":")[0]);
                    sum_packet_lengths += packet_length;
                    System.out.println("So far the sum is " + sum_packet_lengths);
                    packet_lengths.add(sum_packet_lengths);
                    timestamp.add(time_value.getMinuteOfHour());

                }
            }
            //starting new session, initially array lists are empty
            else
            {

                Integer packet_length = Integer.parseInt(words[words.length-2].split(":")[0]);
                System.out.println("extracted packet length herer " +packet_length);
                sum_packet_lengths += packet_length;
                System.out.println("So far the sum is " + sum_packet_lengths);
                packet_lengths.add(sum_packet_lengths);
                timestamp.add(time_value.getMinuteOfHour());
                collector.emit(new Values(sum_packet_lengths));
            }



        }
       else{ //version packets or non openflow packets
           collector.emit(new Values(sum_packet_lengths));
           System.out.println("version line below");
           System.out.println(input);
        }




        /*
        String sentence = input.getString(0);
        Integer packet_length=0;
        String[]  words;
        sentence = sentence.trim();
        System.out.println(sentence);
        System.out.println("sentence above >>>>>>>>");

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
        */

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