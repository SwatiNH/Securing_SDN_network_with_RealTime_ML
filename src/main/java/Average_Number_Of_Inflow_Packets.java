import org.apache.storm.shade.org.joda.time.LocalTime;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.ArrayList;
import java.util.Map;

public class Average_Number_Of_Inflow_Packets implements IRichBolt {
    private OutputCollector collector;
    ArrayList<Integer> timestamp= new ArrayList<Integer>();
    ArrayList<Integer> packet_lengths = new ArrayList<Integer>();
    ArrayList<Integer> number_Of_Packets = new ArrayList<Integer>();
    Integer sum_packet_lengths=0;
    Boolean Flag_Same_Flow=Boolean.FALSE;
    Boolean Flag_Same_session=Boolean.FALSE;
    Integer Packets_Flow=0;
    Integer Packets_Session=0;
    Integer Number_Of_Flows=0;
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
            Flag_Same_Flow = Boolean.FALSE;

            words = sentence.split(" ");
            System.out.println("splitting sentences into words here ");
            System.out.println(words[0]);
            time_value = new LocalTime(formatter.parse(words[0]));
            System.out.println("extracting the time from the words "+time_value);
            Integer result_minute;
            //first time
            if(timestamp.size()==0)
            {
                timestamp.add(time_value.getMinuteOfHour());
            }

            result_minute=timestamp.get(timestamp.size()-1).compareTo(time_value.getMinuteOfHour());
            if(result_minute!=0)
            {
                Flag_Same_session=Boolean.FALSE;
                collector.emit(new Values(number_Of_Packets.size()-1));
                number_Of_Packets.clear();
                timestamp.clear();
                Flag_Same_Flow=Boolean.FALSE;

                //Setting number of flows to 1, starting new session
                Number_Of_Flows=1;
            }
            else{
                Number_Of_Flows+=1;
                Flag_Same_session=Boolean.TRUE;
            }

            timestamp.add(time_value.getMinuteOfHour());


        }
        else{ //version packets or non openflow packets
            if(result) {


                System.out.println("version line below");
                //System.out.println(input);
                if (Flag_Same_Flow) {
                    Packets_Flow += 1;
                } else {
                    number_Of_Packets.add(Packets_Session);
                    Packets_Flow = 1;

                }
                if (Flag_Same_session) {
                    Packets_Session += 1;
                } else {
                    Packets_Session = 1;
                }
                System.out.println("Number of packets in the session --> " + Packets_Session);
                System.out.println("Number of packets in the flow --> " + Packets_Flow);
                System.out.println("Number of flows --> "+Number_Of_Flows);
                System.out.println("Avg number of inflow packets -->"+(Packets_Session/Number_Of_Flows));
                Flag_Same_Flow = Boolean.TRUE;
                collector.emit(new Values(number_Of_Packets));
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