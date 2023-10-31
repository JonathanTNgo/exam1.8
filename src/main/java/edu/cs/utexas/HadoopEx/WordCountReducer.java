package edu.cs.utexas.HadoopEx;

import java.io.IOException;
import org.apache.log4j.Logger;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.PriorityQueue;
import java.util.Comparator;
import java.util.HashMap;
import java.util.PriorityQueue;
import java.util.concurrent.PriorityBlockingQueue;

import javax.naming.Context;

public class WordCountReducer extends  Reducer<Text, Text, Text, Text> {

    private PriorityQueue<FlightData> delays;
    private HashMap<String, String> all;
    private Logger logger = Logger.getLogger(WordCountReducer.class);

    public void setup(Context context) throws IOException, InterruptedException {
        delays = new PriorityQueue<>(3);
    }

   public void reduce(Text key, Iterable<Text> values, Context context)
           throws IOException, InterruptedException {
        logger.info(key);
        logger.info(values);
       for (Text value : values) {
           try {
                delays.add(new FlightData(key.toString(), value.toString()));
                logger.info(key.toString() + " " + value.toString());
                if (delays.size() > 3) {
                    delays.poll();
                }
           } catch (Exception e) {
                continue;
           }
       }
   }

   public void cleanup(Context context) throws IOException, InterruptedException {
        while (!delays.isEmpty()) {
            FlightData curr = delays.poll();
            context.write(new Text(Integer.toString(curr.day_of_week)), new Text(curr.airline + " " + Integer.toString(curr.delay)));
        }
   }
}