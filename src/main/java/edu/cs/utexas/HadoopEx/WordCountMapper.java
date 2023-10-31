package edu.cs.utexas.HadoopEx;

import java.io.IOException;
import java.util.Scanner;
import java.util.StringTokenizer;
import org.apache.log4j.Logger;

import javax.naming.Context;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends Mapper<Object, Text, Text, Text> {

	// Create a counter and initialize with 1
	// Create a hadoop text object to store words
	private Logger logger = Logger.getLogger(WordCountMapper.class);

	public void map(Object key, Text value, Context context) 
			throws IOException, InterruptedException {
		Scanner scanner = new Scanner(value.toString());
		scanner.useDelimiter("\n");
		while (scanner.hasNext()) {
			// String line = itr.nextToken().trim();
			String line = scanner.next();
			String[] split_line = line.split(",");
			// Valid line must have 16 commas (17 elements)
			if (split_line.length != 9) {
				continue;
			}
			if (split_line[0].equals("DAY_OF_WEEK")) {
				continue;
			}

			try {
				int day_of_week = Integer.parseInt(split_line[0]);
				String airline = split_line[1];
				int delay = Integer.parseInt(split_line[7]);
				if (delay > 0) {
					context.write(new Text(Integer.toString(day_of_week)), new Text(airline + " " + Integer.toString(delay)));
				}
			} catch (Exception e) {
				continue;
			}
		}
	}
}