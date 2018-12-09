package org.hadoop.training;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

	@Override
	protected void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {
		// read file line by line and assign 1 to each word
		String line = value.toString();
		String[] words = line.split(" ");
		for (String word : words) {
			context.write(new Text(word), new LongWritable(1));
			
		}

	}

}
