package org.hadoop.training;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.chainsaw.Main;


public class WordCount implements Tool {
		//initialising the configuration object
		private Configuration conf;


	@Override
	public Configuration getConf() {
		
		return conf; //getting conf obj
	}

	@Override
	public void setConf(Configuration conf) {
		// TODO Auto-generated method stub
		this.conf = conf; //setting the conf obj
		
	}

	@Override
	public int run(String[] args) throws Exception {
		
		//initializing the job object with configuration
		
		Job job = new Job(getConf());
		
		//setting the job name
		job.setJobName("Word Count Sample");
		
		//hadoop jar command purpose,for getting main job class
		job.setJarByClass(this.getClass());
		
		//setting the mapper class
		job.setMapperClass(WordMapper.class);
		
		//setting the reducer class
		job.setReducerClass(WordReducer.class);
		
		job.setMapOutputKeyClass(Text.class); //for setting key type
		job.setMapOutputValueClass(LongWritable.class); //for setting value type
		
		job.setOutputKeyClass(Text.class); //for setting o/p key type
		job.setOutputValueClass(LongWritable.class); // for setting o/p map type
		
		job.setInputFormatClass(TextInputFormat.class); //setting input format class
		
		job.setOutputFormatClass(org.apache.hadoop.mapreduce.lib.output.TextOutputFormat.class); // setting output format class
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		

		return job.waitForCompletion(true) ? 0 : -1;
		
	}

	public static void main(String[] args) throws Exception {
			int status= ToolRunner.run(new Configuration(), new WordCount(), args);
			System.out.println("My status: " +status);
			
	}
}

