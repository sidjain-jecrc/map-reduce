package com.sid.dds;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Equijoin {

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		Job mapreduceJob = new Job(conf, "equijoin");

		mapreduceJob.setJarByClass(Equijoin.class);
		mapreduceJob.setMapperClass(JoinMapper.class);
		mapreduceJob.setReducerClass(JoinReducer.class);

		mapreduceJob.setOutputKeyClass(Text.class);
		mapreduceJob.setOutputValueClass(Text.class);
		mapreduceJob.setMapOutputKeyClass(Text.class);
		mapreduceJob.setMapOutputValueClass(Text.class);

		mapreduceJob.setInputFormatClass(TextInputFormat.class);
		mapreduceJob.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(mapreduceJob, new Path(args[1]));
		FileOutputFormat.setOutputPath(mapreduceJob, new Path(args[2]));

		boolean result = mapreduceJob.waitForCompletion(true);
		System.exit(result ? 0 : 1);

	}
}
