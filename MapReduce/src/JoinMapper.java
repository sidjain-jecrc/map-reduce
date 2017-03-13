package com.sid.dds;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class JoinMapper extends Mapper<LongWritable, Text, Text, Text> {

	private Text mapOutKey = new Text();
	private Text mapOutValue = new Text();

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String[] columns = value.toString().split(",");
		if (columns != null && columns.length > 2) {
			String joinColumn = columns[1];
			mapOutKey.set(joinColumn);
			mapOutValue.set(value.toString());
			context.write(mapOutKey, mapOutValue);
		}
	}

}
