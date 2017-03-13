package com.sid.dds;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class JoinReducer extends Reducer<Text, Text, NullWritable, Text> {

	private Text resultingJoin = new Text();
	private String tableOneName = null;

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		List<String> tableOneTupleList = new ArrayList<String>();
		List<String> tableTwoTupleList = new ArrayList<String>();

		for (Text tuple : values) {
			String[] columns = tuple.toString().split(",");
			String tableName = columns[0].trim();

			// Checking if table one name is null, if yes, setting table one's name
			if (columns != null && tableOneName == null) {
				tableOneName = tableName;
				tableOneTupleList.add(tuple.toString());

			// Checking if table name is of first table, if not saving tuple to table two's list
			} else if (columns != null && !tableName.equals(tableOneName)) {
				tableTwoTupleList.add(tuple.toString());
				
			// Checking if table name is of first table and saving tuple to table first's list
			} else if (columns != null && tableName.equals(tableOneName)) {
				tableOneTupleList.add(tuple.toString());
			}
		}

		String joinTuple = "";
		for (String tableOneTuple : tableOneTupleList) {
			for (String tableTwoTuple : tableTwoTupleList) {
				joinTuple = tableOneTuple + ", " + tableTwoTuple;
				resultingJoin.set(joinTuple);
				context.write(NullWritable.get(), resultingJoin);
				resultingJoin.clear();
			}
		}
	}
}
