/**
 * 
 */
package com.skc.hadoop.bilionprime.reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author sitakant
 *
 */
/*public class BillionPrimeReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	protected void reduce(Text text, Iterator<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		int sum = 0;

		while (values.hasNext()) {
			sum += values.next().get();
		}

		context.write(text, new IntWritable(sum));
	}

}*/

public class BillionPrimeReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
	
	protected void reduce(Text key, Iterable<IntWritable> values, org.apache.hadoop.mapreduce.Reducer<Text,IntWritable,Text,IntWritable>.Context context) throws java.io.IOException ,InterruptedException {
		int sum = 0;

		while (values.iterator().hasNext()) {
			sum += values.iterator().next().get();
		}

		context.write(key, new IntWritable(sum));
	}; 
	
	
}
