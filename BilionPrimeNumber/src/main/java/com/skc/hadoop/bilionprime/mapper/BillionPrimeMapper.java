/**
 * 
 */
package com.skc.hadoop.bilionprime.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
/**
 * @author sitakant
 *
 */
public class BillionPrimeMapper extends Mapper<LongWritable, Text, NullWritable, IntWritable>{
	
	final NullWritable nw = NullWritable.get();
	
	protected void map(final LongWritable key, final Text value, final Context context) throws java.io.IOException ,InterruptedException {
		String data = value.toString();
		String individData[] = data.split(",");
		for (String dat : individData) {
			int numData = Integer.valueOf(dat);
			if(isPrime(numData)) {
				context.write(nw, new IntWritable(numData));
			}
		}
		
		
		
	}
	
	public static boolean isPrime(int num) {
		int count = 1;
		boolean response = true;
		if(num == 0 || num == 1) {
			response = false;
		}
		
		for(int i=2;i<=num ; ++i) {
			if(num % i == 0) {
				++count;
			}
			if(count > 2) {
				response = false;
				break;
			}
		}
		
		return response;
	}
	
}
