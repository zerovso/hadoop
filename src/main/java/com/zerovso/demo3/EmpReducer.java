package com.zerovso.demo3;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class EmpReducer extends Reducer<IntWritable, DoubleWritable, IntWritable, DoubleWritable>{

	@Override
	protected void reduce(IntWritable key, Iterable<DoubleWritable> values,
			Reducer<IntWritable, DoubleWritable, IntWritable, DoubleWritable>.Context context) 
					throws IOException, InterruptedException {
		double sum =  0;
		int size = 0;
		for (DoubleWritable sal : values) {
			sum += sal.get();
			size++;
		}
		// 各个部门的平均工资
		context.write(key, new DoubleWritable(sum / size));
	}

}
