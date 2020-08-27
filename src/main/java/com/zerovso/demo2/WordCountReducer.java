package com.zerovso.demo2;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
	
	/*
	 1.排序：hash排序
	 2.合并：
	 a:{1}
	 b:{1}
	 c:{1}
	 d:{1}
	 hello:{1,1,1,1}
	 */
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
			Reducer<Text, IntWritable, Text, IntWritable>.Context context) 
					throws IOException, InterruptedException {
		int count = 0;//统计次数
		for (IntWritable value : values) {
			count += value.get();
		}
		context.write(key, new IntWritable(count));
		
	}

}
