package com.zerovso.demo2;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Mapper;

/*
hello a  -- 输出  hello 1, a 1
hello b
hello c
hello d
结果预期：
a 1
b 2
c 3
d 4
hello 4
 */

public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	@Override
	protected void map(LongWritable key, Text value, 
			Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		// 获取一行
		String line = value.toString();
		// 将一行数据按照空格进行分割,成数据
		String[] lineValues = line.split(" ");
		// 对数组进行循环
		for (String lineValue : lineValues) {
			// 将分割好的数据，完外面写
			// hello 1
			//   a   1
			context.write(new Text(lineValue), new IntWritable(1));
		}
	}
}
