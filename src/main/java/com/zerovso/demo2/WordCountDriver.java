package com.zerovso.demo2;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCountDriver {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// 如果时mac或者linux不用做两步,window才需要
		// 强制加载hadoop.dll
		System.load("D:\\hadoop\\bin\\hadoop.dll");
		// 指定用户,如果当前操作的用户不时root，那么必须指定用户
		System.setProperty("HADOOP_USER_NAME", "root");
		System.setProperty("hadoop.home.dir", "D:\\hadoop");
		//连接hdfs
		//1、创建配置对象
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://192.168.20.11:9000");
		
		//创建查询任务
		Job job = Job.getInstance(conf, "wordcount");
		//指定主执行类
		job.setJarByClass(WordCountDriver.class);
		//指定map执行类
		job.setMapperClass(WordCountMapper.class);
		//指定reducer执行类
		job.setReducerClass(WordCountReducer.class);
		//指定map阶段输出类型
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		//指定reducer阶段输出类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);		
		//输入文件的地址
		FileInputFormat.setInputPaths(job, new Path("/input/wordcount"));
		//输出文件的目录
		FileOutputFormat.setOutputPath(job, new Path("/output/out03"));//输出路径必须是不存在的路径
		System.out.println(job.waitForCompletion(true));//执行

	}

}
