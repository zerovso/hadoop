package com.zerovso.demo3;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Mapper;

import com.zerovso.entity.Emp;



/**
 * <p>Title: EmpMapper</p>  

 * <p>Description: 获取各个部门的平均工资  读取数据/ 输出    IntWritable 部门编号 DoubleWritable 平均工资</p>  

 * @author lingmo 
 
 * @date 2020-8-24  
 */
public class EmpMapper extends Mapper<LongWritable, Text, IntWritable, DoubleWritable> {

	@Override
	protected void map(LongWritable key, Text value, 
			Mapper<LongWritable, Text, IntWritable, DoubleWritable>.Context context)
			throws IOException, InterruptedException {
		
		// 捕获异常
		try {
			// 直接进行数据清洗
			Emp emp = new Emp(value.toString());
			
			context.write(new IntWritable(emp.getDeptno()),
					new DoubleWritable(emp.getSal()+emp.getComm()));
		}catch(Exception e) {
			e.printStackTrace();
			// 如果出现异常,该如何处理？
			// 策略之一就是丢弃该数据
		}
		
	}

	

}
