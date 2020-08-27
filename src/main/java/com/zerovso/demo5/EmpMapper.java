package com.zerovso.demo5;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
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
public class EmpMapper extends Mapper<LongWritable, Text, DoubleWritable, Emp> {

	@Override
	protected void map(LongWritable key, Text value, 
			Mapper<LongWritable, Text, DoubleWritable, Emp>.Context context)
			throws IOException, InterruptedException {
		try {
			Emp emp = new Emp(value.toString());
			context.write(new DoubleWritable(emp.getSal()), emp);
			System.out.println("sal:"+emp.getSal()+"value:"+emp.getValue());
		}catch(Exception e) {
			e.printStackTrace();
		}
		
		
	}

	

}
