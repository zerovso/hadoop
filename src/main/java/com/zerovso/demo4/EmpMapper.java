package com.zerovso.demo4;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

import com.zerovso.entity.Emp;



/**
 * <p>Title: EmpMapper</p>  

 * <p>Description: 获取各个部门的平均工资  读取数据/ 输出    IntWritable 部门编号 DoubleWritable 平均工资</p>  

 * @author lingmo 
 
 * @date 2020-8-24  
 */
public class EmpMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

	@Override
	protected void map(LongWritable key, Text value, 
			Mapper<LongWritable, Text, IntWritable, Text>.Context context)
			throws IOException, InterruptedException {

		try {
			Emp emp = new Emp(value.toString());
			if("scott".equalsIgnoreCase(emp.getEname())) {
				//往zk对应节点下面写值
				ZooKeeper zk = new ZooKeeper("192.168.20.12:2181",3000,null);
				/*
				 * ZooDefs.Ids.OPEN_ACL_UNSAFE 不设置权限
				 * CreateMode.PERSISTENT 节点永久有效
				 */
				zk.create("/mrtest/scottSalTest2", 
						String.valueOf(emp.getSal()).getBytes(), 
						ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			} else {
				context.write(new IntWritable(emp.getDeptno()), value);
			}
		}catch(Exception e) {
			e.printStackTrace();
		}
		
		
	}

	

}
