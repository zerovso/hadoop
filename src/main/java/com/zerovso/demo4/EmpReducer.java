package com.zerovso.demo4;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import com.zerovso.entity.Emp;

public class EmpReducer extends Reducer<IntWritable, Text, Text, Text>{

	@Override
	protected void reduce(IntWritable key, Iterable<Text> values,
			Reducer<IntWritable, Text, Text, Text>.Context context) 
					throws IOException, InterruptedException {
		// 连接zookeeper
		ZooKeeper zk = new ZooKeeper("192.168.20.12:2181",3000,null);
		try {
			// 获取 zookeeper 指定节点的数据
			byte[] scottSalBs = zk.getData("/mrtest/scottSalTest2", null, new Stat());
			// 格式转换
			double scottSal = Double.valueOf(new String(scottSalBs));
			for (Text value : values) {
				System.out.println("遍历的内容："+value.toString());
				Emp emp = new Emp(value.toString());
				if(emp.getSal() > scottSal) {
					context.write(new Text(""), value);//获取工资比scott高的人
				}
			}
		}catch(KeeperException e) {
			e.printStackTrace();
		}
		
		
	}

}
