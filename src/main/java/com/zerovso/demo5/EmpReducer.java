package com.zerovso.demo5;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.zerovso.entity.Emp;

public class EmpReducer extends Reducer<DoubleWritable, Emp, DoubleWritable, Text>{

	@Override
	protected void reduce(DoubleWritable key, Iterable<Emp> values,
			Reducer<DoubleWritable, Emp, DoubleWritable, Text>.Context context) 
					throws IOException, InterruptedException {
		for (Emp emp : values) {
//			System.out.println("sal:"+emp.getSal()+"value:"+emp.toString());
//			System.out.println("key:"+key+"emp.getValue():"+emp.getValue());
			context.write(key, new Text(emp.getValue()));
		}
		
	}

}
