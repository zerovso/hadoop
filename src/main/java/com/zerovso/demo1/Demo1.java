package com.zerovso.demo1;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class Demo1 {

	public static void main(String[] args) throws IOException {
		// 如果时mac或者linux不用做两步,window才需要
		// 强制加载hadoop.dll
		System.load("C:\\Program Files\\hadoop\\bin\\hadoop.dll");
		// 指定用户,如果当前操作的用户不时root，那么必须指定用户
		System.setProperty("HADOOP_USER_NAME", "root");
		System.setProperty("hadoop.home.dir", "C:\\ProgramData\\hadoop");
		//连接hdfs
		//1、创建配置对象
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://192.168.20.11:9000");
		// 2、获取文件系统对象
		FileSystem fs = FileSystem.get(conf);
		//路径对象
		Path path1 = new Path("/input");
		Path path2 = new Path("/input/wordcount");
		//3、判断是否时文件
		System.out.println(fs.isFile(path1));
		System.out.println(fs.isFile(path2));
		//4、判断是否时目录
		System.out.println(fs.isDirectory(path1));
		System.out.println(fs.isDirectory(path2));
		// 5、创建目录
		if(!fs.exists(new Path("/output"))) {
			System.out.println(fs.mkdirs(new Path("/output")));
		}
		// 6、获取输出流
		FSDataOutputStream out = fs.append(path2);
		// 7、获取输入流
		FSDataInputStream in = fs.open(path2);
		
		// 8、关于流的拷贝
//		IOUtils.copyBytes(in, out, conf);
		
		InputStream input = new FileInputStream(new File("我的本地文件路径"));
		
		// 从 loacl 上传文件到 hdfs
		IOUtils.copyBytes(input, out, conf);
		
		OutputStream output = new FileOutputStream(new File("我的本地文件路径"));
		// 从 hdfs 获取文件到 local
		IOUtils.copyBytes(in, output, conf);
		fs.close();
	}

}
