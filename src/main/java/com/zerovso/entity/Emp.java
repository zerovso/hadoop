package com.zerovso.entity;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.Writable;

public class Emp implements Writable{
	
	private Integer empno;
	private String ename;
	private String job;
	private Integer mgr;
	private Date hdate;
	private Double sal;
	private Double comm;
	private Integer deptno;
	private String value;
	
	public Emp() {
		super();
	}
	
	public Emp(String values) {
		//数据清洗
		String[] lineValues = values.split(",");
		if(lineValues.length != 8) {
			throw new RuntimeException("数值长度异常");
		}
		this.setEmpno(lineValues[0]);
		this.setEname(lineValues[1]);
		this.setJob(lineValues[2]);
		this.setMgr(lineValues[3]);
		this.setHdate(lineValues[4]);
		this.setSal(lineValues[5]);
		this.setComm(lineValues[6]);
		this.setDeptno(lineValues[7]);
//		System.out.println("构造获取的values:"+values);
		this.setValue(values);
	}

	public Integer getEmpno() {
		return empno;
	}
	public void setEmpno(Integer empno) {
		this.empno = empno;
	}
	public void setEmpno(String empno) {
		this.empno = Integer.valueOf(empno);
	}
	public String getEname() {
		return ename;
	}
	public void setEname(String ename) {
		this.ename = ename;
	}
	public String getJob() {
		return job;
	}
	public void setJob(String job) {
		this.job = job;
	}
	public Integer getMgr() {
		return mgr;
	}
	public void setMgr(Integer mgr) {
		this.mgr = mgr;
	}
	public void setMgr(String mgr) {
		try {
			this.mgr = Integer.valueOf(mgr);
		}catch(Exception e) {
			this.mgr = 0;
		}
	}
	public Date getHdate() {
		return hdate;
	}
	public void setHdate(Date hdate) {
		this.hdate = hdate;
	}
	public void setHdate(String hdate) {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		try {
			this.hdate = sdf.parse(hdate);
		} catch (ParseException e) {
			this.hdate = null;
		}
	}
	public Double getSal() {
		return sal;
	}
	public void setSal(Double sal) {
		this.sal = sal;
	}
	public void setSal(String sal) {
		this.sal = Double.valueOf(sal);
	}
	public Double getComm() {
		return comm;
	}
	public void setComm(Double comm) {
		this.comm = comm;
	}
	public void setComm(String comm) {
		try {
			this.comm = Double.valueOf(comm);
		}catch(Exception e) {
			this.comm = 0d;
		}
	}
	public Integer getDeptno() {
		return deptno;
	}
	public void setDeptno(Integer deptno) {
		this.deptno = deptno;
	}
	public void setDeptno(String deptno) {
		this.deptno = Integer.valueOf(deptno);
	}

	
	public String getValue() { 
		return value;
	}
	  
	public void setValue(String value) { 
		this.value = value; 
	}
	 
	@Override
	public String toString() {
		return "Emp [empno=" + empno + ", ename=" + ename + ", job=" + job + ", mgr=" + mgr + ", hdate=" + hdate
				+ ", sal=" + sal + ", comm=" + comm + ", deptno=" + deptno + ", value=" + value + "]";
	}
	
	//往外写的时候用的序列化
	public void write(DataOutput out) throws IOException {
		out.writeInt(this.getEmpno());
		out.writeUTF(this.getEname());
		out.writeUTF(this.getJob());
		out.writeInt(this.getMgr());
		out.writeDouble(this.getSal());
		out.writeDouble(this.getComm());
		out.writeDouble(this.getDeptno());
		out.writeUTF(this.getValue());
	}

	//读的时候用的序列化
	public void readFields(DataInput in) throws IOException {
		this.setEmpno(in.readInt());
		this.setEname(in.readUTF());
		this.setJob(in.readUTF());
		this.setMgr(in.readInt());
		this.setSal(in.readDouble());
		this.setComm(in.readDouble());
		this.setDeptno(in.readInt());
		this.setValue(in.readUTF());
	}
	
	
}
