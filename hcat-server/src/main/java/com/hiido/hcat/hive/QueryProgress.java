package com.hiido.hcat.hive;

import java.util.LinkedList;
import java.util.List;

public class QueryProgress {

	private int state;
	private String stderr;
	
	private List<List<Field>> resultSchema = new LinkedList<List<Field>>();
	private List<String> resultDir = new LinkedList<String>();
	private List<String> jobId;
	
	public int getState() {
		return state;
	}
	public void setState(int state) {
		this.state = state;
	}
	public String getStderr() {
		return stderr;
	}
	public void setStderr(String stderr) {
		this.stderr = stderr;
	}
	public List<List<Field>> getResultSchema() {
		return resultSchema;
	}
	public void setResultSchema(List<List<Field>> resultSchema) {
		this.resultSchema = resultSchema;
	}
	public List<String> getResultDir() {
		return resultDir;
	}
	public void setResultDir(List<String> resultDir) {
		this.resultDir = resultDir;
	}
	public List<String> getJobId() {
		return jobId;
	}
	public void setJobId(List<String> jobId) {
		this.jobId = jobId;
	}
}
