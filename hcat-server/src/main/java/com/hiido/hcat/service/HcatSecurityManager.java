package com.hiido.hcat.service;

public class HcatSecurityManager extends SecurityManager{
	@Override
	public void checkExit(int status) {
		super.checkExit(status);
		if(status == 0) {
			throw new SecurityException(ExitStatus.NORMAL.name());
		}
		else
			throw new SecurityException(ExitStatus.ABNORMAL.name());
	}
	
	public static enum ExitStatus {
		NORMAL,
		ABNORMAL;
	}

}
