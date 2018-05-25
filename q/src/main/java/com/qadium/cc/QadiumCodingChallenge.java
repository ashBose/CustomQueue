package com.qadium.cc;

public interface QadiumCodingChallenge {
	public void enqueue(String msg) throws QadiumException;
	public String next(int queue_number) throws QadiumException;
}
