package wdsr.exercise4;

import wdsr.exercise4.sender.JmsSender;

public class Main {

	public static void main(String[] args) {
		JmsSender jmsSender = new JmsSender("anik93.QUEUE");
		jmsSender.sendTextToQueue();
	}

}
