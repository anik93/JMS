package wdsr.exercise4;

import wdsr.exercise4.consumer.JmsConsumer;

public class Main {

	public static void main(String[] args) {
		JmsConsumer jmsConsumer = new JmsConsumer("anik.QUEUE");
		jmsConsumer.registerCallback();
		jmsConsumer.getI();
	}

}
