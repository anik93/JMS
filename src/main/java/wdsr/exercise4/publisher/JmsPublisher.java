package wdsr.exercise4.publisher;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JmsPublisher {
	private static final Logger log = LoggerFactory.getLogger(JmsPublisher.class);
	
	private final String topicName;
	private ActiveMQConnectionFactory connectionFactory = null;
	
	public JmsPublisher(final String topicName) {
		this.topicName = topicName;
		connectionFactory = new ActiveMQConnectionFactory("tcp://localhost:61616");
	}
	
	public void sendTopic() {
		try(Connection connection = connectionFactory.createConnection();
			Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);){
			connection.start();				
			Topic topic = session.createTopic(topicName);
	
			MessageProducer producer = session.createProducer(topic);
			
			producer.setDeliveryMode(DeliveryMode.PERSISTENT);
			TextMessage textMessage = null;
			long start = System.currentTimeMillis();
			for(int i=0; i<10000; i++){
				textMessage = session.createTextMessage("test_"+i);
				producer.send(textMessage);
			}
			long end = System.currentTimeMillis()-start;
			log.info("10000 persistent messages sent in {} milliseconds.",end);
			
			producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
			start = System.currentTimeMillis();
			for(int i=0; i<10000; i++){
				textMessage = session.createTextMessage("test_"+i);
				producer.send(textMessage);
			}
			end = System.currentTimeMillis()-start;
			log.info("10000 non-persistent messages sent in {} milliseconds.",end);
		} catch (Exception e){
			log.error("Error when send message to topic ", e);
		}
		
	}
}
