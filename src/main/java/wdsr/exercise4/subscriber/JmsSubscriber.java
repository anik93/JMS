package wdsr.exercise4.subscriber;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.jms.TopicSubscriber;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JmsSubscriber {
	private static final Logger log = LoggerFactory.getLogger(JmsSubscriber.class);
	
	private Connection connection = null;
	private Session session = null;
	private MessageConsumer consumer = null;
	private TopicSubscriber topicSubscriber = null;
	private int i = 0;
	
	public JmsSubscriber(final String topicName) {
		try {
			ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory("tcp://localhost:61616");
			//connectionFactory.setTrustAllPackages(true);
			connection = connectionFactory.createConnection();
			connection.setClientID("1");
			connection.start();
			session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

			Topic topic = session.createTopic(topicName);

			//consumer = session.createConsumer(topic);
			//consumer = session.createDurableSubscriber(topic, "sub");
			topicSubscriber = session.createDurableSubscriber(topic, "sub");
		} catch (Exception e) {
			log.error("Error message ", e);
		}
	}
	
	public void getMessage() {
		try {
			topicSubscriber.setMessageListener( message -> {
				
				if(message instanceof TextMessage){
					
					try {
						i++;
						log.info("No. {} Message {}", i, ((TextMessage) message).getText());
					} catch (Exception e) {
						log.error("Error message ", e);
					}
				}				
			});
		} catch (JMSException e) {
			log.error("Error message ", e);
		}
		/*MessageListener listner = new MessageListener() {
            public void onMessage(Message message) {
                try {
                    if (message instanceof TextMessage) {
                        TextMessage textMessage = (TextMessage) message;
                        log.info(textMessage.getText());
                    }
                } catch (JMSException e) {
                    log.error("error ",e);
                }
            }
        };
        try {
			consumer.setMessageListener(listner);
		} catch (JMSException e) {
			log.error("error ",e);
		}*/
	}
}
