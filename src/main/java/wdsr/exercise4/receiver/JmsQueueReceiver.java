package wdsr.exercise4.receiver;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.ObjectMessage;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wdsr.exercise4.PriceAlert;
import wdsr.exercise4.VolumeAlert;

/**
 * TODO Complete this class so that it consumes messages from the given queue and invokes the registered callback when an alert is received.
 * 
 * Assume the ActiveMQ broker is running on tcp://localhost:62616
 */
public class JmsQueueReceiver {
	private static final Logger log = LoggerFactory.getLogger(JmsQueueReceiver.class);
	
	private Connection connection = null;
	private Session session = null;
	private MessageConsumer consumer = null;
	/**
	 * Creates this object
	 * @param queueName Name of the queue to consume messages from.
	 */
	public JmsQueueReceiver(final String queueName) {
		try {
			ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory("vm://localhost");
			connectionFactory.setTrustAllPackages(true);
			connection = connectionFactory.createConnection();
			connection.start();
			session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

			Destination destination = session.createQueue(queueName);

			consumer = session.createConsumer(destination);
			
		} catch (Exception e) {
			log.error(e.getMessage());
		}
	}

	/**
	 * Registers the provided callback. The callback will be invoked when a price or volume alert is consumed from the queue.
	 * @param alertService Callback to be registered.
	 */
	public void registerCallback(AlertService alertService) {
		try {
			consumer.setMessageListener( message -> {
				try {
					if(message instanceof TextMessage){
						List<String> alert = new ArrayList<>();
						if(message.getJMSType().equals("PriceAlert")){
							String[] priceAlertBeta = ((TextMessage)message).getText().split("\n");
							
							alert.add(priceAlertBeta[0].split("=")[1]);
							alert.add(priceAlertBeta[1].split("=")[1]);
							alert.add(priceAlertBeta[2].split("=")[1]);
							
							PriceAlert priceAlert = new PriceAlert(Long.parseLong(alert.get(0)), alert.get(1), BigDecimal.valueOf(Long.parseLong(alert.get(2).replaceAll(" ", ""))));
							alertService.processPriceAlert(priceAlert);
						} else if(message.getJMSType().equals("VolumeAlert")){
							String[] volumeAlertBeta = ((TextMessage)message).getText().split("\n");
							
							alert.add(volumeAlertBeta[0].split("=")[1]);
							alert.add(volumeAlertBeta[1].split("=")[1]);
							alert.add(volumeAlertBeta[2].split("=")[1]);
							
							VolumeAlert volumeAlert = new VolumeAlert(Long.parseLong(alert.get(0)), alert.get(1), Long.parseLong(alert.get(2).replaceAll(" ", "")));
							alertService.processVolumeAlert(volumeAlert);
							
						}
					}else if(message instanceof ObjectMessage){							
						if(message.getJMSType().equals("PriceAlert")){								
							PriceAlert priceAlert = (PriceAlert) ((ObjectMessage)message).getObject();
							alertService.processPriceAlert(priceAlert);
						} else if(message.getJMSType().equals("VolumeAlert")){
							VolumeAlert volumeAlert = (VolumeAlert) ((ObjectMessage)message).getObject();
							alertService.processVolumeAlert(volumeAlert);
						}
					}
					
				} catch (JMSException e) {
					log.error(e.getMessage());
				}				
			});
		} catch (JMSException e) {
			log.error(e.getMessage());
		}


	}
	
	/**
	 * Deregisters all consumers and closes the connection to JMS broker.
	 */
	public void shutdown() {
		try {
			if(session !=null)
				session.close();
			if(connection !=null)
				connection.close();
			if(consumer !=null)
				consumer.close();
		} catch (JMSException e) {
			log.error(e.getMessage());
		}
	}

	// TODO
	// This object should start consuming messages when registerCallback method is invoked.
	
	// This object should consume two types of messages:
	// 1. Price alert - identified by header JMSType=PriceAlert - should invoke AlertService::processPriceAlert
	// 2. Volume alert - identified by header JMSType=VolumeAlert - should invoke AlertService::processVolumeAlert
	// Use different message listeners for and a JMS selector 
	
	// Each alert can come as either an ObjectMessage (with payload being an instance of PriceAlert or VolumeAlert class)
	// or as a TextMessage.
	// Text for PriceAlert looks as follows:
	//		Timestamp=<long value>
	//		Stock=<String value>
	//		Price=<long value>
	// Text for VolumeAlert looks as follows:
	//		Timestamp=<long value>
	//		Stock=<String value>
	//		Volume=<long value>
	
	// When shutdown() method is invoked on this object it should remove the listeners and close open connection to the broker.   
}
