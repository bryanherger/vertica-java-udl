package com.microfocus.vertica.mq;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.*;
import java.util.Properties;

public class ActiveMQConsumer {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(ActiveMQConsumer.class);

    private transient volatile boolean running;

    private transient MessageConsumer consumer;
    private transient Connection connection;

    private Properties cfg = new Properties();
    private String brokerURL = "";
    private String queueName = "";
    private String topicName = "";
    private VerticaSink verticaSink = null;

    public ActiveMQConsumer() { }

    public ActiveMQConsumer(Properties c) {
        cfg = c;
        brokerURL = c.getProperty("broker");
        queueName = c.getProperty("queue");
        // pass properties to VerticaSink class
        verticaSink = new VerticaSink(c);
    }

    public ActiveMQConsumer(String b, String q) {
        brokerURL = b;
        topicName = q;
    }

    private void init() throws JMSException {
        // Create a ConnectionFactory
        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(brokerURL);

        // Create a Connection
        connection = connectionFactory.createConnection();
        connection.start();

        // Create a Session
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        // Create the destination (Topic or Queue)
        Destination destination = null;

        // Create the destination (Topic)
        destination = session.createTopic(topicName);

        // Create a MessageConsumer from the Session to the Topic or Queue
        consumer = session.createConsumer(destination);

        // Create a MessageConsumer from the Session to the Topic or Queue
        consumer = session.createConsumer(destination);

        // pass properties to VerticaSink class and open DB connections
        verticaSink = new VerticaSink(cfg);
        verticaSink.open();
    }

    public void open() throws Exception {
        running = true;
        init();
    }

    public void run() {
        while (running) {

            try {
                // Wait for a message
                Message message = consumer.receive(1000);
                if (message instanceof MapMessage)
                {
                    MapMessage mapMessage = (MapMessage) message;
                    onMessage(mapMessage);
                }
                if (message instanceof TextMessage) {
                    TextMessage textMessage = (TextMessage) message;
                    onMessage(textMessage);
                }
                else {
                    if (message != null) {
                        LOG.error("Don't know what to do with this: " + message.toString());
                    } else {
                        LOG.error("null message, will keep waiting");
                    }
                }
            } catch (JMSException e) {
                LOG.error(e.getLocalizedMessage());
                running = false;
            }
        }
        try {
            close();
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        }
    }

    public void cancel() {
        running = false;
    }

    public void close() throws Exception {
        LOG.info("Closing");
        try {
            connection.close();
            verticaSink.close();
        } catch (JMSException e) {
            throw new RuntimeException("Error while closing ActiveMQ connection ", e);
        }
    }

    public void onMessage(MapMessage map) {
        LOG.info("Processing MapMessage");
        verticaSink.invoke(map);
    }

    public void onMessage(TextMessage txt) {
        LOG.info("Processing TextMessage");
        verticaSink.invoke(txt);
    }

}
