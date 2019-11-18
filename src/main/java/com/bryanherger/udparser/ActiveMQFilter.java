package com.bryanherger.udparser;

import com.vertica.sdk.*;
import com.vertica.sdk.State.InputState;
import com.vertica.sdk.State.StreamState;
import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;

public class ActiveMQFilter extends UDParser {
    private transient MessageConsumer consumer;
    private transient javax.jms.Connection connection;
    int maxMessages = 1000, timeout = 60;

    public void onMessage(StreamWriter output, MapMessage map) throws JMSException, DestroyInvocation {
        int i = 0;
        while (map.getMapNames().hasMoreElements()) {
            String param = (String)map.getMapNames().nextElement();
            output.setString(i++, map.getString(param));
        }
        output.next();
    }

    public void onMessage(StreamWriter output, TextMessage txt) throws JMSException, DestroyInvocation {
        output.setString(0, txt.getText());
        output.next();
    }

    @Override
    public StreamState process(ServerInterface srvInterface, DataBuffer input, InputState inputState) {
        Map<String, Object> map = new HashMap<>();
        StreamWriter output = getStreamWriter();
        boolean running = true;
        do {
            try {
                try {
                    // Wait for a message
                    Message message = consumer.receive(10000);
                    if (message != null) {
                        srvInterface.log("Received: " + message.toString());
                    }
                    if (message instanceof MapMessage) {
                        MapMessage mapMessage = (MapMessage) message;
                        onMessage(output, mapMessage);
                        maxMessages--;
                    }
                    if (message instanceof TextMessage) {
                        TextMessage textMessage = (TextMessage) message;
                        onMessage(output, textMessage);
                        maxMessages--;
                    } else {
                        if (message != null) {
                            srvInterface.log("Don't know what to do with this: " + message.toString());
                            maxMessages--;
                        } else {
                            srvInterface.log("null message, done");
                            running = false;
                        }
                    }
                } catch (JMSException e) {
                    srvInterface.log(e.getLocalizedMessage());
                    running = false;
                }
                if (maxMessages <= 0) { running = false; }
            } catch (Exception se) {
                throw new UdfException(0, "ActiveMQ process Exception: " + se.getMessage());
            } catch (DestroyInvocation de) {
                throw new UdfException(0, "ActiveMQ process DI Exception: " + de.getMessage());
            }
        } while (running);
        return StreamState.DONE;
    }

    @Override
    public void setup(ServerInterface srvInterface, SizedColumnTypes returnType) {
        // Connection string, passed in as an argument
        String brokerURL = srvInterface.getParamReader().getString("connect");
        String topicName = srvInterface.getParamReader().getString("topic");
        if (srvInterface.getParamReader().containsParameter("messages")) {
            maxMessages = (int)srvInterface.getParamReader().getLong("messages");
        } else {
            maxMessages = 1000;
        }
        // Establish JDBC connection
        try {
            // Create a ConnectionFactory
            ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(brokerURL);

            // Create a Connection
            connection = connectionFactory.createConnection();
            //connection.start();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            QueueBrowser qBrowser = session.createBrowser(session.createQueue(topicName));
            srvInterface.log("Browsing: " + qBrowser.toString());
            connection.start();
            Enumeration<?> enumMsgs = qBrowser.getEnumeration();
            while (enumMsgs.hasMoreElements()) {
                Message message = (Message) enumMsgs.nextElement();
                srvInterface.log("Browsed: " + message.toString());
            }

            consumer = session.createConsumer(session.createQueue(topicName));

            // Create a Session
            //Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            // Create the destination (Topic or Queue)
            //Destination destination = session.createQueue(topicName);

            // Create a MessageConsumer from the Session to the Topic or Queue
            //consumer = session.createConsumer(destination);
        } catch (Exception se) {
            throw new UdfException(0, "Error polling ActiveMQ: "+se.getMessage());
        }
    }

    @Override
    public void destroy(ServerInterface srvInterface, SizedColumnTypes returnType) {
        // Try to free even on error, to minimize the risk of memory leaks.
        // But do check for errors in the end.
        try {
            connection.close();
        } catch (JMSException e) {
        }
    }
}
