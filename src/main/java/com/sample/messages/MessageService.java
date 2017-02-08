package com.sample.messages;


import org.apache.activemq.ActiveMQConnectionFactory;
import org.json.JSONObject;

import javax.jms.*;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Response;

@Path("/")
public class MessageService {

    @GET
    @Path("/produce/{message}")
    public Response produceMessage(@PathParam("message") String msg) {
        System.out.println(msg);
        initBroker(new MessageProducer(msg));
        return Response.status(200).entity(new JSONObject().put("Message Sent ASYNC", "OK").toString()).build();
    }

    @GET
    @Path("/consume")
    public Response consumeMessage() {
        initBroker(new MessageConsumer());
        return Response.status(200).entity(new JSONObject().put("Message Receive ASYNC", "OK").toString()).build();
    }

    /**
     *
     */
    private void initBroker(Runnable runnable) {
        Thread brokerThread = new Thread(runnable);
        brokerThread.setDaemon(false);
        brokerThread.start();
    }

    /**
     *
     */
    public static class MessageProducer implements Runnable {
        private final String message;

        MessageProducer(String message) {
            this.message = message;
        }

        @Override
        public void run() {
            System.out.println(message);

            try {
                //set up broker
                ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory("vm://localhost");
                //create connection
                Connection connection = connectionFactory.createConnection();
                connection.start();

                //create session
                Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

                //create destination and add to producer
                Destination destination = session.createQueue("dest_url");
                javax.jms.MessageProducer producer = session.createProducer(destination);

                //send message
                TextMessage blobMessage = session.createTextMessage(message);
                System.out.println("SENDING MESSAGE");
                producer.send(blobMessage);

                producer.close();
                connection.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    /**
     *
     */
    public static class MessageConsumer implements Runnable {

        @Override
        public void run() {
            System.out.println();

            try {
                //set up broker
                ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory("vm://localhost");
                //create connection
                Connection connection = connectionFactory.createConnection();
                connection.start();

                //create session
                Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

                //create destination and add to producer
                Destination destination = session.createQueue("dest_url");
                javax.jms.MessageConsumer consumer = session.createConsumer(destination);

                //wait for message
                Message message = consumer.receive(1000);

                if (message instanceof TextMessage) {
                    //send message
                    System.out.println("RECEIVED MESSAGE" + ((TextMessage) message).getText());
                } else {
                    System.out.println("RECEIVED MESSAGE" + message);
                }

                consumer.close();
                connection.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
