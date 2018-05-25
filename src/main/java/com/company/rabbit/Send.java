package com.company.rabbit;

import com.company.model.Employee;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Channel;
import org.codehaus.jackson.map.ObjectMapper;

import java.util.concurrent.TimeoutException;

public class Send  {
    private final static String QUEUE_NAME = "hello";

    public static void main(String[] args) throws java.io.IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        channel.queueDeclare(QUEUE_NAME, false, false, false, null);
        ObjectMapper mapper=new ObjectMapper();
        //String message = "{id:12, fisrtName:ggg, lastName:jjjj, age:22}";
        Employee ee=new Employee();
        ee.setFirstName("nino2");
        ee.setLastName("beridze");
        ee.setAge(26);
        ee.setId(1051);
        String message=mapper.writeValueAsString(ee);


        channel.basicPublish("", QUEUE_NAME, null, message.getBytes("UTF-8"));
        System.out.println(" [x] Sent '" + message + "'");

        channel.close();
        connection.close();

    }
}
