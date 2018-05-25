package com.company;

import com.company.model.Employee;

import com.rabbitmq.client.*;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.hibernate.*;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.cfg.Configuration;
import org.hibernate.criterion.Restrictions;
import org.hibernate.service.ServiceRegistry;
import org.json.JSONObject;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeoutException;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

public class Main {

 static SessionFactory factory;

    public static void main(String[] args) {
        try {
            TransportClient client= new PreBuiltTransportClient(Settings.EMPTY)
                    .addTransportAddress(new TransportAddress(InetAddress.getByName("localhost"), 9300));
            factory = new Configuration().configure().buildSessionFactory();

            Main ME=new Main();
            ME.reciveFromRebbit(client);
        } catch (Throwable ex) {
            System.err.println("Failed to create sessionFactory object." + ex);
            throw new ExceptionInInitializerError(ex);
        }

    }

    public static void insertDocument(Client client, String index, String type,  Employee ee) throws IOException {
        IndexResponse response = client.prepareIndex(index, type, String.valueOf(ee.getId()))
                .setSource(jsonBuilder()
                        .startObject()
                        .field("firstName", ee.getFirstName())
                        .field("lastName", ee.getLastName())
                        .field("age", ee.getAge())
                        .endObject())
                .get();
        System.out.println(response.status());
    }
    public void updateEnt(){
        Session session = factory.openSession();
        Transaction tx = null;
        Query query= session.getNamedQuery("testQuery");
        List<String> test=query.list();
        for (Iterator iterator = test.iterator(); iterator.hasNext();){
            Employee employee = (Employee) iterator.next();
            System.out.print("First Name: " + employee.getFirstName());
            System.out.print("  Last Name: " + employee.getLastName());
            System.out.println("  Salary: " + employee.getAge());
        }
        //query.executeUpdate();
        tx.commit();
    }
    public void insertEnt(Employee ee){

        Session session = factory.openSession();
        Transaction tx = null;
        try {
            tx = session.beginTransaction();
            session.save(ee);
            tx.commit();
        }catch (Exception ex){
            System.err.println(ex.getMessage());
        } finally {
            session.close();
        }
    }
    public void listEmployees(){
        Session session = factory.openSession();
        Transaction tx = null;

        try {
            tx = session.beginTransaction();
            List employees = session.createQuery("FROM Employee").list();
            for (Iterator iterator = employees.iterator(); iterator.hasNext();){
                Employee employee = (Employee) iterator.next();
                System.out.print("First Name: " + employee.getFirstName());
                System.out.print("  Last Name: " + employee.getLastName());
                System.out.println("  Salary: " + employee.getAge());
            }
            tx.commit();
        } catch (HibernateException e) {
            if (tx!=null) tx.rollback();
            e.printStackTrace();
        } finally {
            session.close();
        }
    }

    public void updateEmployee(Integer EmployeeID, int age ){
        Session session = factory.openSession();
        Transaction tx = null;

        try {
            tx = session.beginTransaction();
            Employee employee = (Employee)session.get(Employee.class, EmployeeID);
            employee.setAge( age );
            session.update(employee);
            tx.commit();
        } catch (HibernateException e) {
            if (tx!=null) tx.rollback();
            e.printStackTrace();
        } finally {
            session.close();
        }
    }

    public void deleteEmployee(Integer EmployeeID){

        Session session = factory.openSession();
        Transaction tx = null;

        try {
            Criteria cr = session.createCriteria(Employee.class);
            cr.add(Restrictions.eq("salary", 2000));
            List results = cr.list();
            tx = session.beginTransaction();
            Employee employee = (Employee)session.get(Employee.class, EmployeeID);
            session.delete(employee);
            tx.commit();
        } catch (HibernateException e) {
            if (tx!=null) tx.rollback();
            e.printStackTrace();
        } finally {
            session.close();
        }
    }
public void reciveFromRebbit(final Client client) throws IOException, TimeoutException {
    ConnectionFactory factory = new ConnectionFactory();
    factory.setHost("localhost");
    Connection connection = factory.newConnection();
    Channel channel = connection.createChannel();

    channel.queueDeclare("hello", false, false, false, null);
    System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

    Consumer consumer = new DefaultConsumer(channel) {
        @Override
        public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body)
                throws IOException {
            String message = new String(body, "UTF-8");
            System.out.println(" [x] Received '" + message + "'");
            JSONObject jsonObj = new JSONObject(message);
            Employee ee=new Employee();
            ee.setFirstName(jsonObj.getString("firstName"));
            ee.setLastName(jsonObj.getString("lastName"));
            ee.setAge(jsonObj.getInt("age"));
            ee.setId(jsonObj.getInt("id"));
            Main ME=new Main();
            ME.insertEnt(ee);
            insertDocument(client,"main","person",ee);
        }
    };
    channel.basicConsume("hello", true, consumer);
}

}
