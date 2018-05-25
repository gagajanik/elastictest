package com.company.elasticSearch;

import org.codehaus.jackson.map.ObjectMapper;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

public class ElasticTest<T> {

    public static void main(String[] args) throws IOException, InterruptedException, ExecutionException {
        TransportClient client= new PreBuiltTransportClient(Settings.EMPTY)
                .addTransportAddress(new TransportAddress(InetAddress.getByName("localhost"), 9300));
       // getDocument(client,"main","person","1003");
        //deleteDocument(client,"main","person","1001");
        FieldValue<String> iVal=new FieldValue<String>();
        iVal.setT("gagaa");
       // iVal.updateDocument(client,"main","person","1002","firstName",iVal.getT());
        makeSearch(client,"search1","employee");
      // insertDocument(client,"main","person","1004","nino","janikashvili",17);
        client.close();
    }


    public static void deleteDocument(Client client, String index, String type, String id){

        DeleteResponse response = client.prepareDelete(index, type, id).execute().actionGet();
        System.out.println("Information on the deleted document:");
        System.out.println("Index: " + response.getIndex());
        System.out.println("Type: " + response.getType());
        System.out.println("Id: " + response.getId());
        System.out.println("Version: " + response.getVersion());
    }
    public static void getDocument(Client client, String index, String type, String id){
        GetResponse getResponse = client.prepareGet(index, type, id).execute().actionGet();

        Map<String, Object> source = getResponse.getSource();

        System.out.println("------------------------------");
        System.out.println("Index: " + getResponse.getIndex());
        System.out.println("Type: " + getResponse.getType());
        System.out.println("Id: " + getResponse.getId());
        System.out.println("Version: " + getResponse.getVersion());
        System.out.println(source);
        System.out.println("------------------------------");
    }
/*   public void updateDocument(Client client, String index, String type, String id, String field, T newValue) throws IOException, ExecutionException, InterruptedException {
       UpdateRequest updateRequest = new UpdateRequest();
       updateRequest.index(index);
       updateRequest.type(type);
       updateRequest.id(id);
       updateRequest.doc(jsonBuilder()
               .startObject()
               .field(field, newValue)
               .endObject());
       client.update(updateRequest).get();
   }*/
   public static void makeSearch(Client client, String index, String type) throws IOException {
       ObjectMapper mapper=new ObjectMapper();
       SearchResponse response = client.prepareSearch(index)
               .setTypes(type)
               .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
               .setQuery(QueryBuilders.termQuery("@version", "1"))                 // Query
              // .setPostFilter(QueryBuilders.rangeQuery("age").from(10).to(50))
               //.setPostFilter(QueryBuilders.rangeQuery("_id").from(1001).to(1005))// Filter
               .setFrom(0).setSize(60).setExplain(true)
               .get();
       for (SearchHit hit : response.getHits().getHits()) {
           System.out.println(hit.getSourceAsString());
       }
   }
   public static void insertDocument(Client client, String index, String type, String id, String firstName, String lastName, int age) throws IOException {
       IndexResponse response = client.prepareIndex(index, type, id)
               .setSource(jsonBuilder()
                       .startObject()
                       .field("firstName", firstName)
                       .field("lastName", lastName)
                       .field("age", age)
                       .endObject())
               .get();
   }
    }

