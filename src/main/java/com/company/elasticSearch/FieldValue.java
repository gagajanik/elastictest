package com.company.elasticSearch;

import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.Client;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

public class FieldValue<T> {
    private T t;

    public T getT() {
        return t;
    }

    public void setT(T t) {
        this.t = t;
    }

    public void updateDocument(Client client, String index, String type, String id, String field, T newValue) throws IOException, ExecutionException, InterruptedException {
        UpdateRequest updateRequest = new UpdateRequest();
        updateRequest.index(index);
        updateRequest.type(type);
        updateRequest.id(id);
        updateRequest.doc(jsonBuilder()
                .startObject()
                .field(field, newValue)
                .endObject());
        client.update(updateRequest).get();
    }
}
