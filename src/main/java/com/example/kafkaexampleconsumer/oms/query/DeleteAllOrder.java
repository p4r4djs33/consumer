package com.example.kafkaexampleconsumer.oms.query;

import co.elastic.clients.elasticsearch.core.DeleteRequest;
import com.example.kafkaexampleconsumer.oms.ConnectElastic;

import java.io.IOException;

public class DeleteAllOrder {
    public static void main(String[] args) throws IOException {
        DeleteRequest deleteRequest = new DeleteRequest.Builder().index("order").id("*").build();
        ConnectElastic.client.delete(deleteRequest);
    }
}
