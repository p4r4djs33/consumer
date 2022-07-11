package com.example.kafkaexampleconsumer.oms;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;

public class ConnectElastic {
    public static final RestClient restClient = RestClient.builder(
            new HttpHost("192.168.1.74", 9200)).build();

    // Create the transport with a Jackson mapper
    public static final ElasticsearchTransport transport = new RestClientTransport(
            restClient, new JacksonJsonpMapper());

    // And create the API client
    public static final ElasticsearchClient client = new ElasticsearchClient(transport);
}
