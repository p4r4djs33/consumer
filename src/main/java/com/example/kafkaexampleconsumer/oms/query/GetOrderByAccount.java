package com.example.kafkaexampleconsumer.oms.query;

import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.elasticsearch.core.search.TotalHits;
import co.elastic.clients.elasticsearch.core.search.TotalHitsRelation;
import com.example.kafkaexampleconsumer.oms.ConnectElastic;

import java.io.IOException;
import java.util.Map;
import java.util.*;

public class GetOrderByAccount {
    public static void main(String[] args) throws IOException {
        String searchText = "0001000097";

        SearchResponse<Map> response = ConnectElastic.client.search(s -> s
                        .index("order")
                        .query(q -> q
                                .match(t -> t
                                        .field("Account")
                                        .query(searchText)
                                )
                        ),
                Map.class
        );

        TotalHits total = response.hits().total();
        boolean isExactResult = total.relation() == TotalHitsRelation.Eq;

        if (isExactResult) {
            System.out.println("There are " + total.value() + " results");
        } else {
            System.out.println("There are more than " + total.value() + " results");
        }

        List<Hit<Map>> hits = response.hits().hits();
        for (Hit<Map> hit: hits) {
            Map order = hit.source();
            System.out.println("Found product " + order.toString());
        }
    }
}
