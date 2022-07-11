package com.example.kafkaexampleconsumer.oms;

import co.elastic.clients.elasticsearch.core.IndexRequest;
import co.elastic.clients.elasticsearch.core.IndexResponse;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.elasticsearch.core.search.TotalHits;
import co.elastic.clients.elasticsearch.core.search.TotalHitsRelation;
import co.elastic.clients.json.JsonData;
import com.example.kafkaexampleconsumer.oms.ConnectElastic;
import com.google.gson.JsonObject;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.springframework.stereotype.Component;
import quickfix.*;
import quickfix.field.ApplVerID;
import quickfix.field.OrderID;

import java.io.Reader;
import java.io.StringReader;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

@Component
public class MessageProcessor implements Processor {
    @Override
    public void process(Exchange exchange) throws Exception {

        DataDictionary dataDictionary = new DataDictionary("C:\\Windows-D\\08-FSS\\FIX44.xml");
        MessageFactory messageFactory = new DefaultMessageFactory(ApplVerID.FIX44);

        String rawMessageString = exchange.getIn().getBody(String.class);

        Message message = (Message) MessageUtils.parse(messageFactory, dataDictionary, rawMessageString, false);
        JsonObject jsonObject = new JsonObject();
        Iterator<Field<?>> iterator = message.iterator();
        while (iterator.hasNext()) {
            Field<?> field = iterator.next();
            jsonObject.addProperty(dataDictionary.getFieldName(field.getField()) != null ? dataDictionary.getFieldName(field.getField()) : "" + field.getTag() + "", (String) (field.getObject() != null ? field.getObject() : ""));
        }
        Iterator<Integer> iteratorKeys = message.groupKeyIterator();
        int key;
        while (iteratorKeys.hasNext()) {
            key = iteratorKeys.next();
            for (Group group : message.getGroups(key)) {
                iterator = group.iterator();
                jsonObject = new JsonObject();
                while (iterator.hasNext()) {
                    Field<?> field = iterator.next();
                    jsonObject.addProperty(dataDictionary.getFieldName(field.getField()) != null ? dataDictionary.getFieldName(field.getField()) : "" + field.getTag() + "", (String) (field.getObject() != null ? field.getObject() : ""));
                }
            }
        }

        Reader input = new StringReader(String.valueOf(jsonObject));
        IndexRequest<JsonData> request = IndexRequest.of(i -> {
                    try {
                        return i
                                .index("order")
                                .id(String.valueOf(message.getField(new OrderID()).getValue()))
                                .withJson(input);
                    } catch (FieldNotFound fieldNotFound) {
                        fieldNotFound.printStackTrace();
                    }
                    return null;
                }
        );

        IndexResponse response = ConnectElastic.client.index(request);
        System.out.println("Indexed order with id " + response.id());


/*        if (message.getHeader().getInt(35) == 8) {


            String searchText = String.valueOf(message.getField(new OrderID()).getValue());
            System.out.println(searchText);

            SearchResponse<Map> response = ConnectElastic.client.search(s -> s
                            .index("order")
                            .query(q -> q
                                    .match(t -> t
                                            .field("OrderID")
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
                System.out.println("Found message " + order.toString());
            }

            Reader input = new StringReader(String.valueOf(jsonObject));
            IndexRequest<JsonData> request = IndexRequest.of(i -> i
                    .index("order")
                    .withJson(input)
            );

            IndexResponse response = ConnectElastic.client.index(request);
            System.out.println("Indexed into msgtype8 with id " + response.id());
        } else if (message.getHeader().getInt(35) == 9) {
            Reader input = new StringReader(String.valueOf(jsonObject));
            IndexRequest<JsonData> request = IndexRequest.of(i -> i
                    .index("order")
                    .withJson(input)
            );

            IndexResponse response = ConnectElastic.client.index(request);
            System.out.println("Indexed msgtype9 with id " + response.id());
        }

    }*/
    }
}
