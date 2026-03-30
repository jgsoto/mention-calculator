package com.golden.mention_job.services;

import java.time.LocalDate;
import java.time.ZoneId;
import java.util.*;

import org.bson.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.*;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;

@Service
public class MentionService {

    @Autowired
    private MongoTemplate mongoTemplate;

    public void procesarMencionesDelDiaAnterior() {

        try {
            
            // LocalDate hoyDate = LocalDate.now(zone);
            LocalDate hoyDate = LocalDate.of(2026, 03, 29); // test

            LocalDate ayerDate = hoyDate.minusDays(1);

            String hoyStr = hoyDate.toString();
            String ayerStr = ayerDate.toString();

            // obtener acumulado por query del día actual
            Map<String, Integer> hoyMap = obtenerAcumuladoPorQuery(hoyStr);

            // obtener acumulado del día anterior (desde querycount)
            Document docAyer = mongoTemplate
                    .getCollection("querycount")
                    .find(new Document("date", ayerStr))
                    .first();

            Map<String, Integer> ayerMap = obtenerAcumuladoPorQuery(ayerStr);

            if (docAyer != null) {
                List<Document> queries = (List<Document>) docAyer.get("queries");

                for (Document q : queries) {
                    ayerMap.put(
                        q.getString("query"),
                        q.getInteger("count")
                    );
                }
            }

            List<Document> resultadoFinal = new ArrayList<>();
            int totalDia = 0;

            for (String query : hoyMap.keySet()) {

                int hoyVal = hoyMap.getOrDefault(query, 0);
                int ayerVal = ayerMap.getOrDefault(query, 0);

                int usoReal;

                if (hoyVal >= ayerVal) {
                    usoReal = hoyVal - ayerVal;
                } else {
                    // reinicio de mes
                    usoReal = hoyVal;
                }

                totalDia += usoReal;

                resultadoFinal.add(new Document()
                        .append("query", query)
                        .append("count", usoReal)
                );
            }

            // evitar duplicados
            Query queryCheck = new Query(Criteria.where("date").is(hoyStr));
            if (mongoTemplate.exists(queryCheck, "daily_mentions")) {
                System.out.println("Ya existe: " + hoyStr);
                return;
            }

            Document finalDoc = new Document()
                    .append("date", hoyStr)
                    .append("totalMentions", totalDia)
                    .append("queries", resultadoFinal);

            mongoTemplate.getCollection("daily_mentions").insertOne(finalDoc);

            System.out.println("Guardado correcto: " + hoyStr);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private Map<String, Integer> obtenerAcumuladoPorQuery(String fecha) {

        Aggregation aggregation = Aggregation.newAggregation(

            Aggregation.unwind("countDetails"),

            Aggregation.project()
                .and(
                    DateOperators.DateToString
                        .dateOf("countDetails.date")
                        .toString("%Y-%m-%d")
                        .withTimezone(DateOperators.Timezone.valueOf("UTC"))
                ).as("date")
                .andExpression("toLower(countDetails.query)").as("query")
                .and(ConvertOperators.ToInt.toInt("$countDetails.count")).as("count"),

            Aggregation.match(Criteria.where("date").is(fecha)),

            Aggregation.group("query")
                .sum("count").as("total")
        );

        AggregationResults<Document> results =
            mongoTemplate.aggregate(aggregation, "querycount", Document.class);

        Map<String, Integer> map = new HashMap<>();

        for (Document doc : results) {
            String query = doc.getString("_id");
            Integer total = doc.getInteger("total");

            map.put(query, total);
        }

        return map;
    }
}