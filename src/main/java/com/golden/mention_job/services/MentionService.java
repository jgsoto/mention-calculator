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
            ZoneId zone = ZoneId.of("UTC");

            //LocalDate hoyDate = LocalDate.now(zone);
            LocalDate hoyDate = LocalDate.of(2026,03,30); // test

            LocalDate ayerDate = hoyDate.minusDays(1);

            String hoyStr = hoyDate.toString();
            String ayerStr = ayerDate.toString();

            // 🔥 AHORA devuelve Document (count + idE + qrID)
            Map<String, Document> hoyMap = obtenerAcumuladoPorQuery(hoyStr);
            Map<String, Document> ayerMap = obtenerAcumuladoPorQuery(ayerStr);

            // obtener acumulado del día anterior (desde querycount)
            Document docAyer = mongoTemplate
                    .getCollection("querycount")
                    .find(new Document("date", ayerStr))
                    .first();

            if (docAyer != null) {
                List<Document> queries = (List<Document>) docAyer.get("queries");

                for (Document q : queries) {
                    ayerMap.put(
                            q.getString("query"),
                            new Document()
                                    .append("count", q.getInteger("count"))
                    );
                }
            }

            List<Document> resultadoFinal = new ArrayList<>();
            int totalDia = 0;

            for (String query : hoyMap.keySet()) {

                Document hoyData = hoyMap.get(query);
                Document ayerData = ayerMap.get(query);

                int hoyVal = hoyData != null ? hoyData.getInteger("count", 0) : 0;
                int ayerVal = ayerData != null ? ayerData.getInteger("count", 0) : 0;

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
                        .append("idE", hoyData.getString("idE"))     // ✅ NUEVO
                        .append("qrId", hoyData.getString("qrId"))   // ✅ NUEVO
                        .append("serviceMonth", hoyData.getInteger("serviceMonth"))
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

    // 🔥 MÉTODO MODIFICADO
    private Map<String, Document> obtenerAcumuladoPorQuery(String fecha) {

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
                        .and("idE").as("idE")        // ✅ NUEVO
                        .and("qrId").as("qrId")      // ✅ NUEVO
                        .and("serviceMonth").as("serviceMonth")
                        .and(ConvertOperators.ToInt.toInt("$countDetails.count")).as("count"),
                Aggregation.match(Criteria.where("date").is(fecha)),
                Aggregation.group("query")
                        .sum("count").as("total")
                        .first("idE").as("idE")      // ✅ IMPORTANTE
                        .first("qrId").as("qrId")    // ✅ IMPORTANTE
                        .first("serviceMonth").as("serviceMonth")
        );

        AggregationResults<Document> results =
                mongoTemplate.aggregate(aggregation, "querycount", Document.class);

        Map<String, Document> map = new HashMap<>();

        for (Document doc : results) {
            String query = doc.getString("_id");

            map.put(query, new Document()
                    .append("count", doc.getInteger("total"))
                    .append("idE", doc.getString("idE"))
                    .append("qrId", doc.getString("qrId"))
                    .append("serviceMonth", doc.getInteger("serviceMonth"))
            );
        }

        return map;
    }
}