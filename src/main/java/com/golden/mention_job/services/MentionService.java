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

    private static final String COLLECTION_QUERYCOUNT = "querycount";
    private static final String COLLECTION_DAILY = "daily_mentions";
    private static final ZoneId ZONE = ZoneId.of("UTC");

    public void procesarMencionesDelDiaAnterior() {

        try {

            //LocalDate hoyDate = LocalDate.now(ZONE);
            LocalDate hoyDate = LocalDate.of(2026, 04, 8); // test
            LocalDate ayerDate = hoyDate.minusDays(1);

            String hoyStr = hoyDate.toString();
            String ayerStr = ayerDate.toString();

            // evitar duplicados antes de procesar
            if (mongoTemplate.exists(new Query(Criteria.where("date").is(hoyStr)), COLLECTION_DAILY)) {
                System.out.println("Ya existe: " + hoyStr);
                return;
            }

            Map<String, Document> hoyMap = obtenerAcumuladoPorQuery(hoyStr);
            Map<String, Document> ayerMap = obtenerAcumuladoPorQuery(ayerStr);

            List<Document> resultadoFinal = new ArrayList<>(hoyMap.size());
            int totalDia = 0;

            for (Map.Entry<String, Document> entry : hoyMap.entrySet()) {

                String query = entry.getKey();
                Document hoyData = entry.getValue();
                Document ayerData = ayerMap.get(query);

                int hoyVal = hoyData.getInteger("count", 0);
                int ayerVal = ayerData != null ? ayerData.getInteger("count", 0) : 0;

                Integer hoyServiceMonth = hoyData.getInteger("serviceMonth");
                Integer ayerServiceMonth = ayerData != null ? ayerData.getInteger("serviceMonth") : null;

                int usoReal;

                if (Objects.equals(hoyServiceMonth, ayerServiceMonth)) {
                    usoReal = hoyVal - ayerVal;
                    if (usoReal < 0) {
                        usoReal = hoyVal;
                    }
                } else {
                    usoReal = hoyVal;
                }

                totalDia += usoReal;

                resultadoFinal.add(new Document()
                        .append("query", query)
                        .append("count", usoReal)
                        .append("idE", hoyData.getString("idE"))
                        .append("qrId", hoyData.getString("qrId"))
                        .append("serviceMonth", hoyServiceMonth));
            }

            Document finalDoc = new Document()
                    .append("date", hoyStr)
                    .append("totalMentions", totalDia)
                    .append("queries", resultadoFinal);

            mongoTemplate.getCollection(COLLECTION_DAILY).insertOne(finalDoc);

            System.out.println("Guardado correcto: " + hoyStr);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

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
                        .and("idE").as("idE")
                        .and("qrId").as("qrId")
                        .and("serviceMonth").as("serviceMonth")
                        .and(ConvertOperators.ToInt.toInt("$countDetails.count")).as("count"),
                Aggregation.match(Criteria.where("date").is(fecha)),
                Aggregation.group("query")
                        .sum("count").as("total")
                        .first("idE").as("idE")
                        .first("qrId").as("qrId")
                        .first("serviceMonth").as("serviceMonth")
        );

        AggregationResults<Document> results
                = mongoTemplate.aggregate(aggregation, COLLECTION_QUERYCOUNT, Document.class);

        Map<String, Document> map = new HashMap<>();

        for (Document doc : results) {

            map.put(
                    doc.getString("_id"),
                    new Document()
                            .append("count", doc.getInteger("total"))
                            .append("idE", doc.getString("idE"))
                            .append("qrId", doc.getString("qrId"))
                            .append("serviceMonth", doc.getInteger("serviceMonth"))
            );
        }

        return map;
    }
}
