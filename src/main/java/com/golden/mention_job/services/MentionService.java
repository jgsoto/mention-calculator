package com.golden.mention_job.services;

import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import org.bson.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Sort;
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

            LocalDate hoyDate = LocalDate.of(2026, 4, 8); // test
            // LocalDate hoyDate = LocalDate.now(ZONE).minusDays(1);

            Date hoyDateMongo = Date.from(hoyDate.atStartOfDay(ZONE).toInstant());
            String hoyStr = hoyDate.toString();

            Map<String, Document> hoyMap = obtenerAcumuladoPorQuery(hoyStr);

            int insertados = 0;

            for (Document hoyData : hoyMap.values()) {

                int hoyVal = hoyData.getInteger("count", 0);
                Integer hoyServiceMonth = hoyData.getInteger("serviceMonth");

                String idE = hoyData.getString("idE");
                String query = hoyData.getString("query");

                Query existeQuery = new Query(
                        Criteria.where("date").is(hoyDateMongo)
                                .and("idE").is(idE)
                                .and("query").is(query)
                );

                if (mongoTemplate.exists(existeQuery, COLLECTION_DAILY)) {
                    continue;
                }

                Document ultimoRegistro = obtenerUltimoRegistroPrevio(idE, query, hoyDateMongo);

                int usoReal = hoyVal;

                if (ultimoRegistro != null) {

                    int ultimoCount = ultimoRegistro.getInteger("count", 0);
                    Integer ultimoServiceMonth = ultimoRegistro.getInteger("serviceMonth");

                    if (Objects.equals(hoyServiceMonth, ultimoServiceMonth)) {
                        usoReal = hoyVal - ultimoCount;
                        if (usoReal < 0) {
                            usoReal = hoyVal;
                        }
                    }
                }

                Document doc = new Document()
                        .append("date", hoyDateMongo)
                        .append("query", query)
                        .append("count", usoReal)
                        .append("idE", idE)
                        .append("qrId", hoyData.getString("qrId"))
                        .append("serviceMonth", hoyServiceMonth);

                mongoTemplate.getCollection(COLLECTION_DAILY).insertOne(doc);
                insertados++;
            }

            System.out.println(insertados > 0
                    ? "Guardado correcto: " + hoyStr + " registros: " + insertados
                    : "No había registros nuevos para guardar: " + hoyStr);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private Map<String, Document> obtenerAcumuladoPorQuery(String fecha) {

        Aggregation aggregation = Aggregation.newAggregation(
                Aggregation.unwind("countDetails"),
                Aggregation.project()
                        .and(DateOperators.DateToString
                                .dateOf("countDetails.date")
                                .toString("%Y-%m-%d")
                                .withTimezone(DateOperators.Timezone.valueOf("UTC")))
                        .as("date")
                        .andExpression("toLower(countDetails.query)").as("query")
                        .and("idE").as("idE")
                        .and("qrId").as("qrId")
                        .and("serviceMonth").as("serviceMonth")
                        .and(ConvertOperators.ToInt.toInt("$countDetails.count")).as("count"),
                Aggregation.match(Criteria.where("date").is(fecha)),
                Aggregation.group(Fields.from(
                        Fields.field("idE"),
                        Fields.field("query")
                ))
                        .sum("count").as("total")
                        .first("idE").as("idE")
                        .first("qrId").as("qrId")
                        .first("serviceMonth").as("serviceMonth")
                        .first("query").as("query")
        );

        AggregationResults<Document> results =
                mongoTemplate.aggregate(aggregation, COLLECTION_QUERYCOUNT, Document.class);

        Map<String, Document> map = new HashMap<>();

        for (Document doc : results) {

            String idE = doc.getString("idE");
            String query = doc.getString("query");

            map.put(idE + "|" + query,
                    new Document()
                            .append("count", doc.getInteger("total"))
                            .append("idE", idE)
                            .append("qrId", doc.getString("qrId"))
                            .append("serviceMonth", doc.getInteger("serviceMonth"))
                            .append("query", query)
            );
        }

        return map;
    }

    private Document obtenerUltimoRegistroPrevio(String idE, String query, Date fechaActual) {

        Query q = new Query(
                Criteria.where("query").is(query)
                        .and("idE").is(idE)
                        .and("date").lt(fechaActual)
        ).with(Sort.by(Sort.Direction.DESC, "date"));

        return mongoTemplate.findOne(q, Document.class, COLLECTION_DAILY);
    }
}