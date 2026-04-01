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

            //LocalDate fechaProceso = LocalDate.of(2026, 4, 9); // test
            LocalDate fechaProceso = LocalDate.now(ZONE).minusDays(1);

            Date fechaMongo = Date.from(fechaProceso.atStartOfDay(ZONE).toInstant());
            String fechaStr = fechaProceso.toString();

            Map<String, Document> hoyMap = obtenerAcumuladoPorQuery();

            int insertados = 0;

            for (Document hoyData : hoyMap.values()) {

                int hoyVal = hoyData.getInteger("count", 0);
                Integer hoyServiceMonth = hoyData.getInteger("serviceMonth");

                String idE = hoyData.getString("idE");
                String query = hoyData.getString("query");

                Query existeQuery = new Query(
                        Criteria.where("date").is(fechaMongo)
                                .and("idE").is(idE)
                                .and("query").is(query)
                );

                if (mongoTemplate.exists(existeQuery, COLLECTION_DAILY)) {
                    continue;
                }

                Document ultimoRegistro = obtenerUltimoRegistroPrevio(idE, query, fechaMongo);

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
                        .append("date", fechaMongo)
                        .append("query", query)
                        .append("count", usoReal)
                        .append("idE", idE)
                        .append("qrId", hoyData.getString("qrId"))
                        .append("serviceMonth", hoyServiceMonth);

                mongoTemplate.getCollection(COLLECTION_DAILY).insertOne(doc);
                insertados++;
            }

            System.out.println(insertados > 0
                    ? "Guardado correcto: " + fechaStr + " registros: " + insertados
                    : "No había registros nuevos para guardar: " + fechaStr);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private Map<String, Document> obtenerAcumuladoPorQuery() {

        Aggregation aggregation = Aggregation.newAggregation(
                // 🔹 ignorar registros históricos
                Aggregation.match(
                        Criteria.where("hist").is(false)
                ),
                Aggregation.project()
                        .and("idE").as("idE")
                        .and("qrId").as("qrId")
                        .and("serviceMonth").as("serviceMonth")
                        .and(ConvertOperators.ToInt.toInt("$totalCount")).as("count")
                        .and(ArrayOperators.ArrayElemAt.arrayOf("countDetails.query").elementAt(0)).as("query"),
                Aggregation.group(
                        Fields.from(
                                Fields.field("idE"),
                                Fields.field("query")
                        )
                )
                        .sum("count").as("count")
                        .first("idE").as("idE")
                        .first("qrId").as("qrId")
                        .first("serviceMonth").as("serviceMonth")
                        .first("query").as("query")
        );

        AggregationResults<Document> results
                = mongoTemplate.aggregate(aggregation, COLLECTION_QUERYCOUNT, Document.class);

        Map<String, Document> map = new HashMap<>();

        for (Document doc : results) {

            String idE = doc.getString("idE");
            String query = doc.getString("query");

            map.put(idE + "|" + query,
                    new Document()
                            .append("count", doc.getInteger("count"))
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
                Criteria.where("idE").is(idE)
                        .and("query").is(query)
                        .and("date").lt(fechaActual)
        ).with(Sort.by(Sort.Direction.DESC, "date"));

        return mongoTemplate.findOne(q, Document.class, COLLECTION_DAILY);
    }
}