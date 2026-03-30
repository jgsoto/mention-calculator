package com.golden.mention_job.scheduler;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import com.golden.mention_job.services.MentionService;

@Component
public class MentionScheduler {

    @Autowired
    private MentionService mentionService;

    @Scheduled(fixedRate = 10000) // cada 10 segundos
    public void runDaily() {
        System.out.println("Ejecutando job de menciones...");
        mentionService.procesarMencionesDelDiaAnterior();
    }
}