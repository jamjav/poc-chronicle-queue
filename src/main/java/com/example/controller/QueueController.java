package com.example.controller;

import com.example.model.DataRecord;
import com.example.service.QueueService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/queue")
public class QueueController {

    private final QueueService queueService;

    public QueueController(QueueService queueService) {
        this.queueService = queueService;
    }

    @PostMapping("/write")
    public ResponseEntity<String> writeRecords(@RequestBody List<DataRecord> records) {
        queueService.writeRecords(records);
        return ResponseEntity.ok("Records written successfully");
    }

    @GetMapping("/read")
    public ResponseEntity<List<DataRecord>> readRecords() {
        return ResponseEntity.ok(queueService.readRecords());
    }

    @PostMapping("/clear")
    public ResponseEntity<String> clearQueue() {
        queueService.clearQueue();
        return ResponseEntity.ok("Queue cleared successfully");
    }

    @PostMapping("/filter")
    public ResponseEntity<List<DataRecord>> filterRecords(@RequestBody Map<String, String> filters) {
        return ResponseEntity.ok(queueService.filterRecords(filters));
    }

    @GetMapping("/info")
    public ResponseEntity<Map<String, Object>> getQueueInfo() {
        return ResponseEntity.ok(queueService.getQueueInfo());
    }

    @GetMapping("/health")
    public ResponseEntity<Map<String, Object>> health() {
        Map<String, Object> healthInfo = new HashMap<>();

        // Estado general del servicio
        healthInfo.putAll(queueService.getServiceStatus());

        // Información de la cola
        healthInfo.putAll(queueService.getQueueHealthInfo());

        return ResponseEntity.ok(healthInfo);
    }

    @GetMapping("/statistics")
    public ResponseEntity<Map<String, Object>> getQueueStatistics() {
        return ResponseEntity.ok(queueService.getQueueStatistics());
    }
}