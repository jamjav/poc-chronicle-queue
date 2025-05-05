package com.example.service;

import com.example.model.DataRecord;
import lombok.extern.slf4j.Slf4j;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.File;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

@Slf4j
@Service
public class QueueService {

    @Value("${queue.path:./queue}")
    private String queuePath;

    private ChronicleQueue queue;

    @PostConstruct
    public void init() {
        File queueDir = new File(queuePath);
        if (!queueDir.exists()) {
            queueDir.mkdirs();
        }

        queue = SingleChronicleQueueBuilder
                .binary(queueDir)
                .build();

        log.info("Chronicle Queue initialized at: {}", queuePath);
    }

    @PreDestroy
    public void cleanup() {
        if (queue != null) {
            queue.close();
            log.info("Chronicle Queue closed");
        }
    }

    public void clearQueue() {
        if (queue != null) {
            queue.close();
            log.info("Queue closed for cleanup");
        }

        File queueDir = new File(queuePath);
        if (queueDir.exists()) {
            deleteDirectory(queueDir);
            log.info("Queue directory deleted: {}", queuePath);
        }

        // Reinitialize the queue
        init();
        log.info("Queue reinitialized after cleanup");
    }

    private void deleteDirectory(File directory) {
        File[] files = directory.listFiles();
        if (files != null) {
            for (File file : files) {
                if (file.isDirectory()) {
                    deleteDirectory(file);
                } else {
                    file.delete();
                }
            }
        }
        directory.delete();
    }

    public void writeRecord(DataRecord record) {
        // Generar UUID único
        record.setUniqueId(UUID.randomUUID().toString());
        // Establecer estado inicial
        record.setStatus("Initialize");

        ExcerptAppender appender = queue.acquireAppender();
        appender.writeDocument(w -> w.write("record").marshallable(m -> {
            m.write("id").text(record.getId());
            m.write("name").text(record.getName());
            m.write("phone").text(record.getPhone());
            m.write("email").text(record.getEmail());
            m.write("state").text(record.getState());
            m.write("typeNotification").text(record.getTypeNotification());
            m.write("uniqueId").text(record.getUniqueId());
            m.write("status").text(record.getStatus());
        }));
    }

    public List<DataRecord> readRecords() {
        List<DataRecord> records = new ArrayList<>();
        ExcerptTailer tailer = queue.createTailer();
        while (tailer.readDocument(w -> w.read("record").marshallable(m -> {
            DataRecord record = new DataRecord();
            record.setId(m.read("id").text());
            record.setName(m.read("name").text());
            record.setPhone(m.read("phone").text());
            record.setEmail(m.read("email").text());
            record.setState(m.read("state").text());
            record.setTypeNotification(m.read("typeNotification").text());
            record.setUniqueId(m.read("uniqueId").text());
            record.setStatus(m.read("status").text());
            records.add(record);
        }))) {
            // Continue reading
        }
        return records;
    }

    public List<DataRecord> filterRecords(Map<String, String> filters) {
        List<DataRecord> allRecords = readRecords();

        return allRecords.stream()
                .filter(record -> {
                    for (Map.Entry<String, String> entry : filters.entrySet()) {
                        String field = entry.getKey();
                        String value = entry.getValue();

                        if (value == null || value.isEmpty()) {
                            continue;
                        }

                        switch (field.toLowerCase()) {
                            case "id":
                                if (!record.getId().equalsIgnoreCase(value))
                                    return false;
                                break;
                            case "name":
                                if (!record.getName().equalsIgnoreCase(value))
                                    return false;
                                break;
                            case "phone":
                                if (!record.getPhone().equalsIgnoreCase(value))
                                    return false;
                                break;
                            case "email":
                                if (!record.getEmail().equalsIgnoreCase(value))
                                    return false;
                                break;
                            case "state":
                                if (!record.getState().equalsIgnoreCase(value))
                                    return false;
                                break;
                            case "typenotification":
                                if (!record.getTypeNotification().equalsIgnoreCase(value))
                                    return false;
                                break;
                            case "uniqueid":
                                if (!record.getUniqueId().equalsIgnoreCase(value))
                                    return false;
                                break;
                            case "status":
                                if (!record.getStatus().equalsIgnoreCase(value))
                                    return false;
                                break;
                        }
                    }
                    return true;
                })
                .collect(Collectors.toList());
    }

    public Map<String, Object> getQueueInfo() {
        Map<String, Object> info = new HashMap<>();
        File queueDir = new File(queuePath);

        // Información básica
        info.put("basePath", queuePath);
        info.put("exists", queueDir.exists());

        if (queueDir.exists()) {
            // Obtener archivos de cola
            File[] queueFiles = queueDir.listFiles((dir, name) -> name.endsWith(".cq4"));
            int numberOfQueueFiles = queueFiles != null ? queueFiles.length : 0;

            // Contar registros
            List<DataRecord> records = readRecords();

            // Agrupar por estado
            Map<String, Long> recordsByState = records.stream()
                    .collect(Collectors.groupingBy(DataRecord::getState, Collectors.counting()));

            // Agrupar por tipo de notificación
            Map<String, Long> recordsByNotificationType = records.stream()
                    .collect(Collectors.groupingBy(DataRecord::getTypeNotification, Collectors.counting()));

            // Estadísticas
            info.put("totalQueueFiles", numberOfQueueFiles);
            info.put("totalRecords", records.size());
            info.put("recordsByState", recordsByState);
            info.put("recordsByNotificationType", recordsByNotificationType);
            info.put("lastModified", new Date(queueDir.lastModified()));

            // Tamaño de la cola en disco
            long totalSize = 0;
            if (queueFiles != null) {
                for (File file : queueFiles) {
                    totalSize += file.length();
                }
            }
            info.put("totalSizeBytes", totalSize);
        }

        return info;
    }

    public Map<String, Object> getHealthInfo() {
        Map<String, Object> healthInfo = new HashMap<>();

        // Estado general del servicio
        healthInfo.put("serviceStatus", "UP");
        healthInfo.put("timestamp", new Date());

        // Información de la cola
        File queueDir = new File(queuePath);
        boolean queueExists = queueDir.exists();
        healthInfo.put("queueExists", queueExists);

        if (queueExists) {
            // Estado de la cola
            healthInfo.put("queueStatus", queue != null ? "ACTIVE" : "INACTIVE");

            // Estadísticas de la cola
            File[] queueFiles = queueDir.listFiles((dir, name) -> name.endsWith(".cq4"));
            int numberOfQueueFiles = queueFiles != null ? queueFiles.length : 0;
            healthInfo.put("numberOfQueueFiles", numberOfQueueFiles);

            // Tamaño total de la cola
            long totalSize = 0;
            if (queueFiles != null) {
                for (File file : queueFiles) {
                    totalSize += file.length();
                }
            }
            healthInfo.put("totalQueueSizeBytes", totalSize);

            // Conteo de registros
            List<DataRecord> records = readRecords();
            healthInfo.put("totalRecords", records.size());

            // Conteo por estado
            Map<String, Long> recordsByState = records.stream()
                    .collect(Collectors.groupingBy(DataRecord::getState, Collectors.counting()));
            healthInfo.put("recordsByState", recordsByState);

            // Conteo por tipo de notificación
            Map<String, Long> recordsByNotificationType = records.stream()
                    .collect(Collectors.groupingBy(DataRecord::getTypeNotification, Collectors.counting()));
            healthInfo.put("recordsByNotificationType", recordsByNotificationType);
        }

        return healthInfo;
    }
}