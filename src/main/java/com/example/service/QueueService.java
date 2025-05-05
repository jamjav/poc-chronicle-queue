package com.example.service;

import com.example.model.DataRecord;
import lombok.extern.slf4j.Slf4j;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
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

import net.openhft.chronicle.queue.RollCycles;

@Slf4j
@Service
public class QueueService {

    @Value("${queue.path:./queue}")
    private String queuePath;

    private ChronicleQueue queue;

    private final Object writeLock = new Object();

    @PostConstruct
    public void init() {
        try {
            File queueDir = new File(queuePath);
            if (!queueDir.exists()) {
                queueDir.mkdirs();
            }

            // Configuración más robusta de la cola
            queue = SingleChronicleQueueBuilder
                    .binary(queueDir)
                    .blockSize(64 * 1024 * 1024) // Reducir tamaño del bloque a 64MB
                    .build();

            log.info("Chronicle Queue initialized at: {}", queuePath);
        } catch (Exception e) {
            log.error("Failed to initialize Chronicle Queue", e);
            throw new RuntimeException("Failed to initialize queue", e);
        }
    }

    @PreDestroy
    public void cleanup() {
        if (queue != null) {
            try {
                queue.close();
                log.info("Chronicle Queue closed");
            } catch (Exception e) {
                log.error("Error closing queue", e);
            }
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
        if (queue == null) {
            log.error("Queue is not initialized");
            throw new IllegalStateException("Queue is not initialized");
        }

        // Generar UUID único
        record.setUniqueId(UUID.randomUUID().toString());
        // Establecer estado inicial
        record.setStatus("Initialize");

        try (ChronicleQueue queue = ChronicleQueue.singleBuilder("ruta/del/archivo").build()) {
            ExcerptAppender appender = queue.createAppender();
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
        } catch (Exception e) {
            log.error("Failed to write record with ID: {}. Error: {}", record.getId(), e.getMessage(), e);
        } catch (Throwable t) {
            log.error("Critical error occurred while writing record with ID: {}. Error: {}", record.getId(), t.getMessage(), t);
            throw t; // Re-throw the error to ensure it is not silently ignored
        }
    }

    public void writeRecords(List<DataRecord> records) {
        if (queue == null) {
            log.error("Queue is not initialized");
            throw new IllegalStateException("Queue is not initialized");
        }

        synchronized (writeLock) {
            log.info("Starting write operation for {} records", records.size());
            ExcerptAppender appender = null;
            try {
                appender = queue.createAppender();
                log.info("Acquired appender for writing records.");

                for (int i = 0; i < records.size(); i++) {
                    DataRecord record = records.get(i);
                    log.debug("Preparing to write record {} with ID: {}", i + 1, record.getId());

                    // Validar datos del registro
                    if (record.getId() == null || record.getName() == null || record.getPhone() == null ||
                            record.getEmail() == null || record.getState() == null
                            || record.getTypeNotification() == null) {
                        log.error("Record {} has null fields and will be skipped: {}", i + 1, record);
                        continue;
                    }

                    // Generar UUID único
                    record.setUniqueId(UUID.randomUUID().toString());
                    log.debug("Generated unique ID for record: {}", record.getUniqueId());

                    // Establecer estado inicial
                    record.setStatus("Initialize");
                    log.debug("Set initial status for record: {}", record.getStatus());

                    try {
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
                        log.info("Successfully wrote record {} with ID: {}", i + 1, record.getId());
                    } catch (Exception e) {
                        log.error("Failed to write record {} with ID: {}", i + 1, record.getId(), e);
                    } catch (Throwable t) {
                        log.error("Critical error occurred while writing record {} with ID: {}", i + 1, record.getId(),
                                t);
                        throw t; // Re-throw the error to ensure it is not silently ignored
                    }
                }
            } catch (Exception e) {
                log.error("Error during write operation", e);
                throw new RuntimeException("Failed to write records to queue", e);
            } finally {
                log.info("Write operation completed for {} records", records.size());
            }
        }
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

    public synchronized Map<String, Object> getQueueInfo() {
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

    public Map<String, Object> getQueueStatistics() {
        Map<String, Object> stats = new HashMap<>();

        if (queue == null) {
            log.error("Queue is not initialized");
            stats.put("error", "Queue is not initialized");
            return stats;
        }

        List<DataRecord> records = readRecords();
        int totalRecords = records.size();
        long totalDataSizeBytes = 0;

        for (DataRecord record : records) {
            // Estimar el tamaño de los datos del registro
            totalDataSizeBytes += (record.getId() != null ? record.getId().length() : 0);
            totalDataSizeBytes += (record.getName() != null ? record.getName().length() : 0);
            totalDataSizeBytes += (record.getPhone() != null ? record.getPhone().length() : 0);
            totalDataSizeBytes += (record.getEmail() != null ? record.getEmail().length() : 0);
            totalDataSizeBytes += (record.getState() != null ? record.getState().length() : 0);
            totalDataSizeBytes += (record.getTypeNotification() != null ? record.getTypeNotification().length() : 0);
            totalDataSizeBytes += (record.getUniqueId() != null ? record.getUniqueId().length() : 0);
            totalDataSizeBytes += (record.getStatus() != null ? record.getStatus().length() : 0);
        }

        // Ajustar el cálculo del porcentaje de uso
        long maxQueueSizeBytes = 500 * 1024 * 1024; // Límite de 500 MB
        long totalQueueSizeBytes = totalDataSizeBytes; // Usar el tamaño real de los datos
        double usagePercentage = ((double) totalQueueSizeBytes / maxQueueSizeBytes) * 100;

        stats.put("totalRecords", totalRecords);
        stats.put("totalDataSizeBytes", totalDataSizeBytes);
        stats.put("totalQueueSizeBytes", totalQueueSizeBytes);
        stats.put("maxQueueSizeBytes", maxQueueSizeBytes);
        stats.put("usagePercentage", String.format("%.2f%%", usagePercentage));
        stats.put("maxQueueSizeMB", maxQueueSizeBytes / (1024 * 1024));
        stats.put("totalDataSizeMB", totalDataSizeBytes / (1024 * 1024));
        stats.put("totalQueueSizeMB", totalQueueSizeBytes / (1024 * 1024));
        stats.put("status", usagePercentage >= 100 ? "FULL" : "AVAILABLE");

        log.info("Queue statistics: {} records consuming {} bytes, total queue size: {} bytes, usage: {}%",
                totalRecords, totalDataSizeBytes, totalQueueSizeBytes, usagePercentage);
        return stats;
    }
}