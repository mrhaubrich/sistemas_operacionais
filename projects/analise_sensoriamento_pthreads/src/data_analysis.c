#include "../include/data_analysis.h"

#include <spawn.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/time.h>  // Added for gettimeofday function
#include <sys/un.h>
#include <unistd.h>
extern char **environ;

// Particiona o arquivo CSV em pedaços menores e enfileira-os na fila.
int partition_csv(const MappedCSV *csv, size_t chunk_size,
                  ThreadSafeQueue *queue) {
    if (!csv || !queue || chunk_size == 0) return 0;

    size_t chunk_count = 0;
    const char *data = csv->mapped_data;
    const char *end = data + csv->size;

    // Pular cabeçalho
    const char *header_end = strchr(data, '\n');
    if (!header_end) return 0;

    const char *curr = header_end + 1;  // Começar após o cabeçalho
    size_t header_len = strlen(csv->header);

    while (curr < end && chunk_count < (size_t)csv->data_count) {
        // Iniciar um novo chunk
        const char *chunk_start = curr;
        size_t lines_in_chunk = 0;

        // Acumular linhas até o tamanho do chunk
        while (lines_in_chunk < chunk_size && curr < end) {
            // Encontrar o fim da linha atual
            const char *line_end = strchr(curr, '\n');
            if (!line_end) {
                // Se não houver mais quebras de linha, usar o fim do arquivo
                line_end = end;
            }

            curr = line_end + 1;  // Mover para a próxima linha
            lines_in_chunk++;

            if (line_end >= end) break;  // Chegamos ao fim do arquivo
        }

        // Calcular o tamanho do chunk
        size_t data_len = curr - chunk_start;

        // Enfileirar o chunk
        thread_safe_queue_enqueue(queue, chunk_start, data_len, csv->header,
                                  header_len);
        chunk_count++;
    }

    return chunk_count;
}

/**
 * Particiona o arquivo CSV por dispositivo com algoritmo otimizado.
 * Cada dispositivo se torna um chunk separado na fila.
 */
int partition_csv_by_device(const DeviceMappedCSV *csv,
                            ThreadSafeQueue *queue) {
    if (!csv || !queue || !csv->device_table) return 0;

    // Obter todos os dispositivos únicos
    int device_count = 0;
    char **device_ids =
        device_hash_table_get_all_devices(csv->device_table, &device_count);

    if (!device_ids || device_count == 0) return 0;

    size_t header_len = strlen(csv->header);
    int chunks_created = 0;
    const char *file_start = csv->mapped_data;
    const char *file_end = file_start + csv->size;

    // Pre-compute fixed buffer size for each device - reduces allocations
    const size_t FIXED_BUFFER_SIZE = 8192;   // 8KB buffer for most devices
    const size_t LARGE_BUFFER_SIZE = 65536;  // 64KB for larger devices
    char *reusable_buffer = malloc(LARGE_BUFFER_SIZE);

    if (!reusable_buffer) {
        // Cleanup if buffer allocation fails
        for (int i = 0; i < device_count; i++) {
            free(device_ids[i]);
        }
        free(device_ids);
        return 0;
    }

    // Process devices in batches to limit memory usage
    const int DEVICES_PER_BATCH = 250;  // Increased batch size

    for (int batch_start = 0; batch_start < device_count;
         batch_start += DEVICES_PER_BATCH) {
        int batch_end = batch_start + DEVICES_PER_BATCH;
        if (batch_end > device_count) batch_end = device_count;

        // Process each device in the current batch
        for (int i = batch_start; i < batch_end; i++) {
            // Get the lines for this device
            int line_count = 0;
            const char **device_lines = device_hash_table_get_lines(
                csv->device_table, device_ids[i], &line_count);

            if (!device_lines || line_count == 0) continue;

            // Choose appropriate buffer size based on line count
            char *buffer_ptr = reusable_buffer;
            size_t buffer_size =
                line_count < 50 ? FIXED_BUFFER_SIZE : LARGE_BUFFER_SIZE;
            size_t bytes_written = 0;

            // For large devices, process in chunks
            if (line_count > 200) {
                int chunks_needed =
                    (line_count + 199) / 200;  // Ceiling division
                for (int chunk = 0; chunk < chunks_needed; chunk++) {
                    int start_idx = chunk * 200;
                    int end_idx = (chunk + 1) * 200;
                    if (end_idx > line_count) end_idx = line_count;

                    buffer_ptr = reusable_buffer;
                    bytes_written = 0;

                    // Optimize the inner loop - avoid excessive pointer checks
                    // by batching
                    for (int j = start_idx; j < end_idx; j++) {
                        if (device_lines[j] >= file_start &&
                            device_lines[j] < file_end) {
                            const char *line_end =
                                memchr(device_lines[j], '\n',
                                       file_end - device_lines[j]);
                            if (!line_end) line_end = file_end;

                            size_t line_len = line_end - device_lines[j];

                            // Ensure we don't overflow the buffer
                            if (bytes_written + line_len + 1 >= buffer_size) {
                                // Time to send the buffer and reset
                                char *final_data = malloc(bytes_written + 1);
                                if (final_data) {
                                    memcpy(final_data, reusable_buffer,
                                           bytes_written);
                                    final_data[bytes_written] = '\0';

                                    thread_safe_queue_enqueue(
                                        queue, final_data, bytes_written,
                                        csv->header, header_len);
                                    chunks_created++;
                                }

                                // Reset buffer
                                buffer_ptr = reusable_buffer;
                                bytes_written = 0;
                            }

                            // Copy line to buffer
                            memcpy(buffer_ptr, device_lines[j], line_len);
                            buffer_ptr += line_len;
                            bytes_written += line_len;

                            // Add newline if needed
                            if (*(buffer_ptr - 1) != '\n' &&
                                bytes_written < buffer_size) {
                                *buffer_ptr++ = '\n';
                                bytes_written++;
                            }
                        }
                    }

                    // Send final buffer if not empty
                    if (bytes_written > 0) {
                        char *final_data = malloc(bytes_written + 1);
                        if (final_data) {
                            memcpy(final_data, reusable_buffer, bytes_written);
                            final_data[bytes_written] = '\0';

                            thread_safe_queue_enqueue(queue, final_data,
                                                      bytes_written,
                                                      csv->header, header_len);
                            chunks_created++;
                        }
                    }
                }
            } else {
                // Optimized path for devices with fewer lines
                // Pre-calculate the total size needed
                size_t estimated_size = 0;
                for (int j = 0; j < line_count; j++) {
                    if (device_lines[j] >= file_start &&
                        device_lines[j] < file_end) {
                        const char *line_end = memchr(
                            device_lines[j], '\n', file_end - device_lines[j]);
                        if (!line_end) line_end = file_end;
                        estimated_size +=
                            (line_end - device_lines[j] + 1);  // +1 for newline
                    }
                }

                if (estimated_size == 0) continue;

                // Use a single allocation for the entire device if it fits in
                // buffer
                if (estimated_size < buffer_size) {
                    for (int j = 0; j < line_count; j++) {
                        if (device_lines[j] >= file_start &&
                            device_lines[j] < file_end) {
                            const char *line_end =
                                memchr(device_lines[j], '\n',
                                       file_end - device_lines[j]);
                            if (!line_end) line_end = file_end;

                            size_t line_len = line_end - device_lines[j];
                            memcpy(buffer_ptr, device_lines[j], line_len);
                            buffer_ptr += line_len;
                            bytes_written += line_len;

                            // Add newline if needed and not the last line
                            if (j < line_count - 1 &&
                                *(buffer_ptr - 1) != '\n') {
                                *buffer_ptr++ = '\n';
                                bytes_written++;
                            }
                        }
                    }

                    // Copy to final allocation only once
                    if (bytes_written > 0) {
                        char *final_data = malloc(bytes_written + 1);
                        if (final_data) {
                            memcpy(final_data, reusable_buffer, bytes_written);
                            final_data[bytes_written] = '\0';

                            thread_safe_queue_enqueue(queue, final_data,
                                                      bytes_written,
                                                      csv->header, header_len);
                            chunks_created++;
                        }
                    }
                } else {
                    // Fallback to original logic for larger devices
                    buffer_ptr = reusable_buffer;
                    bytes_written = 0;

                    for (int j = 0; j < line_count; j++) {
                        if (device_lines[j] >= file_start &&
                            device_lines[j] < file_end) {
                            const char *line_end =
                                memchr(device_lines[j], '\n',
                                       file_end - device_lines[j]);
                            if (!line_end) line_end = file_end;

                            size_t line_len = line_end - device_lines[j];

                            if (bytes_written + line_len + 1 >= buffer_size) {
                                char *final_data = malloc(bytes_written + 1);
                                if (final_data) {
                                    memcpy(final_data, reusable_buffer,
                                           bytes_written);
                                    final_data[bytes_written] = '\0';

                                    thread_safe_queue_enqueue(
                                        queue, final_data, bytes_written,
                                        csv->header, header_len);
                                    chunks_created++;
                                }

                                buffer_ptr = reusable_buffer;
                                bytes_written = 0;
                            }

                            memcpy(buffer_ptr, device_lines[j], line_len);
                            buffer_ptr += line_len;
                            bytes_written += line_len;

                            if (j < line_count - 1 &&
                                *(buffer_ptr - 1) != '\n') {
                                *buffer_ptr++ = '\n';
                                bytes_written++;
                            }
                        }
                    }

                    if (bytes_written > 0) {
                        char *final_data = malloc(bytes_written + 1);
                        if (final_data) {
                            memcpy(final_data, reusable_buffer, bytes_written);
                            final_data[bytes_written] = '\0';

                            thread_safe_queue_enqueue(queue, final_data,
                                                      bytes_written,
                                                      csv->header, header_len);
                            chunks_created++;
                        }
                    }
                }
            }
        }
    }

    // Cleanup
    free(reusable_buffer);
    for (int i = 0; i < device_count; i++) {
        free(device_ids[i]);
    }
    free(device_ids);

    return chunks_created;
}

/**
 * Particiona o arquivo CSV por dispositivo com agrupamento inteligente.
 * Dispositivos pequenos são agrupados para reduzir overhead de processamento.
 */
int partition_csv_by_device_optimized(const DeviceMappedCSV *csv,
                                      ThreadSafeQueue *queue) {
    if (!csv || !queue || !csv->device_table) return 0;

    struct timeval start_time, end_time;
    double elapsed_ms;
    gettimeofday(&start_time, NULL);

    // Obter todos os dispositivos únicos
    int device_count = 0;
    char **device_ids =
        device_hash_table_get_all_devices(csv->device_table, &device_count);

    if (!device_ids || device_count == 0) return 0;

    size_t header_len = strlen(csv->header);
    int chunks_created = 0;
    const char *file_start = csv->mapped_data;
    const char *file_end = file_start + csv->size;

    // Parâmetros para agrupar dispositivos pequenos
    const int SMALL_DEVICE_THRESHOLD =
        100;  // Dispositivos com menos de 100 linhas
    const int LINES_PER_CHUNK_TARGET = 10000;  // Aprox. linhas por chunk

    printf(
        "[CHUNKING] Smart binning devices: grouping small devices (<%d lines) "
        "into chunks of ~%d lines\n",
        SMALL_DEVICE_THRESHOLD, LINES_PER_CHUNK_TARGET);

    // Classificar dispositivos em pequenos e grandes
    typedef struct {
        char *device_id;
        int line_count;
    } DeviceInfo;

    DeviceInfo *small_devices = malloc(device_count * sizeof(DeviceInfo));
    DeviceInfo *large_devices = malloc(device_count * sizeof(DeviceInfo));
    int small_count = 0;
    int large_count = 0;

    if (!small_devices || !large_devices) {
        fprintf(stderr, "Failed to allocate memory for device sorting\n");
        // Cleanup
        for (int i = 0; i < device_count; i++) {
            free(device_ids[i]);
        }
        free(device_ids);
        if (small_devices) free(small_devices);
        if (large_devices) free(large_devices);
        return 0;
    }

    // Classificar dispositivos por tamanho
    for (int i = 0; i < device_count; i++) {
        int line_count = 0;
        device_hash_table_get_lines(csv->device_table, device_ids[i],
                                    &line_count);

        if (line_count < SMALL_DEVICE_THRESHOLD) {
            small_devices[small_count].device_id = device_ids[i];
            small_devices[small_count].line_count = line_count;
            small_count++;
        } else {
            large_devices[large_count].device_id = device_ids[i];
            large_devices[large_count].line_count = line_count;
            large_count++;
        }
    }

    printf("[CHUNKING] Classified %d devices as small, %d as large\n",
           small_count, large_count);

    // Buffer reuse strategy - use larger buffer for efficiency
    const size_t BUFFER_SIZE = 1024 * 1024;  // 1MB buffer
    char *reusable_buffer = malloc(BUFFER_SIZE);
    if (!reusable_buffer) {
        fprintf(stderr, "Failed to allocate reusable buffer\n");
        // Cleanup code
        free(small_devices);
        free(large_devices);
        for (int i = 0; i < device_count; i++) {
            free(device_ids[i]);
        }
        free(device_ids);
        return 0;
    }

    // 1. Processar dispositivos grandes individualmente
    printf("[CHUNKING] Processing %d large devices individually\n",
           large_count);
    for (int i = 0; i < large_count; i++) {
        int line_count = 0;
        const char **device_lines = device_hash_table_get_lines(
            csv->device_table, large_devices[i].device_id, &line_count);

        if (!device_lines || line_count == 0) continue;

        // Reset buffer
        char *buffer_ptr = reusable_buffer;
        size_t bytes_written = 0;

        // Copy device data to buffer
        for (int j = 0; j < line_count; j++) {
            if (device_lines[j] >= file_start && device_lines[j] < file_end) {
                const char *line_end =
                    memchr(device_lines[j], '\n', file_end - device_lines[j]);
                if (!line_end) line_end = file_end;

                size_t line_len = line_end - device_lines[j];

                // Check if we need to flush buffer
                if (bytes_written + line_len + 2 >= BUFFER_SIZE) {
                    // Create chunk from current buffer
                    char *chunk_data = malloc(bytes_written + 1);
                    if (chunk_data) {
                        memcpy(chunk_data, reusable_buffer, bytes_written);
                        chunk_data[bytes_written] = '\0';
                        thread_safe_queue_enqueue(queue, chunk_data,
                                                  bytes_written, csv->header,
                                                  header_len);
                        chunks_created++;
                    }

                    // Reset buffer
                    buffer_ptr = reusable_buffer;
                    bytes_written = 0;
                }

                // Copy line to buffer
                memcpy(buffer_ptr, device_lines[j], line_len);
                buffer_ptr += line_len;
                bytes_written += line_len;

                // Add newline if needed
                if (j < line_count - 1 && *(buffer_ptr - 1) != '\n') {
                    *buffer_ptr++ = '\n';
                    bytes_written++;
                }
            }
        }

        // Create final chunk if any data remains
        if (bytes_written > 0) {
            char *chunk_data = malloc(bytes_written + 1);
            if (chunk_data) {
                memcpy(chunk_data, reusable_buffer, bytes_written);
                chunk_data[bytes_written] = '\0';
                thread_safe_queue_enqueue(queue, chunk_data, bytes_written,
                                          csv->header, header_len);
                chunks_created++;
            }
        }
    }

    // 2. Agrupar dispositivos pequenos em chunks balanceados
    printf("[CHUNKING] Grouping %d small devices into balanced chunks\n",
           small_count);

    if (small_count > 0) {
        // Sort small devices by line count (descending) for better balancing
        // Using bubble sort as the dataset is typically small
        for (int i = 0; i < small_count - 1; i++) {
            for (int j = 0; j < small_count - i - 1; j++) {
                if (small_devices[j].line_count <
                    small_devices[j + 1].line_count) {
                    DeviceInfo temp = small_devices[j];
                    small_devices[j] = small_devices[j + 1];
                    small_devices[j + 1] = temp;
                }
            }
        }

        // Group small devices into chunks
        int current_lines = 0;
        int devices_in_chunk = 0;
        char *buffer_ptr = reusable_buffer;
        size_t bytes_written = 0;

        for (int i = 0; i < small_count; i++) {
            int line_count = 0;
            const char **device_lines = device_hash_table_get_lines(
                csv->device_table, small_devices[i].device_id, &line_count);

            if (!device_lines || line_count == 0) continue;

            // Create a new chunk if this device would exceed our target
            // or if buffer is getting full
            if ((current_lines > 0 &&
                 current_lines + line_count > LINES_PER_CHUNK_TARGET) ||
                (bytes_written > BUFFER_SIZE / 2)) {
                // Create chunk from current buffer
                if (bytes_written > 0) {
                    char *chunk_data = malloc(bytes_written + 1);
                    if (chunk_data) {
                        memcpy(chunk_data, reusable_buffer, bytes_written);
                        chunk_data[bytes_written] = '\0';
                        thread_safe_queue_enqueue(queue, chunk_data,
                                                  bytes_written, csv->header,
                                                  header_len);
                        chunks_created++;
                    }

                    printf(
                        "[CHUNKING] Created small-device chunk with %d devices "
                        "(%d lines, %zu bytes)\n",
                        devices_in_chunk, current_lines, bytes_written);

                    // Reset counters and buffer
                    buffer_ptr = reusable_buffer;
                    bytes_written = 0;
                    current_lines = 0;
                    devices_in_chunk = 0;
                }
            }

            // Add device data to current chunk
            for (int j = 0; j < line_count; j++) {
                if (device_lines[j] >= file_start &&
                    device_lines[j] < file_end) {
                    const char *line_end = memchr(device_lines[j], '\n',
                                                  file_end - device_lines[j]);
                    if (!line_end) line_end = file_end;

                    size_t line_len = line_end - device_lines[j];

                    // Check if we need a larger buffer
                    if (bytes_written + line_len + 2 >= BUFFER_SIZE) {
                        // Create chunk from current buffer
                        char *chunk_data = malloc(bytes_written + 1);
                        if (chunk_data) {
                            memcpy(chunk_data, reusable_buffer, bytes_written);
                            chunk_data[bytes_written] = '\0';
                            thread_safe_queue_enqueue(queue, chunk_data,
                                                      bytes_written,
                                                      csv->header, header_len);
                            chunks_created++;

                            printf(
                                "[CHUNKING] Created partial small-device chunk "
                                "with %zu bytes\n",
                                bytes_written);
                        }

                        // Reset buffer
                        buffer_ptr = reusable_buffer;
                        bytes_written = 0;
                    }

                    // Copy line to buffer
                    memcpy(buffer_ptr, device_lines[j], line_len);
                    buffer_ptr += line_len;
                    bytes_written += line_len;

                    // Add newline if needed
                    if (*(buffer_ptr - 1) != '\n') {
                        *buffer_ptr++ = '\n';
                        bytes_written++;
                    }
                }
            }

            current_lines += line_count;
            devices_in_chunk++;
        }

        // Create final chunk if any data remains
        if (bytes_written > 0) {
            char *chunk_data = malloc(bytes_written + 1);
            if (chunk_data) {
                memcpy(chunk_data, reusable_buffer, bytes_written);
                chunk_data[bytes_written] = '\0';
                thread_safe_queue_enqueue(queue, chunk_data, bytes_written,
                                          csv->header, header_len);
                chunks_created++;

                printf(
                    "[CHUNKING] Created final small-device chunk with %d "
                    "devices (%d lines, %zu bytes)\n",
                    devices_in_chunk, current_lines, bytes_written);
            }
        }
    }

    // Cleanup
    free(reusable_buffer);
    free(small_devices);
    free(large_devices);

    // We've used all the device_ids, but we should free the array itself
    for (int i = 0; i < device_count; i++) {
        free(device_ids[i]);
    }
    free(device_ids);

    gettimeofday(&end_time, NULL);
    elapsed_ms = (end_time.tv_sec - start_time.tv_sec) * 1000.0;
    elapsed_ms += (end_time.tv_usec - start_time.tv_usec) / 1000.0;

    printf(
        "[CHUNKING] Created %d total chunks (optimized from %d devices) in "
        "%.2f seconds\n",
        chunks_created, device_count, elapsed_ms / 1000.0);

    return chunks_created;
}

// Gera um caminho UDS único para um dado ID de fatia.
void generate_uds_path(int slice_id, UDSInfo *uds_info) {
    if (!uds_info) return;
    snprintf(uds_info->uds_path, sizeof(uds_info->uds_path),
             "/tmp/uds_slice_%d.sock", slice_id);
    uds_info->socket_fd = -1;
}

// Lança um processo Python para processar um pedaço de dados.
pid_t launch_python_process(const UDSInfo *uds_info, const char *script_path) {
    if (!uds_info || !script_path) return -1;
    pid_t pid;
    char *argv[] = {"python3", (char *)script_path, "--uds-location",
                    (char *)uds_info->uds_path, NULL};
    int status = posix_spawnp(&pid, "python3", NULL, NULL, argv, environ);
    if (status != 0) {
        fprintf(stderr, "posix_spawnp failed: %s\n", strerror(status));
        return -1;
    }
    return pid;
}

// Estabelece uma conexão de servidor UDS.
int establish_uds_server(const UDSInfo *uds_info) {
    if (!uds_info) return -1;
    int fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd < 0) {
        perror("[C] socket");
        return -1;
    }

    struct sockaddr_un addr;
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, uds_info->uds_path, sizeof(addr.sun_path) - 1);

    unlink(uds_info->uds_path);  // Remove se já existir
    if (bind(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("[C] bind");
        close(fd);
        return -1;
    }
    if (listen(fd, 1) < 0) {
        perror("[C] listen");
        close(fd);
        return -1;
    }
    ((UDSInfo *)uds_info)->socket_fd = fd;
    return fd;
}

// Envia um pedaço de CSV por uma conexão UDS.
int send_csv_chunk(const UDSInfo *uds_info, const ThreadSafeQueue *queue) {
    if (!uds_info || !queue) return -1;

    int client_fd = accept(uds_info->socket_fd, NULL, NULL);
    if (client_fd < 0) {
        perror("[C] accept");
        return -1;
    }

    size_t count = thread_safe_queue_get_count((ThreadSafeQueue *)queue);

    for (size_t i = 0; i < count; ++i) {
        const char *slice;
        size_t slice_len;
        const char *header;
        size_t header_len;
        if (thread_safe_queue_dequeue((ThreadSafeQueue *)queue, &slice,
                                      &slice_len, &header, &header_len) != 0) {
            continue;
        }

        // Envia header + '\n' + dados do chunk
        ssize_t sent = send(client_fd, header, header_len, 0);
        if (sent < 0) {
            perror("[SEND_CHUNK] Failed to send header");
            close(client_fd);
            return -1;
        }

        sent = send(client_fd, "\n", 1, 0);
        if (sent < 0) {
            perror("[SEND_CHUNK] Failed to send newline");
            close(client_fd);
            return -1;
        }

        sent = send(client_fd, slice, slice_len, 0);
        if (sent < 0) {
            perror("[C] send slice");
            close(client_fd);
            return -1;
        }

        // NOTE: In this function we don't free the slice because:
        // 1. For the main queue, it's pointing to memory-mapped file data
        // 2. For single_queue in worker_func, the slice is freed after this
        // function returns
    }

    close(client_fd);
    return 0;
}

// Recebe dados processados do CSV por uma conexão UDS.
int receive_processed_csv(const UDSInfo *uds_info, char *buffer,
                          size_t buffer_size) {
    if (!uds_info || !buffer || buffer_size == 0) return -1;
    int client_fd = accept(uds_info->socket_fd, NULL, NULL);
    if (client_fd < 0) return -1;

    ssize_t received = recv(client_fd, buffer, buffer_size - 1, 0);
    if (received < 0) {
        close(client_fd);
        return -1;
    }
    buffer[received] = '\0';
    close(client_fd);
    return (int)received;
}

// Limpa o arquivo UDS após a comunicação.
void cleanup_uds(const UDSInfo *uds_info) {
    if (!uds_info) return;
    if (uds_info->socket_fd >= 0) {
        close(uds_info->socket_fd);
    }
    unlink(uds_info->uds_path);
}
