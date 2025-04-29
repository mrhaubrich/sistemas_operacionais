#include "../include/data_analysis.h"

#include <spawn.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
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
 * Particiona o arquivo CSV por dispositivo.
 * Cada dispositivo se torna um chunk separado na fila.
 */
int partition_csv_by_device(const DeviceMappedCSV *csv,
                            ThreadSafeQueue *queue) {
    if (!csv || !queue || !csv->device_table) return 0;

    printf("[DEBUG] Starting partition_csv_by_device\n");

    // Obter todos os dispositivos únicos
    int device_count = 0;
    char **device_ids =
        device_hash_table_get_all_devices(csv->device_table, &device_count);

    if (!device_ids || device_count == 0) {
        printf("[ERROR] Failed to get device IDs or no devices found\n");
        return 0;
    }

    printf("[DEBUG] Got %d unique devices\n", device_count);
    size_t header_len = strlen(csv->header);
    int chunks_created = 0;
    size_t total_memory_allocated = 0;

    const char *file_start = csv->mapped_data;
    const char *file_end = file_start + csv->size;

    // Processa os dispositivos em lotes para limitar o uso de memória
    const int DEVICES_PER_BATCH = 100;

    for (int batch_start = 0; batch_start < device_count;
         batch_start += DEVICES_PER_BATCH) {
        int batch_end = batch_start + DEVICES_PER_BATCH;
        if (batch_end > device_count) batch_end = device_count;

        printf("[DEBUG] Processing device batch %d to %d (of %d)\n",
               batch_start, batch_end - 1, device_count);

        // Para cada dispositivo no batch atual, criar um chunk com todas as
        // suas linhas
        for (int i = batch_start; i < batch_end; i++) {
            int line_count = 0;
            const char **device_lines = device_hash_table_get_lines(
                csv->device_table, device_ids[i], &line_count);

            if (!device_lines || line_count == 0) {
                printf("[DEBUG] Device %d (%s) has no lines, skipping\n", i,
                       device_ids[i]);
                continue;
            }

            // Se é um dispositivo com várias linhas, processa de forma
            // diferente
            if (line_count > 100) {
                printf(
                    "[DEBUG] Device %d (%s) has %d lines, processing in "
                    "chunks\n",
                    i, device_ids[i], line_count);

                // Processa em batches para evitar alocações muito grandes
                const int LINES_PER_BATCH = 100;
                int batches =
                    (line_count + LINES_PER_BATCH - 1) / LINES_PER_BATCH;

                for (int batch = 0; batch < batches; batch++) {
                    int start_line = batch * LINES_PER_BATCH;
                    int end_line = start_line + LINES_PER_BATCH;
                    if (end_line > line_count) end_line = line_count;

                    size_t batch_size = 0;
                    // Primeiro passo: calcular o tamanho necessário para este
                    // batch
                    for (int j = start_line; j < end_line; j++) {
                        if (device_lines[j] >= file_start &&
                            device_lines[j] < file_end) {
                            const char *line_end =
                                strchr(device_lines[j], '\n');
                            if (!line_end || line_end > file_end)
                                line_end = file_end;
                            batch_size +=
                                (line_end - device_lines[j]) + 1;  // +1 para \n
                        }
                    }

                    if (batch_size == 0) continue;

                    // Aloca memória para o batch
                    char *batch_data = malloc(batch_size + 1);  // +1 para \0
                    if (!batch_data) {
                        fprintf(stderr,
                                "Falha ao alocar memória para batch do "
                                "dispositivo %s\n",
                                device_ids[i]);
                        continue;
                    }

                    // Preenche o batch
                    char *ptr = batch_data;
                    for (int j = start_line; j < end_line; j++) {
                        if (device_lines[j] >= file_start &&
                            device_lines[j] < file_end) {
                            const char *line_end =
                                strchr(device_lines[j], '\n');
                            if (!line_end || line_end > file_end)
                                line_end = file_end;

                            size_t line_len = line_end - device_lines[j];
                            if (line_len > 0) {
                                memcpy(ptr, device_lines[j], line_len);
                                ptr += line_len;

                                // Adiciona quebra de linha se necessário
                                if (j < end_line - 1 ||
                                    (line_len > 0 && *(ptr - 1) != '\n')) {
                                    *ptr++ = '\n';
                                }
                            }
                        }
                    }

                    *ptr = '\0';
                    size_t actual_size = ptr - batch_data;

                    printf(
                        "[DEBUG] Enqueuing device %s batch %d/%d with size %zu "
                        "bytes\n",
                        device_ids[i], batch + 1, batches, actual_size);

                    thread_safe_queue_enqueue(queue, batch_data, actual_size,
                                              csv->header, header_len);
                    chunks_created++;
                    total_memory_allocated += actual_size + 1;
                }

                continue;
            }

            // Calcular o tamanho total das linhas deste dispositivo com
            // segurança
            size_t total_device_data_len = 0;

            // Primeiro passo: calcular o tamanho total de dados necessários
            // sem fazer nenhuma alocação ainda
            for (int j = 0; j < line_count; j++) {
                // Verificar se o ponteiro da linha está na região de memória
                // mapeada
                if (device_lines[j] >= file_start &&
                    device_lines[j] < file_end) {
                    // Encontrar o fim desta linha (newline ou fim do arquivo)
                    const char *line_end = strchr(device_lines[j], '\n');
                    if (!line_end || line_end > file_end) {
                        line_end = file_end;  // Se não encontrar \n, vai até o
                                              // fim do arquivo
                    }

                    // Calcular comprimento seguro da linha
                    size_t line_len = line_end - device_lines[j];
                    total_device_data_len += line_len;

                    // Adicionar espaço para \n se necessário
                    if (line_end < file_end && *line_end != '\n') {
                        total_device_data_len += 1;
                    }
                }
            }

            printf(
                "[DEBUG] Device %d (%s): %d lines, safely calculated %zu "
                "bytes\n",
                i, device_ids[i], line_count, total_device_data_len);

            // Se não há dados válidos para este dispositivo, pule-o
            if (total_device_data_len == 0) {
                printf("[DEBUG] Device %s has no valid data, skipping\n",
                       device_ids[i]);
                continue;
            }

            // Aloca buffer para todas as linhas deste dispositivo
            char *device_data =
                malloc(total_device_data_len + 1);  // +1 para \0
            if (!device_data) {
                fprintf(
                    stderr,
                    "Falha ao alocar memória para dados do dispositivo %s\n",
                    device_ids[i]);
                continue;
            }

            total_memory_allocated += total_device_data_len + 1;
            printf(
                "[DEBUG] Allocated %zu bytes for device %s (total so far: %zu "
                "bytes)\n",
                total_device_data_len + 1, device_ids[i],
                total_memory_allocated);

            // Concatena as linhas deste dispositivo com verificações de
            // segurança
            char *ptr = device_data;
            size_t remaining = total_device_data_len;

            for (int j = 0; j < line_count && remaining > 0; j++) {
                if (device_lines[j] >= file_start &&
                    device_lines[j] < file_end) {
                    const char *line_end = strchr(device_lines[j], '\n');
                    if (!line_end || line_end > file_end) {
                        line_end = file_end;
                    }

                    // Calcular comprimento seguro da linha
                    size_t line_len = line_end - device_lines[j];

                    // Verificar se temos espaço suficiente
                    if (line_len > remaining) {
                        printf(
                            "[WARNING] Not enough space for line %d of device "
                            "%s. "
                            "Truncating data.\n",
                            j, device_ids[i]);
                        line_len = remaining;
                    }

                    // Copiar a linha
                    if (line_len > 0) {
                        memcpy(ptr, device_lines[j], line_len);
                        ptr += line_len;
                        remaining -= line_len;
                    }

                    // Adicionar quebra de linha se necessário
                    if ((j < line_count - 1) && remaining > 0) {
                        *ptr++ = '\n';
                        remaining--;
                    }
                }
            }

            *ptr = '\0';  // Terminador nulo sempre é colocado

            // Enfileira as linhas deste dispositivo como um único chunk
            size_t actual_size = ptr - device_data;
            printf("[DEBUG] Enqueuing device %s with actual size %zu bytes\n",
                   device_ids[i], actual_size);

            thread_safe_queue_enqueue(queue, device_data, actual_size,
                                      csv->header, header_len);
            chunks_created++;

            if (chunks_created % 1000 == 0) {
                printf("[DEBUG] Enqueued %d device chunks\n", chunks_created);
            }
        }

        // Log após cada batch para monitorar o progresso
        printf(
            "[DEBUG] Completed batch %d to %d: %d chunks created so far, %zu "
            "bytes allocated\n",
            batch_start, batch_end - 1, chunks_created, total_memory_allocated);
    }

    printf("[DEBUG] Created %d total chunks, allocated %zu bytes in total\n",
           chunks_created, total_memory_allocated);

    // Libera a lista de IDs de dispositivos
    printf("[DEBUG] Cleaning up device IDs\n");
    for (int i = 0; i < device_count; i++) {
        free(device_ids[i]);
    }
    free(device_ids);

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

    printf("[SEND_CHUNK] Waiting for client connection on %s\n",
           uds_info->uds_path);
    int client_fd = accept(uds_info->socket_fd, NULL, NULL);
    if (client_fd < 0) {
        perror("[C] accept");
        return -1;
    }
    printf("[SEND_CHUNK] Client connected, preparing to send data\n");

    size_t count = thread_safe_queue_get_count((ThreadSafeQueue *)queue);
    printf("[SEND_CHUNK] Queue has %zu items to send\n", count);

    for (size_t i = 0; i < count; ++i) {
        const char *slice;
        size_t slice_len;
        const char *header;
        size_t header_len;
        if (thread_safe_queue_dequeue((ThreadSafeQueue *)queue, &slice,
                                      &slice_len, &header, &header_len) != 0) {
            printf("[SEND_CHUNK] Failed to dequeue item %zu\n", i);
            continue;
        }

        printf(
            "[SEND_CHUNK] Dequeued item %zu: header_len=%zu, slice_len=%zu, "
            "slice=%p\n",
            i, header_len, slice_len, slice);

        // Envia header + '\n' + dados do chunk
        ssize_t sent = send(client_fd, header, header_len, 0);
        if (sent < 0) {
            perror("[SEND_CHUNK] Failed to send header");
            close(client_fd);
            return -1;
        }
        printf("[SEND_CHUNK] Sent header: %zd bytes\n", sent);

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
        printf("[SEND_CHUNK] Sent slice: %zd bytes\n", sent);

        // NOTE: In this function we don't free the slice because:
        // 1. For the main queue, it's pointing to memory-mapped file data
        // 2. For single_queue in worker_func, the slice is freed after this
        // function returns
    }

    printf("[SEND_CHUNK] All chunks sent, closing connection\n");
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
