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

#include "../include/thread_safe_queue.h"

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

// Estrutura para armazenar informações do dispositivo
typedef struct {
    char *device_id;
    int line_count;
} DeviceInfo;

// Ordenar dispositivos por contagem de linhas (decrescente) usando qsort
int compare_device_info(const void *a, const void *b) {
    const DeviceInfo *device_a = (const DeviceInfo *)a;
    const DeviceInfo *device_b = (const DeviceInfo *)b;
    return device_b->line_count - device_a->line_count;
}

/**
 * Particiona o arquivo CSV por dispositivo, otimizando para o número de
 * threads. Os dispositivos são distribuídos entre as threads para balancear o
 * trabalho, mantendo os dados de um mesmo dispositivo juntos.
 */
int partition_csv_by_device_threaded(const DeviceMappedCSV *csv,
                                     ThreadSafeQueue *queue, int num_threads) {
    if (!csv || !queue || !csv->device_table || num_threads <= 0) return 0;

    struct timeval start_time, end_time;
    double elapsed_ms;
    gettimeofday(&start_time, NULL);

    printf("[CHUNKING] Starting device distribution across %d threads\n",
           num_threads);

    // Obter todos os dispositivos únicos
    int device_count = 0;
    char **device_ids =
        device_hash_table_get_all_devices(csv->device_table, &device_count);

    if (!device_ids || device_count == 0) return 0;

    // Coletar linha por dispositivo para ordenação
    DeviceInfo *devices = malloc(device_count * sizeof(DeviceInfo));
    if (!devices) {
        fprintf(stderr, "Failed to allocate memory for device sorting\n");
        // Cleanup
        for (int i = 0; i < device_count; i++) {
            free(device_ids[i]);
        }
        free(device_ids);
        return 0;
    }

    // Preencher array de dispositivos com seus IDs e contagens de linhas
    for (int i = 0; i < device_count; i++) {
        devices[i].device_id = device_ids[i];
        int line_count = 0;
        device_hash_table_get_lines(csv->device_table, device_ids[i],
                                    &line_count);
        devices[i].line_count = line_count;
    }

    qsort(devices, device_count, sizeof(DeviceInfo), compare_device_info);

    // Estrutura para representar a alocação de dispositivos por thread
    typedef struct {
        int total_lines;   // Total de linhas atribuídas a esta thread
        int device_count;  // Número de dispositivos atribuídos
        DeviceInfo *assigned_devices;  // Dispositivos atribuídos
        int capacity;                  // Capacidade do array assigned_devices
    } ThreadAllocation;

    // Criar alocações de thread
    ThreadAllocation *thread_allocations =
        calloc(num_threads, sizeof(ThreadAllocation));
    if (!thread_allocations) {
        fprintf(stderr, "Failed to allocate thread allocations\n");
        free(devices);
        for (int i = 0; i < device_count; i++) {
            free(device_ids[i]);
        }
        free(device_ids);
        return 0;
    }

    // Inicializar capacidade inicial para cada alocação de thread
    for (int i = 0; i < num_threads; i++) {
        thread_allocations[i].capacity = 10;  // Capacidade inicial
        thread_allocations[i].assigned_devices =
            malloc(thread_allocations[i].capacity * sizeof(DeviceInfo));
        if (!thread_allocations[i].assigned_devices) {
            fprintf(stderr, "Failed to allocate devices for thread %d\n", i);
            // Cleanup alocações anteriores
            for (int j = 0; j < i; j++) {
                free(thread_allocations[j].assigned_devices);
            }
            free(thread_allocations);
            free(devices);
            for (int j = 0; j < device_count; j++) {
                free(device_ids[j]);
            }
            free(device_ids);
            return 0;
        }
    }

    // Distribuir dispositivos entre threads (algoritmo de empacotamento guloso)
    // Atribuir cada dispositivo à thread com menos linhas atuais
    for (int i = 0; i < device_count; i++) {
        // Encontrar a thread com a menor quantidade de linhas
        int min_lines_thread = 0;
        for (int t = 1; t < num_threads; t++) {
            if (thread_allocations[t].total_lines <
                thread_allocations[min_lines_thread].total_lines) {
                min_lines_thread = t;
            }
        }

        // Verificar se precisamos aumentar a capacidade
        ThreadAllocation *alloc = &thread_allocations[min_lines_thread];
        if (alloc->device_count >= alloc->capacity) {
            int new_capacity = alloc->capacity * 2;
            DeviceInfo *new_devices = realloc(
                alloc->assigned_devices, new_capacity * sizeof(DeviceInfo));
            if (!new_devices) {
                fprintf(stderr, "Failed to resize devices for thread %d\n",
                        min_lines_thread);
                // Continue com a capacidade atual
            } else {
                alloc->assigned_devices = new_devices;
                alloc->capacity = new_capacity;
            }
        }

        // Adicionar dispositivo à thread com menos linhas
        if (alloc->device_count < alloc->capacity) {
            alloc->assigned_devices[alloc->device_count] = devices[i];
            alloc->total_lines += devices[i].line_count;
            alloc->device_count++;
        }
    }

    // Imprimir informações de distribuição
    printf("[CHUNKING] Device distribution across threads:\n");
    for (int t = 0; t < num_threads; t++) {
        printf("[CHUNKING] Thread %d: %d devices, %d lines\n", t,
               thread_allocations[t].device_count,
               thread_allocations[t].total_lines);
    }

    // Agora criar chunks por thread combinando os dados dos dispositivos
    // atribuídos
    size_t header_len = strlen(csv->header);
    int chunks_created = 0;
    const char *file_start = csv->mapped_data;
    const char *file_end = file_start + csv->size;

    // Buffer para combinar dados de dispositivos por thread
    const size_t BUFFER_SIZE = 1024 * 1024;  // 1MB buffer
    char *reusable_buffer = malloc(BUFFER_SIZE);
    if (!reusable_buffer) {
        fprintf(stderr, "Failed to allocate reusable buffer\n");
        // Cleanup
        for (int t = 0; t < num_threads; t++) {
            free(thread_allocations[t].assigned_devices);
        }
        free(thread_allocations);
        free(devices);
        for (int i = 0; i < device_count; i++) {
            free(device_ids[i]);  // device_ids já não é mais necessário pois
                                  // fizemos a cópia
        }
        free(device_ids);
        return 0;
    }

    // Processar cada thread e criar um único chunk por thread
    for (int t = 0; t < num_threads; t++) {
        ThreadAllocation *alloc = &thread_allocations[t];

        // Se não houver dispositivos atribuídos a esta thread, ainda criamos um
        // chunk vazio para manter a contagem correta de chunks

        // Variáveis para construção do buffer para esta thread
        char *buffer_ptr = reusable_buffer;
        size_t buffer_used = 0;
        size_t total_chunk_size = 0;
        int total_lines = 0;

        // Usaremos uma lista de buffers para quando o tamanho exceder o buffer
        // inicial
        char **buffer_list = NULL;
        size_t *buffer_sizes = NULL;
        int buffer_count = 0;
        int buffer_capacity = 0;

        // Para cada dispositivo atribuído, adicionar suas linhas ao buffer
        for (int d = 0; d < alloc->device_count; d++) {
            DeviceInfo *device = &alloc->assigned_devices[d];

            // Obter linhas para este dispositivo
            int line_count = 0;
            const char **device_lines = device_hash_table_get_lines(
                csv->device_table, device->device_id, &line_count);

            if (!device_lines || line_count == 0) continue;

            // Adicionar linhas deste dispositivo ao buffer
            for (int j = 0; j < line_count; j++) {
                if (device_lines[j] >= file_start &&
                    device_lines[j] < file_end) {
                    const char *line_end = memchr(device_lines[j], '\n',
                                                  file_end - device_lines[j]);
                    if (!line_end) line_end = file_end;

                    size_t line_len = line_end - device_lines[j];

                    // Verificar se precisamos usar um novo buffer (se estiver
                    // cheio)
                    if (buffer_used + line_len + 2 >= BUFFER_SIZE) {
                        // Salvar o buffer atual na lista de buffers
                        if (buffer_count >= buffer_capacity) {
                            // Precisamos expandir os arrays de buffers
                            int new_capacity =
                                buffer_capacity == 0 ? 4 : buffer_capacity * 2;
                            char **new_buffer_list = realloc(
                                buffer_list, new_capacity * sizeof(char *));
                            size_t *new_buffer_sizes = realloc(
                                buffer_sizes, new_capacity * sizeof(size_t));

                            if (!new_buffer_list || !new_buffer_sizes) {
                                fprintf(stderr,
                                        "Failed to expand buffer lists\n");
                                // Continuar com a capacidade atual
                                if (new_buffer_list)
                                    buffer_list = new_buffer_list;
                                if (new_buffer_sizes)
                                    buffer_sizes = new_buffer_sizes;
                            } else {
                                buffer_list = new_buffer_list;
                                buffer_sizes = new_buffer_sizes;
                                buffer_capacity = new_capacity;
                            }
                        }

                        if (buffer_count < buffer_capacity) {
                            // Criar uma cópia do buffer atual
                            char *chunk_buffer = malloc(buffer_used);
                            if (chunk_buffer) {
                                memcpy(chunk_buffer, reusable_buffer,
                                       buffer_used);
                                buffer_list[buffer_count] = chunk_buffer;
                                buffer_sizes[buffer_count] = buffer_used;
                                buffer_count++;
                                total_chunk_size += buffer_used;
                            }
                        }

                        // Resetar buffer
                        buffer_ptr = reusable_buffer;
                        buffer_used = 0;
                    }

                    // Copiar linha para o buffer
                    memcpy(buffer_ptr, device_lines[j], line_len);
                    buffer_ptr += line_len;
                    buffer_used += line_len;
                    total_lines++;

                    // Adicionar nova linha se necessário
                    if (*(buffer_ptr - 1) != '\n') {
                        *buffer_ptr++ = '\n';
                        buffer_used++;
                    }
                }
            }
        }

        // Adicionar o último buffer usado à lista se tiver conteúdo
        if (buffer_used > 0 && buffer_count < buffer_capacity) {
            char *chunk_buffer = malloc(buffer_used);
            if (chunk_buffer) {
                memcpy(chunk_buffer, reusable_buffer, buffer_used);
                buffer_list[buffer_count] = chunk_buffer;
                buffer_sizes[buffer_count] = buffer_used;
                buffer_count++;
                total_chunk_size += buffer_used;
            }
        }

        // Agora combinamos todos os buffers em um único chunk final
        char *final_chunk = NULL;
        if (total_chunk_size > 0) {
            final_chunk = malloc(total_chunk_size);
            if (final_chunk) {
                char *dest_ptr = final_chunk;
                for (int b = 0; b < buffer_count; b++) {
                    memcpy(dest_ptr, buffer_list[b], buffer_sizes[b]);
                    dest_ptr += buffer_sizes[b];
                    free(buffer_list[b]);  // Liberar cada buffer da lista
                }
            }
        } else if (alloc->device_count == 0) {
            // Thread sem dispositivos, criar um chunk vazio
            final_chunk = malloc(1);
            if (final_chunk) {
                final_chunk[0] = '\0';
                total_chunk_size = 0;
            }
        }

        // Liberar as listas de buffers
        free(buffer_list);
        free(buffer_sizes);

        // Adicionar o chunk final à fila
        if (final_chunk) {
            thread_safe_queue_enqueue(queue, final_chunk, total_chunk_size,
                                      csv->header, header_len);
            chunks_created++;
            printf(
                "[CHUNKING] Created chunk for thread %d: %d devices, %d lines, "
                "%zu bytes\n",
                t, alloc->device_count, total_lines, total_chunk_size);
        } else if (alloc->device_count > 0) {
            // Se não conseguimos criar o chunk para uma thread com
            // dispositivos, isso é um erro
            fprintf(stderr, "Failed to create chunk for thread %d\n", t);
        } else {
            // Thread sem dispositivos, criar um chunk vazio
            char *empty_chunk = malloc(1);
            if (empty_chunk) {
                empty_chunk[0] = '\0';
                thread_safe_queue_enqueue(queue, empty_chunk, 0, csv->header,
                                          header_len);
                chunks_created++;
                printf(
                    "[CHUNKING] Created empty chunk for thread %d (no devices "
                    "assigned)\n",
                    t);
            }
        }
    }

    // Limpar recursos
    free(reusable_buffer);

    for (int t = 0; t < num_threads; t++) {
        free(thread_allocations[t].assigned_devices);
    }
    free(thread_allocations);

    free(devices);  // Não liberamos o conteúdo, só o array

    // Os device_ids originais precisam ser liberados
    for (int i = 0; i < device_count; i++) {
        free(device_ids[i]);
    }
    free(device_ids);

    gettimeofday(&end_time, NULL);
    elapsed_ms = (end_time.tv_sec - start_time.tv_sec) * 1000.0;
    elapsed_ms += (end_time.tv_usec - start_time.tv_usec) / 1000.0;

    printf("[CHUNKING] Created %d chunks across %d threads in %.2f seconds\n",
           chunks_created, num_threads, elapsed_ms / 1000.0);

    // Devemos ter exatamente um chunk por thread
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
