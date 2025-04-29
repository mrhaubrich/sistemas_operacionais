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
    size_t line = 0;
    size_t header_len = strlen(csv->header);

    while (line < (size_t)csv->data_count) {
        size_t start = line;
        size_t end = (line + chunk_size < (size_t)csv->data_count)
                         ? (line + chunk_size)
                         : (size_t)csv->data_count;
        const char *start_ptr = csv->line_indices[start];
        const char *end_ptr =
            (end < (size_t)csv->data_count)
                ? csv->line_indices[end]
                : (csv->line_indices[csv->data_count - 1] +
                   strlen(csv->line_indices[csv->data_count - 1]));
        size_t data_len = end_ptr - start_ptr;

        // Enfileira o ponteiro, tamanho do chunk, header e tamanho do header
        thread_safe_queue_enqueue(queue, start_ptr, data_len, csv->header,
                                  header_len);
        chunk_count++;
        line = end;
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

    // Obter todos os dispositivos únicos
    int device_count = 0;
    char **device_ids =
        device_hash_table_get_all_devices(csv->device_table, &device_count);

    if (!device_ids || device_count == 0) {
        return 0;
    }

    size_t header_len = strlen(csv->header);
    int chunks_created = 0;

    // Para cada dispositivo, criar um chunk com todas as suas linhas
    for (int i = 0; i < device_count; i++) {
        int line_count = 0;
        const char **device_lines = device_hash_table_get_lines(
            csv->device_table, device_ids[i], &line_count);

        if (!device_lines || line_count == 0) {
            continue;
        }

        // Calcular o tamanho total das linhas deste dispositivo
        size_t total_device_data_len = 0;
        for (int j = 0; j < line_count; j++) {
            total_device_data_len += strlen(device_lines[j]);
            // Adiciona espaço para caracteres de nova linha
            if (j < line_count - 1 || strchr(device_lines[j], '\n') == NULL) {
                total_device_data_len += 1;
            }
        }

        // Alocar buffer para todas as linhas deste dispositivo
        char *device_data = malloc(total_device_data_len + 1);
        if (!device_data) {
            fprintf(stderr,
                    "Falha ao alocar memória para dados do dispositivo %s\n",
                    device_ids[i]);
            continue;
        }

        // Concatenar todas as linhas deste dispositivo
        char *ptr = device_data;
        for (int j = 0; j < line_count; j++) {
            size_t line_len = strlen(device_lines[j]);
            memcpy(ptr, device_lines[j], line_len);
            ptr += line_len;

            // Adiciona quebra de linha se não existir
            if (j < line_count - 1 ||
                (*(ptr - 1) != '\n' && *(ptr - 1) != '\r')) {
                *ptr++ = '\n';
            }
        }
        *ptr = '\0';

        // Enfileira as linhas deste dispositivo como um único chunk
        thread_safe_queue_enqueue(queue, device_data, ptr - device_data,
                                  csv->header, header_len);
        chunks_created++;

        // device_data será liberado pelo receptor após o uso
    }

    // Libera a lista de IDs de dispositivos
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
                                      &slice_len, &header, &header_len) != 0)
            continue;

        // Envia header + '\n' + dados do chunk
        ssize_t sent = send(client_fd, header, header_len, 0);
        if (sent < 0) {
            close(client_fd);
            return -1;
        }
        sent = send(client_fd, "\n", 1, 0);
        if (sent < 0) {
            close(client_fd);
            return -1;
        }
        sent = send(client_fd, slice, slice_len, 0);
        if (sent < 0) {
            perror("[C] send slice");
            close(client_fd);
            return -1;
        }
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
