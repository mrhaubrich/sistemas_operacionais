#include "../include/data_analysis.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

// Particiona o arquivo CSV em pedaços menores e enfileira-os na fila.
int partition_csv(const MappedCSV *csv, size_t chunk_size,
                  ThreadSafeQueue *queue, size_t max_chunks) {
    if (!csv || !queue || chunk_size == 0) return 0;

    size_t chunk_count = 0;
    size_t line = 0;
    size_t header_len = strlen(csv->header);

    while (line < (size_t)csv->data_count && chunk_count < max_chunks) {
        size_t start = line;
        size_t end = (line + chunk_size < (size_t)csv->data_count)
                         ? (line + chunk_size)
                         : (size_t)csv->data_count;
        // Calcula o tamanho do chunk de dados (sem header)
        const char *start_ptr = csv->line_indices[start];
        const char *end_ptr =
            (end < (size_t)csv->data_count)
                ? csv->line_indices[end]
                : (csv->line_indices[csv->data_count - 1] +
                   strlen(csv->line_indices[csv->data_count - 1]));
        size_t data_len = end_ptr - start_ptr;

        // Aloca espaço para header + '\n' + dados + '\0'
        size_t slice_len = header_len + 1 + data_len;
        char *slice = malloc(slice_len + 1);
        if (!slice) break;

        // Copia header, '\n', depois os dados do chunk
        memcpy(slice, csv->header, header_len);
        slice[header_len] = '\n';
        memcpy(slice + header_len + 1, start_ptr, data_len);
        slice[slice_len] = '\0';

        thread_safe_queue_enqueue(queue, slice);
        chunk_count++;
        line = end;
    }
    return chunk_count;
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
    pid_t pid = fork();
    if (pid == 0) {
        execlp("python3", "python3", script_path, "--uds-location",
               uds_info->uds_path, (char *)NULL);
        perror("exec python3 failed");
        _exit(1);
    }
    return pid;
}

// Estabelece uma conexão de servidor UDS.
int establish_uds_server(const UDSInfo *uds_info) {
    if (!uds_info) return -1;
    int fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd < 0) return -1;

    struct sockaddr_un addr;
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, uds_info->uds_path, sizeof(addr.sun_path) - 1);

    unlink(uds_info->uds_path);  // Remove se já existir
    if (bind(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        close(fd);
        return -1;
    }
    if (listen(fd, 1) < 0) {
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
    if (client_fd < 0) return -1;

    size_t count = thread_safe_queue_get_count((ThreadSafeQueue *)queue);
    for (size_t i = 0; i < count; ++i) {
        const char *slice = thread_safe_queue_dequeue((ThreadSafeQueue *)queue);
        if (!slice) continue;
        size_t len = strlen(slice);
        ssize_t sent = send(client_fd, slice, len, 0);
        if (sent < 0) {
            close(client_fd);
            return -1;
        }
        free((void *)slice);
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
