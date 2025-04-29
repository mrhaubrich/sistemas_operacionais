#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <sys/wait.h>
#include <time.h>

#include "../include/data_analysis.h"
#include "../include/file_mapping.h"
#include "../include/hash_table.h"
#include "../include/thread_safe_queue.h"
#include "../include/utils.h"

// Define a timer structure and functions
typedef struct {
    struct timeval start;
    struct timeval end;
    double elapsed_ms;
    const char *name;
} Timer;

// Start a timer
void start_timer(Timer *timer, const char *name) {
    timer->name = name;
    gettimeofday(&timer->start, NULL);
}

// Stop a timer and print the elapsed time
void stop_timer(Timer *timer) {
    gettimeofday(&timer->end, NULL);
    timer->elapsed_ms = (timer->end.tv_sec - timer->start.tv_sec) * 1000.0;
    timer->elapsed_ms += (timer->end.tv_usec - timer->start.tv_usec) / 1000.0;
    printf("[TIMING] %s took %.2f ms (%.2f seconds)\n", timer->name,
           timer->elapsed_ms, timer->elapsed_ms / 1000.0);
}

typedef struct {
    int thread_id;
    ThreadSafeQueue *queue;
    const char *script_path;
    int *line_counts;
    char **results;        // buffer pointers for received CSVs
    size_t *result_sizes;  // sizes of received CSVs
} WorkerArgs;

void *worker_func(void *arg) {
    WorkerArgs *wargs = (WorkerArgs *)arg;
    int id = wargs->thread_id;
    ThreadSafeQueue *queue = wargs->queue;
    const char *script_path = wargs->script_path;

// Only log worker start at debug level
#ifdef DEBUG
    printf("[WORKER-%d] Worker thread started\n", id);
#endif

    int total_lines = 0;
    char *final_result = NULL;
    size_t final_result_size = 0;
    int chunks_processed = 0;
    size_t total_memory_freed = 0;

    Timer chunk_timer;
    double total_processing_time = 0.0;

    while (1) {
        // Try to dequeue a chunk from the queue
        const char *slice = NULL;
        size_t slice_len = 0;
        const char *header = NULL;
        size_t header_len = 0;
        int dq = thread_safe_queue_dequeue(queue, &slice, &slice_len, &header,
                                           &header_len);
        if (dq != 0) {
// Queue is empty
#ifdef DEBUG
            printf("[WORKER-%d] Queue is empty, exiting worker loop\n", id);
#endif
            break;
        }

#ifdef DEBUG
        printf("[WORKER-%d] Dequeued chunk #%d: slice_len=%zu\n", id,
               chunks_processed + 1, slice_len);
#endif

        start_timer(&chunk_timer, "Python chunk processing");

        UDSInfo uds_info;
        generate_uds_path(id, &uds_info);
        int server_fd = establish_uds_server(&uds_info);
        if (server_fd < 0) {
#ifdef DEBUG
            printf(
                "[WORKER-%d] Failed to establish UDS server, freeing slice "
                "memory %p\n",
                id, slice);
#endif
            free((void *)slice);
            total_memory_freed += slice_len;
            continue;
        }
        pid_t pid = launch_python_process(&uds_info, script_path);
        if (pid < 0) {
#ifdef DEBUG
            printf(
                "[WORKER-%d] Failed to launch Python process, freeing slice "
                "memory %p\n",
                id, slice);
#endif
            free((void *)slice);
            total_memory_freed += slice_len;
            cleanup_uds(&uds_info);
            continue;
        }

#ifdef DEBUG
        printf("[WORKER-%d] Processing chunk #%d with Python process %d\n", id,
               chunks_processed + 1, pid);
#endif

        // Send the chunk to the Python process
        // We need to create a temporary queue for this chunk
        ThreadSafeQueue *single_queue = thread_safe_queue_create();
        thread_safe_queue_enqueue(single_queue, slice, slice_len, header,
                                  header_len);
        send_csv_chunk(&uds_info, single_queue);
        thread_safe_queue_destroy(single_queue);

        // Receive processed result and count lines
        size_t buffer_size = 1024 * 1024;
        char *buffer = malloc(buffer_size);
        int received =
            buffer ? receive_processed_csv(&uds_info, buffer, buffer_size) : -1;
        if (received > 0) {
            int lines = 0;
            for (int j = 0; j < received; ++j)
                if (buffer[j] == '\n') lines++;
            total_lines += lines;

#ifdef DEBUG
            printf("[WORKER-%d] Received %d bytes, %d lines from Python\n", id,
                   received, lines);
#endif

            // Concatenate results if needed
            char *new_result =
                realloc(final_result, final_result_size + received);
            if (new_result) {
                memcpy(new_result + final_result_size, buffer, received);
                final_result = new_result;
                final_result_size += received;
            }
        }
#ifdef DEBUG
        else {
            printf("[WORKER-%d] No data received from Python process\n", id);
        }
#endif

        if (buffer) free(buffer);

// Free the device data chunk after processing
#ifdef DEBUG
        printf("[WORKER-%d] Freeing slice memory %p of size %zu\n", id, slice,
               slice_len);
#endif

        free((void *)slice);
        total_memory_freed += slice_len;
        chunks_processed++;

        if (pid > 0) waitpid(pid, NULL, 0);
        cleanup_uds(&uds_info);

        stop_timer(&chunk_timer);
        total_processing_time += chunk_timer.elapsed_ms;
    }

#ifdef DEBUG
    printf(
        "[WORKER-%d] Worker finished. Processed %d chunks, freed %zu bytes, "
        "collected %d lines. Total processing time: %.2f ms\n",
        id, chunks_processed, total_memory_freed, total_lines,
        total_processing_time);
#endif

    wargs->line_counts[id] = total_lines;
    wargs->results[id] = final_result;
    wargs->result_sizes[id] = final_result_size;

    pthread_exit(NULL);
}

/**
 * Encontra o índice da coluna de dispositivo no cabeçalho CSV
 * @param header O cabeçalho CSV
 * @param device_column_name O nome da coluna a ser buscada
 * @return O índice da coluna ou -1 se não for encontrada
 */
int find_device_column(const char *header, const char *device_column_name) {
    if (!header || !device_column_name) return -1;

    // Por padrão, usamos '|' como delimitador CSV
    int column_index = 0;

    // Tokenize o cabeçalho
    char *header_copy = strdup(header);
    if (!header_copy) return -1;

    char *token = strtok(header_copy, "|");
    while (token) {
        // Remove espaços extras
        while (*token == ' ' && *token) token++;
        char *end = token + strlen(token) - 1;
        while (end > token && *end == ' ') *end-- = '\0';

        // Compara o nome da coluna
        if (strcmp(token, device_column_name) == 0) {
            free(header_copy);
            return column_index;
        }

        token = strtok(NULL, "|");
        column_index++;
    }

    free(header_copy);
    return -1;  // Coluna não encontrada
}

/**
 * Ponto de entrada principal para o programa de processamento de arquivos CSV
 */
int main(int argc, char *argv[]) {
    Timer total_timer, mapping_timer, hash_building_timer, partitioning_timer,
        processing_timer;

    start_timer(&total_timer, "Total program execution");

    printf(
        "Processador de Arquivos CSV - Usando pthreads para processamento "
        "paralelo\n");
    printf("----------------------------------------------------------\n");

    // Validar argumentos da linha de comando
    if (argc < 2) {
        fprintf(stderr,
                "Uso: %s <caminho_para_arquivo_csv> [coluna_dispositivo]\n",
                argv[0]);
        fprintf(stderr,
                "     coluna_dispositivo: Nome da coluna que contém o ID do "
                "dispositivo (padrão: 'device')\n");
        return EXIT_FAILURE;
    }

    const char *filepath = argv[1];
    const char *device_column_name = (argc > 2) ? argv[2] : "device";

    // Validar extensão do arquivo
    if (!validate_csv_extension(filepath)) {
        return EXIT_FAILURE;
    }

    // Imprimir informações do sistema
    int num_processors = get_available_number_of_processors();
    printf("[MAIN] Processadores disponíveis: %d\n", num_processors);
    printf("[MAIN] Processando arquivo: %s\n", filepath);
    printf("[MAIN] Coluna de dispositivo: %s\n\n", device_column_name);

    // Primeira etapa: mapear o arquivo para obter o cabeçalho e encontrar o
    // índice da coluna de dispositivo
    start_timer(&mapping_timer, "Initial CSV mapping");
    MappedCSV temp_csv = map_csv(filepath);
    if (temp_csv.header == NULL) {
        fprintf(stderr, "[MAIN] Falha ao mapear o arquivo\n");
        return EXIT_FAILURE;
    }
    stop_timer(&mapping_timer);

    // Encontrar o índice da coluna de dispositivo
    int device_column = find_device_column(temp_csv.header, device_column_name);
    if (device_column < 0) {
        fprintf(stderr,
                "[MAIN] Erro: Coluna '%s' não encontrada no cabeçalho\n",
                device_column_name);
        unmap_csv(&temp_csv);
        return EXIT_FAILURE;
    }

    printf("[MAIN] Índice da coluna de dispositivo '%s': %d\n",
           device_column_name, device_column);

    // Liberar o CSV temporário
    printf("[MAIN] Unmapping temporary CSV\n");
    unmap_csv(&temp_csv);

    // Mapear novamente usando a abordagem baseada em dispositivo
    printf("[MAIN] Mapping CSV with device-based approach\n");
    start_timer(&hash_building_timer, "Device hash table building");
    DeviceMappedCSV deviceMappedCsv = map_device_csv(filepath, device_column);
    if (deviceMappedCsv.header == NULL || !deviceMappedCsv.device_table) {
        fprintf(stderr,
                "[MAIN] Falha ao mapear o arquivo utilizando tabela hash de "
                "dispositivos\n");
        return EXIT_FAILURE;
    }
    stop_timer(&hash_building_timer);

    // Imprimir informações do arquivo
    printf("\n[MAIN] Informações do CSV:\n");
    printf("[MAIN] - Linhas: %d\n", deviceMappedCsv.data_count);
    printf("[MAIN] - Cabeçalho: %s\n", deviceMappedCsv.header);
    printf("[MAIN] - Dispositivos únicos: %d\n",
           deviceMappedCsv.device_table->device_count);

    // --- NOVA LÓGICA USANDO PTHREADS PARA PROCESSAMENTO PARALELO COM PYTHON
    // POR DISPOSITIVO ---

    printf("[MAIN] Creating thread-safe queue\n");
    ThreadSafeQueue *queue = thread_safe_queue_create();
    if (!queue) {
        fprintf(stderr, "[MAIN] Falha ao criar fila de chunks\n");
        unmap_device_csv(&deviceMappedCsv);
        return EXIT_FAILURE;
    }

    printf("[MAIN] Processadores disponíveis: %d\n", num_processors);

    // Particionar dispositivos distribuindo-os entre as threads
    printf("[MAIN] Partitioning CSV by device across threads\n");
    start_timer(&partitioning_timer,
                "CSV partitioning by device across threads");

    // Use the new function that distributes devices across threads
    size_t num_chunks = partition_csv_by_device_threaded(&deviceMappedCsv,
                                                         queue, num_processors);

    stop_timer(&partitioning_timer);
    printf("[MAIN] Número de chunks particionados: %zu\n", num_chunks);

    if (num_chunks == 0) {
        fprintf(stderr, "[MAIN] Falha ao particionar o CSV por dispositivos\n");
        thread_safe_queue_destroy(queue);
        unmap_device_csv(&deviceMappedCsv);
        return EXIT_FAILURE;
    }

    // Caminho para o script Python
    const char *script_path = "./src/script/analyze_data.py";

    // O número de threads para processar será igual ao número de chunks criados
    size_t n_threads = num_chunks;

    // Cria threads e argumentos
    printf("[MAIN] Allocating memory for %zu threads\n", n_threads);
    pthread_t *threads = malloc(sizeof(pthread_t) * n_threads);
    WorkerArgs *args = malloc(sizeof(WorkerArgs) * n_threads);
    int *line_counts = calloc(n_threads, sizeof(int));
    char **results = calloc(n_threads, sizeof(char *));
    size_t *result_sizes = calloc(n_threads, sizeof(size_t));

    if (!threads || !args || !line_counts || !results || !result_sizes) {
        fprintf(stderr, "[MAIN] Falha ao alocar recursos de threads\n");
        if (threads) free(threads);
        if (args) free(args);
        if (line_counts) free(line_counts);
        if (results) free(results);
        if (result_sizes) free(result_sizes);
        thread_safe_queue_destroy(queue);
        unmap_device_csv(&deviceMappedCsv);
        return EXIT_FAILURE;
    }

    printf("[MAIN] Iniciando %zu threads para processar %zu dispositivos...\n",
           n_threads, num_chunks);

    start_timer(&processing_timer, "Parallel data processing");

    for (size_t i = 0; i < n_threads; ++i) {
        args[i].thread_id = (int)i;
        args[i].queue = queue;
        args[i].script_path = script_path;
        args[i].line_counts = line_counts;
        args[i].results = results;
        args[i].result_sizes = result_sizes;
        pthread_create(&threads[i], NULL, worker_func, &args[i]);
    }

    // Aguarda todas as threads terminarem
    printf("[MAIN] Waiting for all worker threads to finish...\n");
    for (size_t i = 0; i < n_threads; ++i) {
        pthread_join(threads[i], NULL);
        printf("[MAIN] Thread %zu joined\n", i);
    }

    stop_timer(&processing_timer);

    // Soma total de linhas retornadas
    int total_lines = 0;
    for (size_t i = 0; i < n_threads; ++i) {
        total_lines += line_counts[i];
    }

    // Limpeza antecipada dos arrays de contagem e argumentos
    printf("[MAIN] Cleaning up thread resources\n");
    free(threads);
    free(args);
    free(line_counts);

    printf("[MAIN] Destroying thread-safe queue\n");
    thread_safe_queue_destroy(queue);

    printf("[MAIN] Unmapping device CSV\n");
    unmap_device_csv(&deviceMappedCsv);

    // Print first 10 lines of all returned CSVs
    printf(
        "\n[MAIN] Primeiras 10 linhas retornadas por todos os CSVs "
        "processados:\n");
    for (size_t i = 0; i < n_threads; ++i) {
        printf("[MAIN] Thread %zu: %d linhas\n", i, line_counts[i]);
        if (results[i] && result_sizes[i] > 0) {
            int line = 0;
            size_t pos = 0;
            while (pos < result_sizes[i] && line < 10) {
                size_t start = pos;
                while (pos < result_sizes[i] && results[i][pos] != '\n') pos++;
                size_t len = pos - start;
                if (len > 0 ||
                    (pos < result_sizes[i] && results[i][pos] == '\n')) {
                    printf("%.*s\n", (int)len, results[i] + start);
                    line++;
                }
                if (pos < result_sizes[i] && results[i][pos] == '\n') pos++;
            }
            if (line == 0) printf("<Nenhuma linha retornada>\n");
        } else {
            printf("<Nenhum resultado>\n");
        }
        printf("\n");
    }

    printf(
        "\n[MAIN] Total de linhas retornadas por todos os CSVs processados: "
        "%d\n",
        total_lines);

    // Limpeza final dos resultados
    printf("[MAIN] Final cleanup of results\n");
    for (size_t i = 0; i < n_threads; ++i) {
        if (results[i]) {
            printf("[MAIN] Freeing result from thread %zu (%p, %zu bytes)\n", i,
                   results[i], result_sizes[i]);
            free(results[i]);
        }
    }
    free(results);
    free(result_sizes);
    printf("\n[MAIN] Arquivo desmapeado e recursos liberados\n");

    stop_timer(&total_timer);

    printf("\n[TIMING] ====== Performance Summary ======\n");
    printf("[TIMING] Initial CSV mapping: %.2f seconds\n",
           mapping_timer.elapsed_ms / 1000.0);
    printf("[TIMING] Device hash table building: %.2f seconds\n",
           hash_building_timer.elapsed_ms / 1000.0);
    printf("[TIMING] CSV partitioning by device: %.2f seconds\n",
           partitioning_timer.elapsed_ms / 1000.0);
    printf("[TIMING] Parallel data processing: %.2f seconds\n",
           processing_timer.elapsed_ms / 1000.0);
    printf("[TIMING] Total program execution: %.2f seconds\n",
           total_timer.elapsed_ms / 1000.0);
    printf("[TIMING] =================================\n");

    return EXIT_SUCCESS;
}