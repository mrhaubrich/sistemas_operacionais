#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/wait.h>
#include <time.h>

#include "../include/data_analysis.h"
#include "../include/file_mapping.h"
#include "../include/hash_table.h"
#include "../include/thread_safe_queue.h"
#include "../include/utils.h"

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

    int total_lines = 0;
    char *final_result = NULL;
    size_t final_result_size = 0;

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
            break;
        }

        UDSInfo uds_info;
        generate_uds_path(id, &uds_info);
        int server_fd = establish_uds_server(&uds_info);
        if (server_fd < 0) {
            continue;
        }
        pid_t pid = launch_python_process(&uds_info, script_path);
        if (pid < 0) {
            cleanup_uds(&uds_info);
            continue;
        }

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
            // Concatenate results if needed
            char *new_result =
                realloc(final_result, final_result_size + received);
            if (new_result) {
                memcpy(new_result + final_result_size, buffer, received);
                final_result = new_result;
                final_result_size += received;
            }
        }
        if (buffer) free(buffer);

        if (pid > 0) waitpid(pid, NULL, 0);
        cleanup_uds(&uds_info);
    }

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
    printf("Processadores disponíveis: %d\n", num_processors);
    printf("Processando arquivo: %s\n", filepath);
    printf("Coluna de dispositivo: %s\n\n", device_column_name);

    // Primeira etapa: mapear o arquivo para obter o cabeçalho e encontrar o
    // índice da coluna de dispositivo
    MappedCSV temp_csv = map_csv(filepath);
    if (temp_csv.header == NULL) {
        fprintf(stderr, "Falha ao mapear o arquivo\n");
        return EXIT_FAILURE;
    }

    // Encontrar o índice da coluna de dispositivo
    int device_column = find_device_column(temp_csv.header, device_column_name);
    if (device_column < 0) {
        fprintf(stderr, "Erro: Coluna '%s' não encontrada no cabeçalho\n",
                device_column_name);
        unmap_csv(&temp_csv);
        return EXIT_FAILURE;
    }

    printf("Índice da coluna de dispositivo '%s': %d\n", device_column_name,
           device_column);

    // Liberar o CSV temporário
    unmap_csv(&temp_csv);

    // Mapear novamente usando a abordagem baseada em dispositivo
    DeviceMappedCSV deviceMappedCsv = map_device_csv(filepath, device_column);
    if (deviceMappedCsv.header == NULL || !deviceMappedCsv.device_table) {
        fprintf(stderr,
                "Falha ao mapear o arquivo utilizando tabela hash de "
                "dispositivos\n");
        return EXIT_FAILURE;
    }

    // Imprimir informações do arquivo
    printf("\nInformações do CSV:\n");
    printf("- Linhas: %d\n", deviceMappedCsv.data_count);
    printf("- Cabeçalho: %s\n", deviceMappedCsv.header);
    printf("- Dispositivos únicos: %d\n",
           deviceMappedCsv.device_table->device_count);

    // --- NOVA LÓGICA USANDO PTHREADS PARA PROCESSAMENTO PARALELO COM PYTHON
    // POR DISPOSITIVO ---

    ThreadSafeQueue *queue = thread_safe_queue_create();
    if (!queue) {
        fprintf(stderr, "Falha ao criar fila de chunks\n");
        unmap_device_csv(&deviceMappedCsv);
        return EXIT_FAILURE;
    }

    // Particionar por dispositivo em vez de linhas
    size_t num_chunks = partition_csv_by_device(&deviceMappedCsv, queue);
    printf("Número de dispositivos particionados: %zu\n", num_chunks);

    if (num_chunks == 0) {
        fprintf(stderr, "Falha ao particionar o CSV por dispositivos\n");
        thread_safe_queue_destroy(queue);
        unmap_device_csv(&deviceMappedCsv);
        return EXIT_FAILURE;
    }

    // Caminho para o script Python
    const char *script_path = "./src/script/analyze_data.py";

    // Determinar número de threads para usar (mínimo entre num_processors e
    // num_chunks)
    size_t n_threads = ((size_t)num_processors < num_chunks)
                           ? (size_t)num_processors
                           : num_chunks;

    // Cria threads e argumentos
    pthread_t *threads = malloc(sizeof(pthread_t) * n_threads);
    WorkerArgs *args = malloc(sizeof(WorkerArgs) * n_threads);
    int *line_counts = calloc(n_threads, sizeof(int));
    char **results = calloc(n_threads, sizeof(char *));
    size_t *result_sizes = calloc(n_threads, sizeof(size_t));

    if (!threads || !args || !line_counts || !results || !result_sizes) {
        fprintf(stderr, "Falha ao alocar recursos de threads\n");
        if (threads) free(threads);
        if (args) free(args);
        if (line_counts) free(line_counts);
        if (results) free(results);
        if (result_sizes) free(result_sizes);
        thread_safe_queue_destroy(queue);
        unmap_device_csv(&deviceMappedCsv);
        return EXIT_FAILURE;
    }

    printf("Iniciando %zu threads para processar %zu dispositivos...\n",
           n_threads, num_chunks);

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
    for (size_t i = 0; i < n_threads; ++i) {
        pthread_join(threads[i], NULL);
    }

    // Soma total de linhas retornadas
    int total_lines = 0;
    for (size_t i = 0; i < n_threads; ++i) {
        total_lines += line_counts[i];
    }

    // Limpeza antecipada dos arrays de contagem e argumentos
    free(threads);
    free(args);
    free(line_counts);
    thread_safe_queue_destroy(queue);
    unmap_device_csv(&deviceMappedCsv);

    // Print first 10 lines of all returned CSVs
    printf("\nPrimeiras 10 linhas retornadas por todos os CSVs processados:\n");
    for (size_t i = 0; i < n_threads; ++i) {
        printf("Thread %zu: %d linhas\n", i, line_counts[i]);
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

    printf("\nTotal de linhas retornadas por todos os CSVs processados: %d\n",
           total_lines);

    // Limpeza final dos resultados
    for (size_t i = 0; i < num_chunks; ++i) {
        if (results[i]) free(results[i]);
    }
    free(results);
    free(result_sizes);
    printf("\nArquivo desmapeado e recursos liberados\n");

    return EXIT_SUCCESS;
}