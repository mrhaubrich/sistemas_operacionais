#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/wait.h>
#include <time.h>

#include "../include/data_analysis.h"
#include "../include/file_mapping.h"
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

    UDSInfo uds_info;
    generate_uds_path(id, &uds_info);
    int server_fd = establish_uds_server(&uds_info);
    if (server_fd < 0) {
        wargs->line_counts[id] = 0;
        wargs->results[id] = NULL;
        wargs->result_sizes[id] = 0;
        pthread_exit(NULL);
    }
    pid_t pid = launch_python_process(&uds_info, script_path);
    if (pid < 0) {
        cleanup_uds(&uds_info);
        wargs->line_counts[id] = 0;
        wargs->results[id] = NULL;
        wargs->result_sizes[id] = 0;
        pthread_exit(NULL);
    }
    // Envia o chunk para o processo Python
    send_csv_chunk(&uds_info, queue);

    // Recebe resultado processado e conta linhas
    size_t buffer_size = 1024 * 1024;
    char *buffer = malloc(buffer_size);
    int received =
        buffer ? receive_processed_csv(&uds_info, buffer, buffer_size) : -1;
    int lines = 0;
    if (received > 0) {
        for (int j = 0; j < received; ++j)
            if (buffer[j] == '\n') lines++;
        wargs->results[id] = buffer;
        wargs->result_sizes[id] = (size_t)received;
    } else {
        if (buffer) free(buffer);
        wargs->results[id] = NULL;
        wargs->result_sizes[id] = 0;
    }
    wargs->line_counts[id] = lines;

    // Aguarda processo filho e limpa UDS
    if (pid > 0) waitpid(pid, NULL, 0);
    cleanup_uds(&uds_info);

    printf("Thread %d completed processing.\n", id);

    pthread_exit(NULL);
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
    if (!validate_args(argc, argv)) {
        return EXIT_FAILURE;
    }

    const char *filepath = argv[1];

    // Validar extensão do arquivo
    if (!validate_csv_extension(filepath)) {
        return EXIT_FAILURE;
    }

    // Imprimir informações do sistema
    int num_processors = get_available_number_of_processors();
    printf("Processadores disponíveis: %d\n", num_processors);
    printf("Processando arquivo: %s\n\n", filepath);

    // Mapear arquivo na memória
    MappedCSV mappedCsv = map_csv(filepath);
    if (mappedCsv.header == NULL) {
        fprintf(stderr, "Falha ao mapear o arquivo\n");
        return EXIT_FAILURE;
    }

    // Imprimir informações do arquivo
    print_csv_info(&mappedCsv);

    // --- NOVA LÓGICA USANDO PTHREADS PARA PROCESSAMENTO PARALELO COM PYTHON
    // ---

    size_t n_cpus = num_processors;
    ThreadSafeQueue *queue = thread_safe_queue_create();
    if (!queue) {
        fprintf(stderr, "Falha ao criar fila de chunks\n");
        unmap_csv(&mappedCsv);
        return EXIT_FAILURE;
    }

    int calculated_size =
        mappedCsv.data_count / n_cpus + (mappedCsv.data_count % n_cpus != 0);
    int chunk_size = (calculated_size < 100000) ? calculated_size : 100000;

    printf("Tamanho do chunk: %d\n", chunk_size);

    size_t num_chunks = partition_csv(&mappedCsv, chunk_size, queue);
    printf("Número de chunks particionados: %zu\n", num_chunks);

    if (num_chunks == 0) {
        fprintf(stderr, "Falha ao particionar o CSV\n");
        thread_safe_queue_destroy(queue);
        unmap_csv(&mappedCsv);
        return EXIT_FAILURE;
    }

    // Caminho para o script Python
    const char *script_path = "./src/script/analyze_data.py";

    // Cria threads e argumentos
    pthread_t *threads = malloc(sizeof(pthread_t) * num_chunks);
    WorkerArgs *args = malloc(sizeof(WorkerArgs) * num_chunks);
    int *line_counts = calloc(num_chunks, sizeof(int));
    char **results = calloc(num_chunks, sizeof(char *));
    size_t *result_sizes = calloc(num_chunks, sizeof(size_t));
    if (!threads || !args || !line_counts || !results || !result_sizes) {
        fprintf(stderr, "Falha ao alocar recursos de threads\n");
        free(threads);
        free(args);
        free(line_counts);
        free(results);
        free(result_sizes);
        thread_safe_queue_destroy(queue);
        unmap_csv(&mappedCsv);
        return EXIT_FAILURE;
    }

    for (size_t i = 0; i < n_cpus; ++i) {
        args[i].thread_id = (int)i;
        args[i].queue = queue;
        args[i].script_path = script_path;
        args[i].line_counts = line_counts;
        args[i].results = results;
        args[i].result_sizes = result_sizes;
        pthread_create(&threads[i], NULL, worker_func, &args[i]);
    }

    // Aguarda todas as threads terminarem
    for (size_t i = 0; i < n_cpus; ++i) {
        pthread_join(threads[i], NULL);
    }

    // Soma total de linhas retornadas
    int total_lines = 0;
    for (size_t i = 0; i < num_chunks; ++i) {
        total_lines += line_counts[i];
    }

    // Print first 10 lines of all returned CSVs
    printf("\nPrimeiras 10 linhas retornadas por todos os CSVs processados:\n");
    for (size_t i = 0; i < num_chunks; ++i) {
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

    // Limpeza
    for (size_t i = 0; i < num_chunks; ++i) {
        if (results[i]) free(results[i]);
    }
    free(threads);
    free(args);
    free(line_counts);
    free(results);
    free(result_sizes);
    thread_safe_queue_destroy(queue);
    unmap_csv(&mappedCsv);
    printf("\nArquivo desmapeado e recursos liberados\n");

    return EXIT_SUCCESS;
}