#include "../include/thread_utils.h"

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "../include/line_count.h"

/**
 * Aloca memória para recursos de threads
 */
ThreadResources *allocate_thread_resources(int num_threads) {
    if (num_threads <= 0) {
        fprintf(stderr, "Erro: Número inválido de threads solicitado (%d)\n",
                num_threads);
        return NULL;
    }

    ThreadResources *res = malloc(sizeof(ThreadResources));
    if (!res) {
        perror("Falha ao alocar recursos de threads");
        return NULL;
    }

    res->num_threads = num_threads;
    res->threads = malloc(sizeof(pthread_t) * num_threads);
    res->thread_data = malloc(sizeof(ThreadData) * num_threads);
    res->total_lines = 0;

    if (!res->threads || !res->thread_data) {
        perror("Falha ao alocar memória para arrays de threads");
        free(res->threads);
        free(res->thread_data);
        free(res);
        return NULL;
    }

    return res;
}

/**
 * Libera toda a memória alocada para recursos de threads
 */
void free_thread_resources(ThreadResources *resources) {
    if (!resources) {
        return;
    }

    // Libera arrays de threads
    free(resources->threads);
    free(resources->thread_data);

    // Libera a estrutura principal
    free(resources);
}

/**
 * Calcula o tamanho do bloco para uma thread específica
 */
size_t calculate_block_size(int thread_index, int num_threads,
                            size_t total_size) {
    if (num_threads <= 0) return 0;

    size_t block_size = total_size / num_threads;
    size_t remaining = total_size % num_threads;

    // A última thread recebe quaisquer bytes restantes
    return (thread_index == num_threads - 1) ? block_size + remaining
                                             : block_size;
}

/**
 * Inicializa dados de thread para uma thread específica
 */
void initialize_thread_data(ThreadData *thread_data, int index,
                            const char *data, size_t block_size,
                            size_t block_offset) {
    if (!thread_data || !data) return;

    thread_data[index].start = data + block_offset;
    thread_data[index].size = block_size;
    thread_data[index].line_count = 0;
}

/**
 * Ajusta os limites do bloco para garantir que estejam alinhados com quebras de
 * linha
 */
void adjust_block_boundaries(ThreadData *thread_data, int i, const char *data) {
    if (!thread_data || !data || i <= 0) return;

    // Encontra o próximo caractere de nova linha no bloco desta thread
    const char *ptr = thread_data[i].start;
    const char *end = thread_data[i].start + thread_data[i].size;

    // Procura pelo próximo caractere de nova linha para alinhar o bloco
    while (ptr < end && *ptr != '\n') {
        ptr++;
    }

    // Se encontrarmos uma nova linha, avançamos para fazer um limite limpo
    if (ptr < end && *ptr == '\n') {
        ptr++;
    }

    // Calcula o tamanho do ajuste
    size_t adjustment = ptr - thread_data[i].start;

    // Move o limite
    thread_data[i].start = ptr;
    thread_data[i].size -= adjustment;
    thread_data[i - 1].size += adjustment;
}

/**
 * Identifica linhas duplicadas que podem ocorrer nos limites das threads
 */
int correct_duplicate_lines(ThreadData *thread_data, int num_threads,
                            const char *data, size_t size) {
    if (!thread_data || !data || num_threads <= 1) return 0;

    int duplicates = 0;

    // Verifica cada limite de thread para possíveis duplicatas
    for (int i = 1; i < num_threads; i++) {
        const char *prev_end =
            thread_data[i - 1].start + thread_data[i - 1].size - 1;

        // Se o bloco anterior não termina com uma nova linha e o bloco atual
        // não começa com uma nova linha, provavelmente temos uma linha contada
        // duas vezes
        if ((prev_end > data) && (*prev_end != '\n') &&
            (thread_data[i].start < data + size) &&
            (*(thread_data[i].start) != '\n')) {
            duplicates++;
        }
    }

    return duplicates;
}

/**
 * Inicia threads para processar blocos de arquivo em paralelo
 */
int start_threads(ThreadResources *resources, const char *data, size_t size) {
    if (!resources || !data || size == 0) {
        return -1;
    }

    size_t current_offset = 0;

    for (int i = 0; i < resources->num_threads; i++) {
        // Calcula o tamanho do bloco desta thread
        size_t block_size =
            calculate_block_size(i, resources->num_threads, size);

        // Inicializa os dados desta thread
        initialize_thread_data(resources->thread_data, i, data, block_size,
                               current_offset);

        // Ajusta os limites do bloco para alinhar com novas linhas
        adjust_block_boundaries(resources->thread_data, i, data);

        // Cria a thread e inicia-a
        int result =
            pthread_create(&resources->threads[i], NULL, count_lines_worker,
                           &resources->thread_data[i]);
        if (result != 0) {
            fprintf(stderr, "Falha ao criar thread %d: %s\n", i,
                    strerror(result));
            return -1;
        }

        // Atualiza o offset para a próxima thread
        current_offset = (resources->thread_data[i].start - data) +
                         resources->thread_data[i].size;
    }

    return 0;
}

/**
 * Aguarda todas as threads terminarem e coleta seus resultados
 */
int join_threads_and_collect_results(ThreadResources *resources) {
    if (!resources) return 0;

    int total_line_count = 0;

    for (int i = 0; i < resources->num_threads; i++) {
        int result = pthread_join(resources->threads[i], NULL);
        if (result != 0) {
            fprintf(stderr, "Falha ao aguardar thread %d: %s\n", i,
                    strerror(result));
            // Continua com outras threads
        }

        total_line_count += resources->thread_data[i].line_count;
    }

    // Armazena o total de linhas
    resources->total_lines = total_line_count;

    return total_line_count;
}