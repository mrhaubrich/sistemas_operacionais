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
    res->global_line_index = NULL;
    res->total_lines = 0;

    if (!res->threads || !res->thread_data) {
        perror("Falha ao alocar memória para arrays de threads");
        free(res->threads);
        free(res->thread_data);
        free(res);
        return NULL;
    }

    // Inicializa os campos de índice de linha para cada dado de thread
    for (int i = 0; i < num_threads; i++) {
        res->thread_data[i].line_indices = NULL;
        res->thread_data[i].index_capacity = 0;
        res->thread_data[i].index_count = 0;
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

    // Libera índices de linha para cada thread
    for (int i = 0; i < resources->num_threads; i++) {
        free(resources->thread_data[i].line_indices);
    }

    // Libera índice de linha global
    free(resources->global_line_index);

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
    thread_data[index].line_indices = NULL;
    thread_data[index].index_capacity = 0;
    thread_data[index].index_count = 0;
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

    return total_line_count;
}

/**
 * Mescla índices de linha locais das threads em um índice global
 */
const char **merge_line_indices(ThreadResources *resources) {
    if (!resources) return NULL;

    int total_lines = 0;

    // Conta o total de linhas para alocação
    for (int i = 0; i < resources->num_threads; i++) {
        ThreadData *thread_data = &resources->thread_data[i];
        if (thread_data->line_indices != NULL && thread_data->index_count > 0) {
            total_lines += thread_data->index_count;
        }
    }

    resources->total_lines = total_lines;

    // Nenhuma linha para indexar
    if (total_lines == 0) {
        return NULL;
    }

    // Aloca memória para o índice global
    const char **global_index = malloc(sizeof(const char *) * total_lines);
    if (!global_index) {
        perror("Falha ao alocar memória para índice de linha global");
        return NULL;
    }

    // Copia todos os índices de linha válidos de cada thread
    int global_idx = 0;
    for (int i = 0; i < resources->num_threads; i++) {
        ThreadData *thread_data = &resources->thread_data[i];

        if (thread_data->line_indices == NULL ||
            thread_data->index_count <= 0) {
            continue;  // Ignora dados de thread inválidos
        }

        // Copia apenas índices válidos
        for (int j = 0;
             j < thread_data->index_count && global_idx < total_lines; j++) {
            // Inclui apenas ponteiros válidos
            if (thread_data->line_indices[j] >= thread_data->start &&
                thread_data->line_indices[j] <
                    thread_data->start + thread_data->size) {
                global_index[global_idx++] = thread_data->line_indices[j];
            }
        }
    }

    // Atualiza a contagem real de índices armazenados
    resources->total_lines = global_idx;

    printf("Índice global construído com sucesso com %d linhas\n", global_idx);

    resources->global_line_index = global_index;
    return global_index;
}

/**
 * Remove índices de linha duplicados do índice global
 */
int remove_duplicate_line_indices(const char **line_indices, int total_lines,
                                  int num_duplicates) {
    if (!line_indices || total_lines <= 0 || num_duplicates <= 0) {
        return 0;
    }

    int removed = 0;

    // Primeira passagem: Verifica duplicatas claras nos limites das threads
    for (int i = 1; i < total_lines && removed < num_duplicates; i++) {
        // Se esta linha começa imediatamente após a anterior terminar
        // e não há nova linha entre elas, provavelmente é uma duplicata
        const char *current = line_indices[i];
        const char *previous = line_indices[i - 1];

        // Encontra o final da linha anterior (próxima nova linha)
        const char *prev_end = strchr(previous, '\n');

        // Se encontramos uma nova linha e a linha atual começa logo após
        // a linha anterior sem sua própria nova linha, provavelmente é uma
        // duplicata
        if (prev_end && current == prev_end + 1 && *current != '\n') {
            // Remove esta duplicata deslocando todos os índices seguintes para
            // baixo
            memmove(&line_indices[i], &line_indices[i + 1],
                    (total_lines - i - 1) * sizeof(const char *));
            removed++;

            // Ajusta i para verificar novamente o novo elemento que agora está
            // na posição i
            i--;
        }
    }

    // Se não conseguimos encontrar todas as duplicatas com a primeira
    // abordagem, usamos uma estratégia mais agressiva
    if (removed < num_duplicates) {
        // Segunda passagem: Remove com base em inícios de linha muito próximos,
        // que podem ocorrer nos limites das threads onde o padrão exato não
        // corresponde à nossa primeira passagem
        for (int i = 1; i < total_lines - removed && removed < num_duplicates;
             i++) {
            const char *current = line_indices[i];
            const char *previous = line_indices[i - 1];

            // Se as linhas estão muito próximas umas das outras (dentro de
            // alguns bytes), elas podem ser duplicatas do processo de divisão
            // de threads
            if (current > previous && current - previous < 5) {
                // Remove esta duplicata deslocando todos os índices seguintes
                memmove(&line_indices[i], &line_indices[i + 1],
                        (total_lines - removed - i - 1) * sizeof(const char *));
                removed++;

                // Ajusta i para verificar novamente o novo elemento
                i--;
            }
        }
    }

    return removed;
}