#include "../include/line_count.h"

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "../include/thread_utils.h"
#include "../include/utils.h"

/**
 * Contar linhas sequencialmente em um buffer de memória
 * @param data Ponteiro para o buffer de dados
 * @param size Tamanho do buffer de dados em bytes
 * @return Número de linhas no buffer
 */
int count_lines_in_memory(const char *data, size_t size) {
    if (!data || size == 0) {
        return 0;
    }

    int line_count = 0;
    const char *p = data;
    const char *end = data + size;

    while (p < end) {
        const char *nl = memchr(p, '\n', end - p);
        if (nl) {
            line_count++;
            p = nl + 1;
        } else {
            if (p < end) {
                line_count++;  // Contar a última linha se não terminar com \n
            }
            break;
        }
    }

    return line_count;
}

/**
 * Contar linhas em paralelo e construir um índice de linhas para o buffer de
 * dados fornecido
 * @param data Ponteiro para o buffer de dados
 * @param size Tamanho do buffer de dados em bytes
 * @param line_index_ptr Ponteiro para armazenar o índice de linhas resultante
 * @param total_lines_ptr Ponteiro para armazenar o número total de linhas
 * indexadas
 * @return Número total de linhas contadas
 */
int count_lines_in_memory_parallel(const char *data, size_t size,
                                   const char ***line_index_ptr,
                                   int *total_lines_ptr) {
    if (!data || size == 0) {
        if (line_index_ptr) *line_index_ptr = NULL;
        if (total_lines_ptr) *total_lines_ptr = 0;
        return 0;
    }

    // Para arquivos pequenos (menos de 100KB), usar processamento sequencial
    // para evitar sobrecarga de threads
    if (size < 102400) {
        int line_count = count_lines_in_memory(data, size);

        // Criar um índice de linhas simples para o caso sequencial
        const char **line_index = NULL;
        int indexed_lines = 0;

        if (line_count > 0) {
            line_index = malloc(sizeof(const char *) * line_count);
            if (line_index) {
                const char *p = data;
                const char *end = data + size;
                indexed_lines = 0;

                // Registrar o início da primeira linha
                line_index[indexed_lines++] = p;

                // Encontrar e registrar o início de cada linha subsequente
                while (p < end && indexed_lines < line_count) {
                    const char *nl = memchr(p, '\n', end - p);
                    if (nl && nl + 1 < end) {
                        p = nl + 1;
                        line_index[indexed_lines++] = p;
                    } else {
                        break;
                    }
                }
            }
        }

        if (line_index_ptr) *line_index_ptr = line_index;
        if (total_lines_ptr) *total_lines_ptr = indexed_lines;

        return line_count;
    }

    // Para arquivos maiores, usar processamento paralelo
    int total_line_count = 0;
    const int num_threads = get_available_number_of_processors();

    // Limitar o número de threads para arquivos pequenos para evitar sobrecarga
    int actual_threads =
        (size < 1048576)
            ? 2
            : num_threads;  // Usar menos threads para arquivos < 1MB

    ThreadResources *resources = allocate_thread_resources(actual_threads);
    if (!resources) {
        fprintf(stderr, "Falha ao alocar memória para threads\n");
        if (line_index_ptr) *line_index_ptr = NULL;
        if (total_lines_ptr) *total_lines_ptr = 0;
        return count_lines_in_memory(
            data,
            size);  // Alternativa para contagem sequencial
    }

    if (start_threads(resources, data, size) != 0) {
        fprintf(stderr, "Falha ao iniciar threads\n");
        free_thread_resources(resources);
        if (line_index_ptr) *line_index_ptr = NULL;
        if (total_lines_ptr) *total_lines_ptr = 0;
        return count_lines_in_memory(
            data,
            size);  // Alternativa para contagem sequencial
    }

    total_line_count = join_threads_and_collect_results(resources);

    // Mesclar todos os índices de linhas locais das threads em um índice global
    const char **global_index = merge_line_indices(resources);
    int total_indexed_lines = resources->total_lines;

    if (!global_index) {
        fprintf(stderr, "Falha ao mesclar índices de linhas\n");
        if (line_index_ptr) *line_index_ptr = NULL;
        if (total_lines_ptr) *total_lines_ptr = 0;
    } else {
        printf(
            "Índice global de linhas criado com sucesso com %d linhas "
            "indexadas\n",
            total_indexed_lines);

        if (line_index_ptr) *line_index_ptr = global_index;
        if (total_lines_ptr) *total_lines_ptr = total_indexed_lines;
    }

    // Corrigir linhas duplicadas nos limites das threads
    int duplicates = correct_duplicate_lines(resources->thread_data,
                                             actual_threads, data, size);
    total_line_count -= duplicates;

    if (duplicates > 0) {
        printf("Corrigidas %d linhas duplicadas\n", duplicates);
    }

    // Transferir propriedade do índice de linhas para o chamador
    resources->global_line_index = NULL;
    free_thread_resources(resources);

    return total_line_count;
}

/**
 * Função de trabalho da thread que conta linhas em sua porção atribuída do
 * arquivo e constrói um índice de linhas para acesso rápido.
 * @param arg Dados da thread contendo o bloco a ser processado
 * @return NULL
 */
void *count_lines_worker(void *arg) {
    ThreadData *data = (ThreadData *)arg;
    if (!data || !data->start) {
        return NULL;
    }

    int local_count = 0;
    const char *p = data->start;
    const char *end = p + data->size;

    // Inicializar array de índice de linhas com capacidade inicial
    const int INITIAL_CAPACITY = 1024;
    data->line_indices = malloc(sizeof(const char *) * INITIAL_CAPACITY);
    data->index_capacity = INITIAL_CAPACITY;
    data->index_count = 0;

    if (!data->line_indices) {
        fprintf(stderr,
                "Falha ao alocar memória para array de índice de linhas\n");
        data->line_count = 0;
        return NULL;
    }

    // A primeira linha sempre começa no início do bloco
    if (p < end) {
        data->line_indices[data->index_count++] = p;
    }

    // Contar linhas no bloco atribuído e construir índice simultaneamente
    while (p < end) {
        const char *nl = memchr(p, '\n', end - p);
        if (nl) {
            local_count++;
            p = nl + 1;

            // Se encontramos uma quebra de linha e ainda há dados, registrar o
            // início da próxima linha
            if (p < end) {
                // Expandir o array de índice se necessário
                if (data->index_count >= data->index_capacity) {
                    int new_capacity = data->index_capacity * 2;
                    const char **new_indices =
                        realloc(data->line_indices,
                                sizeof(const char *) * new_capacity);
                    if (!new_indices) {
                        fprintf(stderr,
                                "Falha ao redimensionar array de índice de "
                                "linhas\n");
                        break;
                    }
                    data->line_indices = new_indices;
                    data->index_capacity = new_capacity;
                }

                // Armazenar o ponteiro para o início desta linha
                data->line_indices[data->index_count++] = p;
            }
        } else {
            // Se não houver mais quebras de linha, mas ainda houver dados, é a
            // última linha
            if (p < end) {
                local_count++;
            }
            break;
        }
    }

    // Armazenar a contagem local no resultado da thread
    data->line_count = local_count;

    return NULL;
}
