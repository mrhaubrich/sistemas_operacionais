#include "../include/line_count.h"

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "../include/thread_utils.h"
#include "../include/utils.h"

// Add these global variables at the top of the file
const char **global_line_index = NULL;
int total_indexed_lines = 0;

int count_lines_in_memory(const char *data, size_t size) {
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
                line_count++;
            }
            break;
        }
    }

    return line_count;
}

int count_lines_in_memory_parallel(const char *data, size_t size,
                                   size_t block_count) {
    int total_line_count = 0;
    const int num_threads = get_available_number_of_processors();

    ThreadResources *resources = allocate_thread_resources(num_threads);
    if (!resources) {
        fprintf(stderr, "Falha na alocação de memória para threads\n");
        return count_lines_in_memory(data, size);
    }

    if (start_threads(resources, data, size) != 0) {
        fprintf(stderr, "Falha ao iniciar threads\n");
    }

    total_line_count = join_threads_and_collect_results(resources);

    // Merge all thread-local line indices into a global index
    const char **global_index = merge_line_indices(resources);
    if (!global_index) {
        fprintf(stderr, "Falha ao mesclar índices de linha\n");
        global_line_index = NULL;
        total_indexed_lines = 0;
    } else {
        // Set the global variables to the validated values from
        // merge_line_indices
        global_line_index = global_index;
        total_indexed_lines = resources->total_lines;

        printf(
            "Índice global de linhas criado com sucesso, total de %d linhas "
            "indexadas\n",
            resources->total_lines);
    }

    int duplicates = correct_duplicate_lines(resources->thread_data,
                                             num_threads, data, size);
    total_line_count -= duplicates;

    printf("Linhas duplicadas corrigidas: %d\n", duplicates);

    // Don't free resources->global_line_index anymore, it's now managed by
    // file_mapping
    resources->global_line_index = NULL;
    free_thread_resources(resources);

    return total_line_count;
}

void *count_lines_worker(void *arg) {
    ThreadData *data = (ThreadData *)arg;
    int local_count = 0;
    const char *p = data->start;
    const char *end = p + data->size;

    // Initialize line index array with initial capacity
    const int INITIAL_CAPACITY = 1024;
    data->line_indices = malloc(sizeof(const char *) * INITIAL_CAPACITY);
    data->index_capacity = INITIAL_CAPACITY;
    data->index_count = 0;

    if (!data->line_indices) {
        fprintf(stderr, "Failed to allocate memory for line index array\n");
        data->line_count = 0;
        return NULL;
    }

    // The first line always starts at the beginning of the block
    if (p < end) {
        data->line_indices[data->index_count++] = p;
    }

    // Conta linhas no bloco designado e constrói o índice simultaneamente
    while (p < end) {
        const char *nl = memchr(p, '\n', end - p);
        if (nl) {
            local_count++;
            p = nl + 1;

            // If we found a line break and there's still data, record the start
            // of the next line
            if (p < end) {
                // Check if we need to resize the index array
                if (data->index_count >= data->index_capacity) {
                    int new_capacity = data->index_capacity * 2;
                    const char **new_indices =
                        realloc(data->line_indices,
                                sizeof(const char *) * new_capacity);
                    if (!new_indices) {
                        fprintf(stderr, "Failed to resize line index array\n");
                        break;
                    }
                    data->line_indices = new_indices;
                    data->index_capacity = new_capacity;
                }

                // Store the pointer to the start of this line
                data->line_indices[data->index_count++] = p;
            }
        } else {
            // Se não encontrou mais \n, mas ainda há dados, é a última linha
            if (p < end) {
                local_count++;
            }
            break;
        }
    }

    // Armazena a contagem local no resultado da thread
    data->line_count = local_count;

    return NULL;
}
