#include "../include/line_count.h"

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "../include/thread_utils.h"
#include "../include/utils.h"

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

    int duplicates = correct_duplicate_lines(resources->thread_data,
                                             num_threads, data, size);
    total_line_count -= duplicates;

    free_thread_resources(resources);

    return total_line_count;
}

void *count_lines_worker(void *arg) {
    ThreadData *data = (ThreadData *)arg;
    int local_count = 0;
    const char *p = data->start;
    const char *end = p + data->size;

    // Conta linhas no bloco designado
    while (p < end) {
        const char *nl = memchr(p, '\n', end - p);
        if (nl) {
            local_count++;
            p = nl + 1;
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
