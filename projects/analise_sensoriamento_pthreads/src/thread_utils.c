#include "../include/thread_utils.h"

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>

#include "../include/line_count.h"

ThreadResources *allocate_thread_resources(int num_threads) {
    ThreadResources *res = malloc(sizeof(ThreadResources));
    if (!res) {
        return NULL;
    }

    res->num_threads = num_threads;
    res->threads = malloc(sizeof(pthread_t) * num_threads);
    res->thread_data = malloc(sizeof(ThreadData) * num_threads);

    if (!res->threads || !res->thread_data) {
        free(res->threads);
        free(res->thread_data);
        free(res);
        return NULL;
    }

    return res;
}

void free_thread_resources(ThreadResources *resources) {
    if (resources) {
        free(resources->threads);
        free(resources->thread_data);
        free(resources);
    }
}

size_t calculate_block_size(int thread_index, int num_threads,
                            size_t total_size) {
    size_t block_size = total_size / num_threads;
    size_t remaining = total_size % num_threads;

    return (thread_index == num_threads - 1) ? block_size + remaining
                                             : block_size;
}

void initialize_thread_data(ThreadData *thread_data, int index,
                            const char *data, size_t block_size,
                            size_t block_offset) {
    thread_data[index].start = data + block_offset;
    thread_data[index].size = block_size;
    thread_data[index].line_count = 0;
}

void adjust_block_boundaries(ThreadData *thread_data, int i, const char *data) {
    if (i > 0) {
        const char *ptr = thread_data[i].start;
        while (ptr < data + thread_data[i].size && *ptr != '\n') {
            ptr++;
        }
        if (ptr < data + thread_data[i].size && *ptr == '\n') {
            ptr++;
        }
        size_t adjustment = ptr - thread_data[i].start;
        thread_data[i].start = ptr;
        thread_data[i].size -= adjustment;
        thread_data[i - 1].size += adjustment;
    }
}

int correct_duplicate_lines(ThreadData *thread_data, int num_threads,
                            const char *data, size_t size) {
    int duplicates = 0;

    for (int i = 1; i < num_threads; i++) {
        const char *prev_end =
            thread_data[i - 1].start + thread_data[i - 1].size - 1;

        if ((prev_end > data) && (*prev_end != '\n') &&
            (thread_data[i].start < data + size) &&
            (*(thread_data[i].start) != '\n')) {
            duplicates++;
        }
    }

    printf("Linhas duplicadas corrigidas: %d\n", duplicates);

    return duplicates;
}

int start_threads(ThreadResources *resources, const char *data, size_t size) {
    size_t current_offset = 0;

    for (int i = 0; i < resources->num_threads; i++) {
        size_t block_size =
            calculate_block_size(i, resources->num_threads, size);

        initialize_thread_data(resources->thread_data, i, data, block_size,
                               current_offset);

        adjust_block_boundaries(resources->thread_data, i, data);

        if (pthread_create(&resources->threads[i], NULL, count_lines_worker,
                           &resources->thread_data[i]) != 0) {
            fprintf(stderr, "Falha ao criar thread %d\n", i);
            return -1;
        }

        current_offset = (resources->thread_data[i].start - data) +
                         resources->thread_data[i].size;
    }
    return 0;
}

int join_threads_and_collect_results(ThreadResources *resources) {
    int total_line_count = 0;

    for (int i = 0; i < resources->num_threads; i++) {
        pthread_join(resources->threads[i], NULL);
        total_line_count += resources->thread_data[i].line_count;
    }

    return total_line_count;
}