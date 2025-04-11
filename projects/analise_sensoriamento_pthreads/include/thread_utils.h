#ifndef THREAD_UTILS_H
#define THREAD_UTILS_H

#include <pthread.h>
#include <stddef.h>

typedef struct {
    const char *start;
    size_t size;
    int line_count;
} ThreadData;

typedef struct {
    pthread_t *threads;
    ThreadData *thread_data;
    int num_threads;
} ThreadResources;

ThreadResources *allocate_thread_resources(int num_threads);
void free_thread_resources(ThreadResources *resources);
size_t calculate_block_size(int thread_index, int num_threads,
                            size_t total_size);
void initialize_thread_data(ThreadData *thread_data, int index,
                            const char *data, size_t block_size,
                            size_t block_offset);
void adjust_block_boundaries(ThreadData *thread_data, int i, const char *data);
int correct_duplicate_lines(ThreadData *thread_data, int num_threads,
                            const char *data, size_t size);
int start_threads(ThreadResources *resources, const char *data, size_t size);
int join_threads_and_collect_results(ThreadResources *resources);

#endif  // THREAD_UTILS_H