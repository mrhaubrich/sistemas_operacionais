#ifndef THREAD_UTILS_H
#define THREAD_UTILS_H

#include <pthread.h>
#include <stddef.h>

typedef struct {
    const char *start;
    size_t size;
    int line_count;
    const char **line_indices;  // Array to hold pointers to start of each line
    int index_capacity;         // Current allocation size for the index array
    int index_count;  // Number of lines (pointers) stored in the array
} ThreadData;

typedef struct {
    pthread_t *threads;
    ThreadData *thread_data;
    int num_threads;
    const char **global_line_index;  // Global array of all line pointers
    int total_lines;                 // Total number of lines indexed
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
const char **merge_line_indices(ThreadResources *resources);
int remove_duplicate_line_indices(const char **line_indices, int total_lines,
                                  int num_duplicates);

#endif  // THREAD_UTILS_H