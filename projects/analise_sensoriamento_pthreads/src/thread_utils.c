#include "../include/thread_utils.h"

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "../include/line_count.h"

ThreadResources *allocate_thread_resources(int num_threads) {
    ThreadResources *res = malloc(sizeof(ThreadResources));
    if (!res) {
        return NULL;
    }

    res->num_threads = num_threads;
    res->threads = malloc(sizeof(pthread_t) * num_threads);
    res->thread_data = malloc(sizeof(ThreadData) * num_threads);
    res->global_line_index = NULL;
    res->total_lines = 0;

    if (!res->threads || !res->thread_data) {
        free(res->threads);
        free(res->thread_data);
        free(res);
        return NULL;
    }

    // Initialize the line index fields for each thread data
    for (int i = 0; i < num_threads; i++) {
        res->thread_data[i].line_indices = NULL;
        res->thread_data[i].index_capacity = 0;
        res->thread_data[i].index_count = 0;
    }

    return res;
}

void free_thread_resources(ThreadResources *resources) {
    if (resources) {
        // Free line indices for each thread
        for (int i = 0; i < resources->num_threads; i++) {
            free(resources->thread_data[i].line_indices);
        }

        // Free global line index
        free(resources->global_line_index);
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
    thread_data[index].line_indices = NULL;
    thread_data[index].index_capacity = 0;
    thread_data[index].index_count = 0;
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

const char **merge_line_indices(ThreadResources *resources) {
    int total_lines = 0;

    // Count total lines for allocation
    for (int i = 0; i < resources->num_threads; i++) {
        ThreadData *thread_data = &resources->thread_data[i];
        if (thread_data->line_indices != NULL && thread_data->index_count > 0) {
            total_lines += thread_data->index_count;
        }
    }

    resources->total_lines = total_lines;

    // Allocate memory for global index
    const char **global_index = malloc(sizeof(const char *) * total_lines);
    if (!global_index) {
        fprintf(stderr, "Failed to allocate memory for global line index\n");
        return NULL;
    }

    // Copy all valid line indices from each thread
    int global_idx = 0;
    for (int i = 0; i < resources->num_threads; i++) {
        ThreadData *thread_data = &resources->thread_data[i];

        if (thread_data->line_indices == NULL ||
            thread_data->index_count <= 0) {
            continue;  // Skip invalid thread data
        }

        // Only copy valid indices
        for (int j = 0;
             j < thread_data->index_count && global_idx < total_lines; j++) {
            // Only include valid pointers
            if (thread_data->line_indices[j] >= thread_data->start &&
                thread_data->line_indices[j] <
                    thread_data->start + thread_data->size) {
                global_index[global_idx++] = thread_data->line_indices[j];
            }
        }
    }

    // Update actual count of indices stored
    resources->total_lines = global_idx;

    printf("Successfully built global index with %d lines\n", global_idx);

    resources->global_line_index = global_index;
    return global_index;
}

int remove_duplicate_line_indices(const char **line_indices, int total_lines,
                                  int num_duplicates) {
    if (!line_indices || total_lines <= 0 || num_duplicates <= 0) {
        return 0;
    }

    int removed = 0;

    // Identify thread boundaries where duplicates occur
    // Typically duplicates happen at thread block boundaries
    for (int i = 1; i < total_lines && removed < num_duplicates; i++) {
        // If this line starts immediately after the previous one ends
        // and there's no newline between them, it's likely a duplicate
        const char *current = line_indices[i];
        const char *previous = line_indices[i - 1];

        // Find end of previous line (next newline)
        const char *prev_end = strchr(previous, '\n');

        // If we found a newline and the current line starts right after
        // previous line without its own newline start, it's likely a duplicate
        if (prev_end && current == prev_end + 1 && *current != '\n') {
            // Remove this duplicate by shifting all following indices down
            memmove(&line_indices[i], &line_indices[i + 1],
                    (total_lines - i - 1) * sizeof(const char *));
            removed++;

            // Adjust i to recheck the new element that's now in position i
            i--;
        }
    }

    // If we couldn't find all duplicates with the above approach, use a more
    // aggressive strategy
    if (removed < num_duplicates) {
        // Second pass: Remove based on very close line starts, which might
        // happen at thread boundaries where the exact pattern doesn't match our
        // first pass
        for (int i = 1; i < total_lines - removed && removed < num_duplicates;
             i++) {
            const char *current = line_indices[i];
            const char *previous = line_indices[i - 1];

            // If lines are very close to each other (within a few bytes), they
            // might be duplicates from the thread splitting process
            if (current > previous && current - previous < 5) {
                // Remove this duplicate by shifting all following indices
                memmove(&line_indices[i], &line_indices[i + 1],
                        (total_lines - removed - i - 1) * sizeof(const char *));
                removed++;

                // Adjust i to recheck the new element
                i--;
            }
        }
    }

    return removed;
}