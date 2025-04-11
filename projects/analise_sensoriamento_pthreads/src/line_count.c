#include "../include/line_count.h"

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "../include/thread_utils.h"
#include "../include/utils.h"

/**
 * Count lines sequentially in a memory buffer
 * @param data Pointer to the data buffer
 * @param size Size of the data buffer in bytes
 * @return Number of lines in the buffer
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
                line_count++;  // Count the last line if it doesn't end with
                               // newline
            }
            break;
        }
    }

    return line_count;
}

/**
 * Count lines in parallel and build a line index for the given data buffer
 * @param data Pointer to the data buffer
 * @param size Size of the data buffer in bytes
 * @param line_index_ptr Pointer to store the resulting line index
 * @param total_lines_ptr Pointer to store the total number of indexed lines
 * @return Total number of lines counted
 */
int count_lines_in_memory_parallel(const char *data, size_t size,
                                   const char ***line_index_ptr,
                                   int *total_lines_ptr) {
    if (!data || size == 0) {
        if (line_index_ptr) *line_index_ptr = NULL;
        if (total_lines_ptr) *total_lines_ptr = 0;
        return 0;
    }

    // For small files (less than 100KB), use sequential processing to avoid
    // thread overhead
    if (size < 102400) {
        int line_count = count_lines_in_memory(data, size);

        // Create a simple line index for the sequential case
        const char **line_index = NULL;
        int indexed_lines = 0;

        if (line_count > 0) {
            line_index = malloc(sizeof(const char *) * line_count);
            if (line_index) {
                const char *p = data;
                const char *end = data + size;
                indexed_lines = 0;

                // Record the start of the first line
                line_index[indexed_lines++] = p;

                // Find and record the start of each subsequent line
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

    // For larger files, use parallel processing
    int total_line_count = 0;
    const int num_threads = get_available_number_of_processors();

    // Limit the number of threads for small files to avoid overhead
    int actual_threads =
        (size < 1048576) ? 2
                         : num_threads;  // Use fewer threads for files < 1MB

    ThreadResources *resources = allocate_thread_resources(actual_threads);
    if (!resources) {
        fprintf(stderr, "Failed to allocate memory for threads\n");
        if (line_index_ptr) *line_index_ptr = NULL;
        if (total_lines_ptr) *total_lines_ptr = 0;
        return count_lines_in_memory(data,
                                     size);  // Fallback to sequential count
    }

    if (start_threads(resources, data, size) != 0) {
        fprintf(stderr, "Failed to start threads\n");
        free_thread_resources(resources);
        if (line_index_ptr) *line_index_ptr = NULL;
        if (total_lines_ptr) *total_lines_ptr = 0;
        return count_lines_in_memory(data,
                                     size);  // Fallback to sequential count
    }

    total_line_count = join_threads_and_collect_results(resources);

    // Merge all thread-local line indices into a global index
    const char **global_index = merge_line_indices(resources);
    int total_indexed_lines = resources->total_lines;

    if (!global_index) {
        fprintf(stderr, "Failed to merge line indices\n");
        if (line_index_ptr) *line_index_ptr = NULL;
        if (total_lines_ptr) *total_lines_ptr = 0;
    } else {
        printf("Global line index successfully created with %d indexed lines\n",
               total_indexed_lines);

        if (line_index_ptr) *line_index_ptr = global_index;
        if (total_lines_ptr) *total_lines_ptr = total_indexed_lines;
    }

    // Correct for duplicate lines at thread boundaries
    int duplicates = correct_duplicate_lines(resources->thread_data,
                                             actual_threads, data, size);
    total_line_count -= duplicates;

    if (duplicates > 0) {
        printf("Corrected %d duplicate lines\n", duplicates);
    }

    // Pass ownership of the line index to caller
    resources->global_line_index = NULL;
    free_thread_resources(resources);

    return total_line_count;
}

/**
 * Thread worker function that counts lines in its assigned portion of the file
 * and builds a line index for fast access.
 * @param arg Thread data containing the block to process
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

    // Count lines in the assigned block and build index simultaneously
    while (p < end) {
        const char *nl = memchr(p, '\n', end - p);
        if (nl) {
            local_count++;
            p = nl + 1;

            // If we found a line break and there's still data, record the start
            // of the next line
            if (p < end) {
                // Grow the index array if needed
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
            // If there are no more newlines but still data, it's the last line
            if (p < end) {
                local_count++;
            }
            break;
        }
    }

    // Store the local count in the thread result
    data->line_count = local_count;

    return NULL;
}
