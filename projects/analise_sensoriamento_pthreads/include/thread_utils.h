#ifndef THREAD_UTILS_H
#define THREAD_UTILS_H

#include <pthread.h>
#include <stddef.h>

/**
 * Structure containing data for a single thread's work on a file segment
 */
typedef struct {
    const char *start;  // Pointer to start of this thread's data segment
    size_t size;        // Size of this thread's data segment
    int line_count;     // Number of lines counted by this thread
    const char **line_indices;  // Array of pointers to line starts
    int index_capacity;         // Allocated capacity for line_indices
    int index_count;            // Number of entries in line_indices
} ThreadData;

/**
 * Structure containing resources for thread-based parallel processing
 */
typedef struct {
    pthread_t *threads;              // Array of thread handles
    ThreadData *thread_data;         // Array of thread data structures
    int num_threads;                 // Number of threads
    const char **global_line_index;  // Global array of all line pointers
    int total_lines;                 // Total number of indexed lines
} ThreadResources;

/**
 * Allocates memory for thread resources
 * @param num_threads Number of threads to allocate
 * @return Pointer to allocated ThreadResources or NULL on failure
 */
ThreadResources *allocate_thread_resources(int num_threads);

/**
 * Frees all memory allocated for thread resources
 * @param resources Pointer to ThreadResources to free
 */
void free_thread_resources(ThreadResources *resources);

/**
 * Calculates the block size for a specific thread
 * @param thread_index Thread index (0-based)
 * @param num_threads Total number of threads
 * @param total_size Total size of data to process
 * @return Size in bytes for the thread's block
 */
size_t calculate_block_size(int thread_index, int num_threads,
                            size_t total_size);

/**
 * Initializes thread data for a specific thread
 * @param thread_data Array of thread data structures
 * @param index Index of the thread to initialize
 * @param data Pointer to the start of file data
 * @param block_size Size of the thread's block
 * @param block_offset Offset from start of data to the thread's block
 */
void initialize_thread_data(ThreadData *thread_data, int index,
                            const char *data, size_t block_size,
                            size_t block_offset);

/**
 * Adjusts block boundaries to ensure they align with line breaks
 * @param thread_data Array of thread data structures
 * @param i Current thread index to adjust
 * @param data Pointer to the start of file data
 */
void adjust_block_boundaries(ThreadData *thread_data, int i, const char *data);

/**
 * Identifies duplicate lines that may occur at thread boundaries
 * @param thread_data Array of thread data structures
 * @param num_threads Total number of threads
 * @param data Pointer to the start of file data
 * @param size Total size of data
 * @return Number of duplicate lines found
 */
int correct_duplicate_lines(ThreadData *thread_data, int num_threads,
                            const char *data, size_t size);

/**
 * Starts threads to process file blocks in parallel
 * @param resources Thread resources structure
 * @param data Pointer to the start of file data
 * @param size Total size of data
 * @return 0 on success, -1 on failure
 */
int start_threads(ThreadResources *resources, const char *data, size_t size);

/**
 * Waits for all threads to finish and collects their results
 * @param resources Thread resources structure
 * @return Total line count from all threads
 */
int join_threads_and_collect_results(ThreadResources *resources);

/**
 * Merges thread-local line indices into a global index
 * @param resources Thread resources structure
 * @return Pointer to global line index array or NULL on failure
 */
const char **merge_line_indices(ThreadResources *resources);

/**
 * Removes duplicate line indices from the global index
 * @param line_indices Line indices array
 * @param total_lines Total number of lines in the array
 * @param num_duplicates Expected number of duplicates to remove
 * @return Number of entries actually removed
 */
int remove_duplicate_line_indices(const char **line_indices, int total_lines,
                                  int num_duplicates);

#endif  // THREAD_UTILS_H