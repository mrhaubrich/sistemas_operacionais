#ifndef LINE_COUNT_H
#define LINE_COUNT_H

#include <stddef.h>

/**
 * Counts lines sequentially in a memory buffer
 * @param data Pointer to the data buffer
 * @param size Size of the data buffer in bytes
 * @return Number of lines in the buffer
 */
int count_lines_in_memory(const char *data, size_t size);

/**
 * Counts lines in parallel and builds a line index for the given data buffer
 * @param data Pointer to the data buffer
 * @param size Size of the data buffer in bytes
 * @param line_index_ptr Pointer to store the resulting line index
 * @param total_lines_ptr Pointer to store the total number of indexed lines
 * @return Total number of lines counted
 */
int count_lines_in_memory_parallel(const char *data, size_t size,
                                   const char ***line_index_ptr,
                                   int *total_lines_ptr);

/**
 * Thread worker function that counts lines in its assigned portion of the file
 * @param arg Pointer to the ThreadData structure
 * @return NULL
 */
void *count_lines_worker(void *arg);

#endif  // LINE_COUNT_H