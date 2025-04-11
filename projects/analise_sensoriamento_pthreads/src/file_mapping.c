#include "../include/file_mapping.h"

#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>  // Added for memchr and memcpy
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include "../include/line_count.h"
#include "../include/thread_utils.h"

/**
 * Maps a file into memory and builds a line index for fast access
 * @param filepath Path to the file to map
 * @return MappedFile structure containing the mapping and line information
 */
MappedFile map_file(const char *filepath) {
    MappedFile result = {NULL, 0, 0, 0, NULL, 0};

    if (!filepath) {
        fprintf(stderr, "Invalid file path\n");
        return result;
    }

    int fd = open(filepath, O_RDONLY);
    if (fd == -1) {
        perror("Error opening file for mapping");
        return result;
    }

    struct stat sb;
    if (fstat(fd, &sb) == -1) {
        perror("Error getting file size");
        close(fd);
        return result;
    }

    if (sb.st_size == 0) {
        fprintf(stderr, "File is empty\n");
        close(fd);
        return result;
    }

    char *data = mmap(NULL, sb.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
    if (data == MAP_FAILED) {
        perror("Error mapping file");
        close(fd);
        return result;
    }

    close(fd);  // Close file descriptor as it's no longer needed after mapping

    // Initialize the result with the mapped data
    result.data = data;
    result.size = sb.st_size;
    result.block_count = sb.st_blocks;

    // Count lines and build line index in parallel
    const char **line_indices = NULL;
    int total_indexed_lines = 0;

    result.line_count = count_lines_in_memory_parallel(
        data, sb.st_size, &line_indices, &total_indexed_lines);

    // Store the line index in the MappedFile structure
    result.line_indices = line_indices;
    result.total_indexed_lines = total_indexed_lines;

    // Verify if the line count and indexed lines count match
    if (result.line_count != result.total_indexed_lines &&
        result.line_indices != NULL) {
        printf("Adjusting line index from %d to %d lines\n",
               result.total_indexed_lines, result.line_count);

        if (result.line_count < result.total_indexed_lines) {
            // We have more indices than actual lines (due to duplicates)
            int num_duplicates = result.total_indexed_lines - result.line_count;

            // Remove duplicate entries from the array
            int removed = remove_duplicate_line_indices(
                result.line_indices, result.total_indexed_lines,
                num_duplicates);

            // Update the total indexed lines count
            result.total_indexed_lines -= removed;
        }
    }

    return result;
}

/**
 * Unmaps a previously mapped file and frees associated resources
 * @param file Pointer to the MappedFile structure
 */
void unmap_file(MappedFile *file) {
    if (!file || !file->data) {
        return;
    }

    // Free the line index array
    free(file->line_indices);
    file->line_indices = NULL;
    file->total_indexed_lines = 0;

    // Unmap the file
    munmap(file->data, file->size);
    file->data = NULL;
    file->size = 0;
    file->line_count = 0;
    file->block_count = 0;
}

/**
 * Gets a line from the mapped file by line number (0-based)
 * @param file Mapped file structure
 * @param line_number The 0-based line number to retrieve
 * @param line_length Pointer to store the length of the line (without null
 * terminator)
 * @return Allocated string containing the line (caller must free) or NULL on
 * error
 */
char *get_line(const MappedFile *file, int line_number, int *line_length) {
    if (!file || !file->data || line_number < 0 ||
        line_number >= file->line_count) {
        if (line_length) *line_length = 0;
        return NULL;
    }

    const char *line_start = NULL;
    const char *line_end = NULL;

    // Fast path: use the line index if available
    if (file->line_indices && line_number < file->total_indexed_lines) {
        line_start = file->line_indices[line_number];

        // Find the end of this line (next newline or end of file)
        line_end =
            memchr(line_start, '\n', file->data + file->size - line_start);
        if (!line_end) line_end = file->data + file->size;
    } else {
        // Slow path: scan through the file
        const char *p = file->data;
        const char *end = file->data + file->size;
        int current_line = 0;

        while (p < end && current_line < line_number) {
            const char *nl = memchr(p, '\n', end - p);
            if (nl) {
                current_line++;
                p = nl + 1;
            } else {
                break;
            }
        }

        if (current_line != line_number) {
            if (line_length) *line_length = 0;
            return NULL;
        }

        line_start = p;
        line_end = memchr(p, '\n', end - p);
        if (!line_end) line_end = end;
    }

    // Calculate line length and allocate memory
    int len = line_end - line_start;
    char *result = malloc(len + 1);

    if (!result) {
        if (line_length) *line_length = 0;
        return NULL;
    }

    // Copy the line contents
    memcpy(result, line_start, len);
    result[len] = '\0';

    if (line_length) *line_length = len;
    return result;
}