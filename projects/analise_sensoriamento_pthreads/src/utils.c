#include "../include/utils.h"

#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/sysinfo.h>

/**
 * Validates that a filename has a .csv extension
 * @param filename The filename to validate
 * @return true if the filename ends with .csv, false otherwise
 */
bool validate_csv_extension(const char *filename) {
    if (!filename) {
        fprintf(stderr, "Error: Invalid filename (NULL)\n");
        return false;
    }

    const char *dot = strrchr(filename, '.');
    const bool is_valid = dot && strcmp(dot, ".csv") == 0;

    if (!is_valid) {
        fprintf(stderr, "Error: File must have .csv extension\n");
    }
    return is_valid;
}

/**
 * Validates command line arguments
 * @param argc Argument count
 * @param argv Argument vector
 * @return true if arguments are valid, false otherwise
 */
bool validate_args(int argc, char *argv[]) {
    const bool is_valid = argc == 2;

    if (!is_valid) {
        fprintf(stderr, "Usage: %s <path_to_csv_file>\n", argv[0]);
    }
    return is_valid;
}

/**
 * Gets the number of available processors in the system
 * @return Number of available processors
 */
int get_available_number_of_processors(void) {
    int nprocs = get_nprocs();
    return nprocs > 0 ? nprocs : 1;  // Always return at least 1
}

/**
 * Prints a range of lines from a mapped file
 * @param file The mapped file
 * @param start_line The first line to print (0-based)
 * @param num_lines Number of lines to print
 */
void print_lines_range(MappedFile file, int start_line, int num_lines) {
    if (file.data == NULL || file.size == 0) {
        printf("No data to display.\n");
        return;
    }

    if (start_line < 0) start_line = 0;

    // Calculate how many lines to print
    int total_lines = file.line_count;
    int end_line = start_line + num_lines;
    if (end_line > total_lines) end_line = total_lines;

    int lines_to_print = end_line - start_line;

    if (lines_to_print <= 0) {
        printf("No lines to display in the specified range.\n");
        return;
    }

    printf("Displaying lines %d to %d (total lines: %d)\n", start_line + 1,
           end_line, total_lines);

    // Use our enhanced get_line function for retrieving lines
    for (int i = start_line; i < end_line; i++) {
        int line_length = 0;
        char *line = get_line(&file, i, &line_length);

        if (line) {
            printf("Line %d: %s\n", i + 1, line);
            free(line);
        } else {
            printf("Line %d: <error retrieving line>\n", i + 1);
        }
    }

    if (end_line < total_lines) {
        printf("... (%d additional lines not displayed)\n",
               total_lines - end_line);
    }
}

/**
 * Prints the first n lines of a mapped file
 * @param file The mapped file
 * @param n Number of lines to print (if n <= 0, prints all lines)
 */
void print_first_n_lines(MappedFile file, int n) {
    print_lines_range(file, 0, n <= 0 ? file.line_count : n);
}

/**
 * Creates a formatted string representation of a memory size
 * @param size Size in bytes
 * @param buffer Buffer to store the result
 * @param buffer_size Size of the buffer
 */
void format_size(size_t size, char *buffer, size_t buffer_size) {
    if (size < 1024) {
        snprintf(buffer, buffer_size, "%zu bytes", size);
    } else if (size < 1024 * 1024) {
        snprintf(buffer, buffer_size, "%.2f KB", size / 1024.0);
    } else if (size < 1024 * 1024 * 1024) {
        snprintf(buffer, buffer_size, "%.2f MB", size / (1024.0 * 1024.0));
    } else {
        snprintf(buffer, buffer_size, "%.2f GB",
                 size / (1024.0 * 1024.0 * 1024.0));
    }
}

/**
 * Prints information about a mapped file
 * @param file The mapped file
 */
void print_file_info(const MappedFile *file) {
    if (!file || !file->data) {
        printf("File not mapped or invalid\n");
        return;
    }

    char size_str[32];
    format_size(file->size, size_str, sizeof(size_str));

    printf("File Information:\n");
    printf("- Size: %s (%zu bytes)\n", size_str, file->size);
    printf("- Lines: %d\n", file->line_count);
    printf("- Indexed lines: %d\n", file->total_indexed_lines);
    printf("- Block count: %zu\n", file->block_count);

    // For efficiency analysis
    if (file->line_indices) {
        printf("- Line indexing: Available (fast access)\n");
    } else {
        printf("- Line indexing: Not available (sequential scan)\n");
    }
}
