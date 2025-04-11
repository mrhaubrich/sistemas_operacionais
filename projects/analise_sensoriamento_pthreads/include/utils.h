#ifndef UTILS_H
#define UTILS_H

#include <stdbool.h>

#include "file_mapping.h"

/**
 * Validates that a filename has a .csv extension
 * @param filename The filename to validate
 * @return true if the filename ends with .csv, false otherwise
 */
bool validate_csv_extension(const char *filename);

/**
 * Validates command line arguments
 * @param argc Argument count
 * @param argv Argument vector
 * @return true if arguments are valid, false otherwise
 */
bool validate_args(int argc, char *argv[]);

/**
 * Gets the number of available processors in the system
 * @return Number of available processors
 */
int get_available_number_of_processors(void);

/**
 * Prints the first n lines of a mapped file
 * @param file The mapped file
 * @param n Number of lines to print (if n <= 0, prints all lines)
 */
void print_first_n_lines(MappedFile file, int n);

/**
 * Prints a range of lines from a mapped file
 * @param file The mapped file
 * @param start_line The first line to print (0-based)
 * @param num_lines Number of lines to print
 */
void print_lines_range(MappedFile file, int start_line, int num_lines);

/**
 * Creates a formatted string representation of a memory size
 * @param size Size in bytes
 * @param buffer Buffer to store the result
 * @param buffer_size Size of the buffer
 */
void format_size(size_t size, char *buffer, size_t buffer_size);

/**
 * Prints information about a mapped file
 * @param file The mapped file
 */
void print_file_info(const MappedFile *file);

#endif  // UTILS_H