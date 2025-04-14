#ifndef FILE_MAPPING_H
#define FILE_MAPPING_H

#include <stddef.h>

/**
 * Structure representing a memory-mapped file with line indexing information
 */
typedef struct {
    char *data;                 // Pointer to mapped data
    size_t size;                // Size of mapped data in bytes
    size_t block_count;         // Number of blocks in file
    int line_count;             // Total number of lines in file
    const char **line_indices;  // Array of pointers to each line start
    int total_indexed_lines;    // Number of lines indexed
} MappedFile;

typedef struct {
    const char *header;         // Pointer to the header of the csv
    const char **line_indices;  // Array of pointers to line starts
    int data_count;             // Number of lines in the CSV file
    size_t size;                // Size of mapped data in bytes
} MappedCSV;

/**
 * Maps a file into memory and builds a line index for fast access
 * @param filepath Path to the file to map
 * @return MappedCSV structure containing the mapping and line information
 */
MappedCSV map_csv(const char *filepath);

/**
 * Unmaps a previously mapped file and frees associated resources
 * @param file Pointer to the MappedCSV structure
 */
void unmap_csv(MappedCSV *csv);

/**
 * Gets a line from the mapped CSV by line number (0-based)
 * @param csv Mapped CSV structure
 * @param line_number The 0-based line number to retrieve
 * @param line_length Pointer to store the length of the line (without null
 * terminator)
 * @return Allocated string containing the line (caller must free) or NULL on
 * error
 */
char *get_line(const MappedCSV *csv, int line_number, int *line_length);

#endif  // FILE_MAPPING_H