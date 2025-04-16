#ifndef CSV_DATA_H
#define CSV_DATA_H

#include <stdint.h>
#include <sys/types.h>

typedef struct {
    uint32_t start_offset;  // Offset from base pointer (not a direct pointer)
    uint32_t end_offset;    // Offset one past the last character
} CSVField;

typedef struct {
    CSVField device;
    CSVField data;
    CSVField temperatura;
    CSVField umidade;
    CSVField luminosidade;
    CSVField ruido;
    CSVField eco2;
    CSVField etvoc;
} CSVLine;

// Struct representing the whole CSV file, with header as a CSVField.
typedef struct {
    const char *base;   // Base pointer to mmap'd data
    CSVField header;    // Header row as a CSVField (offsets)
    CSVLine *lines;     // Array of CSVLine structs (one per data row)
    int line_count;     // Number of data lines (not counting header)
    void *mapped_data;  // Pointer to original mapped memory (for cleanup)
    size_t size;        // Size of mapped data in bytes
} CSVFile;

// Map a CSV file into memory and parse into CSVFile structure
CSVFile csvfile_map(const char *filepath);

// Unmap and free all resources associated with a CSVFile
void csvfile_unmap(CSVFile *csv);

// Get the number of data lines (not counting header)
static inline int csvfile_line_count(const CSVFile *csv) {
    return csv ? csv->line_count : 0;
}

// Get a CSVLine by index (0-based, not counting header), returns NULL if out of
// bounds
const CSVLine *csvfile_get_line(const CSVFile *csv, int line_number);

// Copy a CSVField to a null-terminated string (caller must free)
char *csvfield_to_string(const CSVFile *csv, const CSVField *field);

// Print a range of lines from the CSVFile (for debugging)
void csvfile_print_lines(const CSVFile *csv, int start_line, int num_lines);

// Print the header as a string (for debugging)
void csvfile_print_header(const CSVFile *csv);

// Print info about the CSVFile (size, line count, etc.)
void csvfile_print_info(const CSVFile *csv);

#endif  // CSV_DATA_H