#ifndef FILE_MAPPING_H
#define FILE_MAPPING_H

#include <stddef.h>

typedef struct {
    char *data;
    size_t size;
    size_t block_count;
    int line_count;
} MappedFile;

MappedFile map_file(const char *filepath);
void unmap_file(MappedFile *file);

#endif  // FILE_MAPPING_H