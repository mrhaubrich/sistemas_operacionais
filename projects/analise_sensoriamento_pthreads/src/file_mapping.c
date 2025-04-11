#include "../include/file_mapping.h"

#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include "../include/line_count.h"
#include "../include/thread_utils.h"

extern const char **global_line_index;
extern int total_indexed_lines;

MappedFile map_file(const char *filepath) {
    MappedFile result = {NULL, 0, 0, 0, NULL, 0};

    int fd = open(filepath, O_RDONLY);
    if (fd == -1) {
        perror("Erro ao abrir o arquivo para mapeamento");
        return result;
    }

    struct stat sb;
    if (fstat(fd, &sb) == -1) {
        perror("Erro ao obter o tamanho do arquivo");
        close(fd);
        return result;
    }

    char *data = mmap(NULL, sb.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
    if (data == MAP_FAILED) {
        perror("Erro ao mapear o arquivo");
        close(fd);
        return result;
    }

    close(fd);

    result.data = data;
    result.size = sb.st_size;
    result.block_count = sb.st_blocks;
    result.line_count =
        count_lines_in_memory_parallel(data, sb.st_size, sb.st_blocks);

    // Store the global line index in the MappedFile structure
    result.line_indices = global_line_index;
    result.total_indexed_lines = total_indexed_lines;

    // Verify if the line count and indexed lines count match
    if (result.line_count != result.total_indexed_lines &&
        result.line_indices != NULL) {
        printf("Ajustando índice de linhas de %d para %d linhas\n",
               result.total_indexed_lines, result.line_count);

        if (result.line_count < result.total_indexed_lines) {
            // We have more indices than actual lines (due to duplicates)
            int num_duplicates = result.total_indexed_lines - result.line_count;

            // Actually remove the duplicate entries from the array
            int removed = remove_duplicate_line_indices(
                result.line_indices, result.total_indexed_lines,
                num_duplicates);

            // Update the total indexed lines count
            result.total_indexed_lines -= removed;
        }
    }

    return result;
}

void unmap_file(MappedFile *file) {
    if (file && file->data) {
        // Free the line index array
        free(file->line_indices);
        file->line_indices = NULL;
        file->total_indexed_lines = 0;

        // Unmap the file
        munmap(file->data, file->size);
        file->data = NULL;
        file->size = 0;
        file->line_count = 0;
    }
}