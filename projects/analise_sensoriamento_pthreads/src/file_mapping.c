#include "../include/file_mapping.h"

#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include "../include/line_count.h"

MappedFile map_file(const char *filepath) {
    MappedFile result = {NULL, 0, 0};

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
    return result;
}

void unmap_file(MappedFile *file) {
    if (file && file->data) {
        munmap(file->data, file->size);
        file->data = NULL;
        file->size = 0;
        file->line_count = 0;
    }
}