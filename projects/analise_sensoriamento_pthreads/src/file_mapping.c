#include "../include/file_mapping.h"

#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>  // Adicionado para memchr e memcpy
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include "../include/line_count.h"
#include "../include/thread_utils.h"

/**
 * Mapeia um arquivo na memória e constrói um índice de linhas para acesso
 * rápido
 * @param filepath Caminho para o arquivo a ser mapeado
 * @return Estrutura MappedFile contendo o mapeamento e informações de linha
 */
MappedFile map_file(const char *filepath) {
    MappedFile result = {NULL, 0, 0, 0, NULL, 0};

    if (!filepath) {
        fprintf(stderr, "Caminho do arquivo inválido\n");
        return result;
    }

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

    if (sb.st_size == 0) {
        fprintf(stderr, "Arquivo está vazio\n");
        close(fd);
        return result;
    }

    char *data = mmap(NULL, sb.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
    if (data == MAP_FAILED) {
        perror("Erro ao mapear o arquivo");
        close(fd);
        return result;
    }

    close(fd);  // Fecha o descritor de arquivo, pois não é mais necessário após
                // o mapeamento

    // Inicializa o resultado com os dados mapeados
    result.data = data;
    result.size = sb.st_size;
    result.block_count = sb.st_blocks;

    // Conta linhas e constrói índice de linhas em paralelo
    const char **line_indices = NULL;
    int total_indexed_lines = 0;

    result.line_count = count_lines_in_memory_parallel(
        data, sb.st_size, &line_indices, &total_indexed_lines);

    // Armazena o índice de linhas na estrutura MappedFile
    result.line_indices = line_indices;
    result.total_indexed_lines = total_indexed_lines;

    // Verifica se a contagem de linhas e o número de linhas indexadas coincidem
    if (result.line_count != result.total_indexed_lines &&
        result.line_indices != NULL) {
        printf("Ajustando índice de linhas de %d para %d linhas\n",
               result.total_indexed_lines, result.line_count);

        if (result.line_count < result.total_indexed_lines) {
            // Temos mais índices do que linhas reais (devido a duplicatas)
            int num_duplicates = result.total_indexed_lines - result.line_count;

            // Remove entradas duplicadas do array
            int removed = remove_duplicate_line_indices(
                result.line_indices, result.total_indexed_lines,
                num_duplicates);

            // Atualiza a contagem total de linhas indexadas
            result.total_indexed_lines -= removed;
        }
    }

    return result;
}

/**
 * Desmapeia um arquivo previamente mapeado e libera os recursos associados
 * @param file Ponteiro para a estrutura MappedFile
 */
void unmap_file(MappedFile *file) {
    if (!file || !file->data) {
        return;
    }

    // Libera o array de índice de linhas
    free(file->line_indices);
    file->line_indices = NULL;
    file->total_indexed_lines = 0;

    // Desmapeia o arquivo
    munmap(file->data, file->size);
    file->data = NULL;
    file->size = 0;
    file->line_count = 0;
    file->block_count = 0;
}

/**
 * Obtém uma linha do arquivo mapeado pelo número da linha (baseado em 0)
 * @param file Estrutura do arquivo mapeado
 * @param line_number O número da linha baseado em 0 para recuperar
 * @param line_length Ponteiro para armazenar o comprimento da linha (sem
 * terminador nulo)
 * @return String alocada contendo a linha (o chamador deve liberar) ou NULL em
 * caso de erro
 */
char *get_line(const MappedFile *file, int line_number, int *line_length) {
    if (!file || !file->data || line_number < 0 ||
        line_number >= file->line_count) {
        if (line_length) *line_length = 0;
        return NULL;
    }

    const char *line_start = NULL;
    const char *line_end = NULL;

    // Caminho rápido: usa o índice de linhas, se disponível
    if (file->line_indices && line_number < file->total_indexed_lines) {
        line_start = file->line_indices[line_number];

        // Encontra o final desta linha (próxima nova linha ou fim do arquivo)
        line_end =
            memchr(line_start, '\n', file->data + file->size - line_start);
        if (!line_end) line_end = file->data + file->size;
    } else {
        // Caminho lento: escaneia o arquivo
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

    // Calcula o comprimento da linha e aloca memória
    int len = line_end - line_start;
    char *result = malloc(len + 1);

    if (!result) {
        if (line_length) *line_length = 0;
        return NULL;
    }

    // Copia o conteúdo da linha
    memcpy(result, line_start, len);
    result[len] = '\0';

    if (line_length) *line_length = len;
    return result;
}