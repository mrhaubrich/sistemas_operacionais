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
 * @param ficsvle Ponteiro para a estrutura MappedCSV
 */
void unmap_csv(MappedCSV *csv) {
    if (!csv || !csv->header || !csv->line_indices) {
        return;
    }

    // Libera o array de índice de linhas
    free((void *)csv->header);
    csv->header = NULL;
    csv->line_indices = NULL;
    csv->data_count = 0;

    // Desmapeia o arquivo
    munmap((void *)csv->header, csv->size);
    csv->header = NULL;
    csv->size = 0;
    csv->data_count = 0;
}

/**
 * Obtém uma linha do arquivo CSV mapeado pelo número da linha (baseado em 0)
 * @param csv Estrutura do arquivo CSV mapeado
 * @param line_number O número da linha baseado em 0 para recuperar
 * @param line_length Ponteiro para armazenar o comprimento da linha (sem
 * terminador nulo)
 * @return String alocada contendo a linha (o chamador deve liberar) ou NULL em
 * caso de erro
 */
char *get_line(const MappedCSV *csv, int line_number, int *line_length) {
    if (!csv || !csv->line_indices || line_number < 0 ||
        line_number >= csv->data_count) {
        if (line_length) *line_length = 0;
        return NULL;
    }

    const char *line_start = csv->line_indices[line_number];
    const char *line_end =
        memchr(line_start, '\n', csv->header + csv->data_count - line_start);
    if (!line_end) line_end = csv->header + csv->data_count;

    int len = line_end - line_start;
    char *result = malloc(len + 1);

    if (!result) {
        if (line_length) *line_length = 0;
        return NULL;
    }

    memcpy(result, line_start, len);
    result[len] = '\0';

    if (line_length) *line_length = len;
    return result;
}

/**
 * Mapeia um arquivo CSV na memória e retorna uma estrutura MappedCSV
 * @param filepath Caminho para o arquivo CSV a ser mapeado
 * @return Estrutura MappedCSV contendo o mapeamento e informações de linha
 */
MappedCSV map_csv(const char *filepath) {
    MappedFile temp_result = map_file(filepath);
    MappedCSV result = {NULL, NULL, 0, 0};

    if (temp_result.data == NULL) {
        return result;
    }

    // Define o cabeçalho para apontar para o início do arquivo
    result.header = temp_result.data;

    // Encontra o primeiro caractere de nova linha para separar o cabeçalho dos
    // dados
    const char *first_newline =
        memchr(temp_result.data, '\n', temp_result.size);
    if (first_newline) {
        // Calcula o comprimento do cabeçalho (excluindo nova linha)
        size_t header_len = first_newline - temp_result.data;

        // Cria uma cópia do cabeçalho com terminador nulo
        char *header_copy = malloc(header_len + 1);
        if (header_copy) {
            memcpy(header_copy, temp_result.data, header_len);
            header_copy[header_len] = '\0';
            result.header = header_copy;
        }
    }

    // Ajusta line_indices para excluir o cabeçalho
    result.line_indices =
        temp_result.line_indices + 1;  // Pula a linha do cabeçalho
    result.data_count =
        temp_result.line_count - 1;  // Exclui o cabeçalho da contagem de dados
    result.size = temp_result.size;

    return result;
}