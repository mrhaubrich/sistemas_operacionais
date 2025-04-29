#include "../include/file_mapping.h"

#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>  // Adicionado para memchr e memcpy
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

/**
 * Mapeia um arquivo na memória
 * @param filepath Caminho para o arquivo a ser mapeado
 * @return Estrutura MappedFile contendo o mapeamento
 */
MappedFile map_file(const char *filepath) {
    MappedFile result = {NULL, 0, 0, 0};

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

    // Conta linhas simples
    int line_count = 0;
    for (off_t i = 0; i < sb.st_size; i++) {
        if (data[i] == '\n') {
            line_count++;
        }
    }
    // Se o arquivo não termina com uma nova linha, conta a última linha também
    if (sb.st_size > 0 && data[sb.st_size - 1] != '\n') {
        line_count++;
    }
    result.line_count = line_count;

    return result;
}

/**
 * Desmapeia um arquivo previamente mapeado e libera os recursos associados
 * @param csv Ponteiro para a estrutura MappedCSV
 */
void unmap_csv(MappedCSV *csv) {
    if (!csv) return;

    // Free header only if it was allocated (not just pointed to mmap region)
    if (csv->header && csv->mapped_data &&
        (csv->header < (const char *)csv->mapped_data ||
         csv->header >= (const char *)csv->mapped_data + csv->size)) {
        free((void *)csv->header);
    }
    csv->header = NULL;
    csv->data_count = 0;

    // Unmap the original mapped file if possible
    if (csv->mapped_data && csv->size > 0) {
        munmap(csv->mapped_data, csv->size);
        csv->mapped_data = NULL;
    }
    csv->size = 0;
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
    if (!csv || !csv->mapped_data || line_number < 0 ||
        line_number >= csv->data_count) {
        if (line_length) *line_length = 0;
        return NULL;
    }

    // Percorre o arquivo para encontrar a linha solicitada
    const char *data = csv->mapped_data;
    const char *end = data + csv->size;

    // Pula o cabeçalho
    const char *curr = memchr(data, '\n', csv->size);
    if (!curr) {
        if (line_length) *line_length = 0;
        return NULL;  // Arquivo não tem quebra de linha
    }
    curr++;  // Move para depois da quebra de linha do cabeçalho

    // Encontra a linha solicitada
    int current_line = 0;
    while (curr < end && current_line < line_number) {
        const char *next_line = memchr(curr, '\n', end - curr);
        if (!next_line) break;  // Acabou o arquivo
        curr = next_line + 1;
        current_line++;
    }

    if (current_line != line_number) {
        if (line_length) *line_length = 0;
        return NULL;  // Linha não encontrada
    }

    // Encontra o fim da linha atual
    const char *line_end = memchr(curr, '\n', end - curr);
    if (!line_end) line_end = end;  // Última linha sem quebra

    int len = line_end - curr;
    char *result = malloc(len + 1);

    if (!result) {
        if (line_length) *line_length = 0;
        return NULL;
    }

    memcpy(result, curr, len);
    result[len] = '\0';

    if (line_length) *line_length = len;
    return result;
}

/**
 * Mapeia um arquivo CSV na memória e retorna uma estrutura MappedCSV
 * @param filepath Caminho para o arquivo CSV a ser mapeado
 * @return Estrutura MappedCSV contendo o mapeamento
 */
MappedCSV map_csv(const char *filepath) {
    MappedCSV result = {NULL, 0, 0, NULL};

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

    close(fd);

    // Procura o primeiro caractere de nova linha para separar o cabeçalho
    const char *first_newline = memchr(data, '\n', sb.st_size);
    if (!first_newline) {
        munmap(data, sb.st_size);
        fprintf(stderr, "Arquivo CSV inválido: sem cabeçalho\n");
        return result;
    }

    // Calcula o comprimento do cabeçalho (excluindo nova linha)
    size_t header_len = first_newline - data;

    // Cria uma cópia do cabeçalho com terminador nulo
    char *header_copy = malloc(header_len + 1);
    if (!header_copy) {
        perror("Erro ao alocar memória para o cabeçalho");
        munmap(data, sb.st_size);
        return result;
    }

    memcpy(header_copy, data, header_len);
    header_copy[header_len] = '\0';

    // Conta as linhas no arquivo (exceto o cabeçalho)
    int line_count = 0;
    const char *p = first_newline + 1;  // Pula o cabeçalho
    const char *end = data + sb.st_size;

    while (p < end) {
        p = memchr(p, '\n', end - p);
        if (!p) break;  // Não há mais quebras de linha
        line_count++;
        p++;  // Move para depois da quebra de linha
    }

    // Se o arquivo não termina com uma nova linha e há dados após a última
    // quebra
    if (p && p < end) {
        line_count++;
    }

    // Preenche a estrutura de resultado
    result.header = header_copy;
    result.data_count = line_count;
    result.size = sb.st_size;
    result.mapped_data = data;

    return result;
}