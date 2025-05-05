#ifndef FILE_MAPPING_H
#define FILE_MAPPING_H

#include <stddef.h>

/**
 * Estrutura representando um arquivo mapeado na memória
 */
typedef struct {
    char *data;          // Ponteiro para os dados mapeados
    size_t size;         // Tamanho dos dados mapeados em bytes
    size_t block_count;  // Número de blocos no arquivo
    int line_count;      // Número total de linhas no arquivo
} MappedFile;

typedef struct {
    const char *header;  // Ponteiro para o cabeçalho do csv
    int data_count;      // Número de linhas no arquivo CSV
    size_t size;         // Tamanho dos dados mapeados em bytes
    void *mapped_data;   // Ponteiro original para a região mmap
} MappedCSV;

/**
 * Mapeia um arquivo na memória
 * @param filepath Caminho para o arquivo a ser mapeado
 * @return Estrutura MappedCSV contendo o mapeamento
 */
MappedCSV map_csv(const char *filepath);

/**
 * Desmapeia um arquivo previamente mapeado e libera os recursos associados
 * @param csv Ponteiro para a estrutura MappedCSV
 */
void unmap_csv(MappedCSV *csv);

/**
 * Obtém uma linha do arquivo CSV mapeado pelo número da linha (baseado em 0)
 * @param csv Estrutura do arquivo CSV mapeado
 * @param line_number O número da linha baseado em 0 para recuperar
 * @param line_length Ponteiro para armazenar o comprimento da linha (sem
 * terminador nulo)
 * @return String alocada contendo a linha (o chamador deve liberar) ou NULL em
 * caso de erro
 */
char *get_line(const MappedCSV *csv, int line_number, int *line_length);

#endif  // FILE_MAPPING_H