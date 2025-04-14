#ifndef LINE_COUNT_H
#define LINE_COUNT_H

#include <stddef.h>

/**
 * Conta linhas sequencialmente em um buffer de memória
 * @param data Ponteiro para o buffer de dados
 * @param size Tamanho do buffer de dados em bytes
 * @return Número de linhas no buffer
 */
int count_lines_in_memory(const char *data, size_t size);

/**
 * Conta linhas em paralelo e constrói um índice de linhas para o buffer de
 * dados fornecido
 * @param data Ponteiro para o buffer de dados
 * @param size Tamanho do buffer de dados em bytes
 * @param line_index_ptr Ponteiro para armazenar o índice de linhas resultante
 * @param total_lines_ptr Ponteiro para armazenar o número total de linhas
 * indexadas
 * @return Número total de linhas contadas
 */
int count_lines_in_memory_parallel(const char *data, size_t size,
                                   const char ***line_index_ptr,
                                   int *total_lines_ptr);

/**
 * Função de trabalho da thread que conta linhas em sua porção atribuída do
 * arquivo
 * @param arg Ponteiro para a estrutura ThreadData
 * @return NULL
 */
void *count_lines_worker(void *arg);

#endif  // LINE_COUNT_H