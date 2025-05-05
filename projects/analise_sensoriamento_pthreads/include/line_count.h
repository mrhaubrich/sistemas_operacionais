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
 * Função de trabalho da thread que conta linhas em sua porção atribuída do
 * arquivo
 * @param arg Ponteiro para a estrutura ThreadData
 * @return NULL
 */
void *count_lines_worker(void *arg);

#endif  // LINE_COUNT_H