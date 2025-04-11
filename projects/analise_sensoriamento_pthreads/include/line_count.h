#ifndef LINE_COUNT_H
#define LINE_COUNT_H

#include <stddef.h>

int count_lines_in_memory(const char *data, size_t size);
int count_lines_in_memory_parallel(const char *data, size_t size,
                                   size_t block_count);
/**
 * Função executada pelas threads para contar linhas em um bloco específico
 * @param arg Ponteiro para a estrutura ThreadData
 * @return NULL
 */
void *count_lines_worker(void *arg);

#endif  // LINE_COUNT_H