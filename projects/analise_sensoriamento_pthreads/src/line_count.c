#include "../include/line_count.h"

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "../include/thread_utils.h"

/**
 * Contar linhas sequencialmente em um buffer de memória
 * @param data Ponteiro para o buffer de dados
 * @param size Tamanho do buffer de dados em bytes
 * @return Número de linhas no buffer
 */
int count_lines_in_memory(const char *data, size_t size) {
    if (!data || size == 0) {
        return 0;
    }

    int line_count = 0;
    const char *p = data;
    const char *end = data + size;

    while (p < end) {
        const char *nl = memchr(p, '\n', end - p);
        if (nl) {
            line_count++;
            p = nl + 1;
        } else {
            if (p < end) {
                line_count++;  // Contar a última linha se não terminar com \n
            }
            break;
        }
    }

    return line_count;
}

/**
 * Função de trabalho da thread que conta linhas em sua porção atribuída do
 * arquivo
 * @param arg Dados da thread contendo o bloco a ser processado
 * @return NULL
 */
void *count_lines_worker(void *arg) {
    ThreadData *data = (ThreadData *)arg;
    if (!data || !data->start) {
        return NULL;
    }

    int local_count = 0;
    const char *p = data->start;
    const char *end = p + data->size;

    // Contar linhas no bloco atribuído
    while (p < end) {
        const char *nl = memchr(p, '\n', end - p);
        if (nl) {
            local_count++;
            p = nl + 1;
        } else {
            // Se não houver mais quebras de linha, mas ainda houver dados, é a
            // última linha
            if (p < end) {
                local_count++;
            }
            break;
        }
    }

    // Armazenar a contagem local no resultado da thread
    data->line_count = local_count;

    return NULL;
}
