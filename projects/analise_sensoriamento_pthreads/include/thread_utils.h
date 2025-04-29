#ifndef THREAD_UTILS_H
#define THREAD_UTILS_H

#include <pthread.h>
#include <stddef.h>

/**
 * Estrutura contendo dados para o trabalho de uma única thread em um segmento
 * de arquivo
 */
typedef struct {
    const char
        *start;      // Ponteiro para o início do segmento de dados desta thread
    size_t size;     // Tamanho do segmento de dados desta thread
    int line_count;  // Número de linhas contadas por esta thread
} ThreadData;

/**
 * Estrutura contendo recursos para processamento paralelo baseado em threads
 */
typedef struct {
    pthread_t *threads;       // Array de identificadores de threads
    ThreadData *thread_data;  // Array de estruturas de dados de thread
    int num_threads;          // Número de threads
    int total_lines;          // Número total de linhas contadas
} ThreadResources;

/**
 * Aloca memória para recursos de threads
 * @param num_threads Número de threads a serem alocadas
 * @return Ponteiro para ThreadResources alocado ou NULL em caso de falha
 */
ThreadResources *allocate_thread_resources(int num_threads);

/**
 * Libera toda a memória alocada para recursos de threads
 * @param resources Ponteiro para ThreadResources a ser liberado
 */
void free_thread_resources(ThreadResources *resources);

/**
 * Calcula o tamanho do bloco para uma thread específica
 * @param thread_index Índice da thread (base 0)
 * @param num_threads Número total de threads
 * @param total_size Tamanho total dos dados a serem processados
 * @return Tamanho em bytes do bloco da thread
 */
size_t calculate_block_size(int thread_index, int num_threads,
                            size_t total_size);

/**
 * Inicializa dados de thread para uma thread específica
 * @param thread_data Array de estruturas de dados de thread
 * @param index Índice da thread a ser inicializada
 * @param data Ponteiro para o início dos dados do arquivo
 * @param block_size Tamanho do bloco da thread
 * @param block_offset Offset do início dos dados para o bloco da thread
 */
void initialize_thread_data(ThreadData *thread_data, int index,
                            const char *data, size_t block_size,
                            size_t block_offset);

/**
 * Ajusta os limites do bloco para garantir que estejam alinhados com quebras de
 * linha
 * @param thread_data Array de estruturas de dados de thread
 * @param i Índice atual da thread a ser ajustado
 * @param data Ponteiro para o início dos dados do arquivo
 */
void adjust_block_boundaries(ThreadData *thread_data, int i, const char *data);

/**
 * Identifica linhas duplicadas que podem ocorrer nos limites das threads
 * @param thread_data Array de estruturas de dados de thread
 * @param num_threads Número total de threads
 * @param data Ponteiro para o início dos dados do arquivo
 * @param size Tamanho total dos dados
 * @return Número de linhas duplicadas encontradas
 */
int correct_duplicate_lines(ThreadData *thread_data, int num_threads,
                            const char *data, size_t size);

/**
 * Inicia threads para processar blocos de arquivo em paralelo
 * @param resources Estrutura de recursos de threads
 * @param data Ponteiro para o início dos dados do arquivo
 * @param size Tamanho total dos dados
 * @return 0 em caso de sucesso, -1 em caso de falha
 */
int start_threads(ThreadResources *resources, const char *data, size_t size);

/**
 * Aguarda todas as threads terminarem e coleta seus resultados
 * @param resources Estrutura de recursos de threads
 * @return Contagem total de linhas de todas as threads
 */
int join_threads_and_collect_results(ThreadResources *resources);

#endif  // THREAD_UTILS_H