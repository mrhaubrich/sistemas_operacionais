#ifndef DATA_ANALYSIS_H
#define DATA_ANALYSIS_H

#include <stddef.h>      // Para size_t
#include <stdio.h>       // Para operações de FILE
#include <sys/socket.h>  // Para funções relacionadas a socket
#include <sys/un.h>      // Para sockaddr_un
#include <unistd.h>      // Para fork e exec

#include "file_mapping.h"  // Inclusão para MappedCSV
#include "hash_table.h"    // Inclusão para DeviceMappedCSV
#include "thread_safe_queue.h"

// Estrutura para representar informações do UDS
typedef struct {
    char uds_path[108];  // Caminho para o UDS (comprimento máximo para
                         // sockaddr_un.sun_path)
    int socket_fd;       // Descritor de arquivo para o UDS
} UDSInfo;

// Protótipos de funções
/**
 * Particiona o arquivo CSV por dispositivo, otimizando para o número de
 * threads. Os dispositivos são distribuídos entre as threads para balancear o
 * trabalho, mantendo os dados de um mesmo dispositivo juntos.
 * @param csv Estrutura DeviceMappedCSV representando o arquivo CSV mapeado com
 * tabela de dispositivos.
 * @param queue Fila de espera para armazenar os pedaços.
 * @param num_threads Número de threads disponíveis para processamento.
 * @return Número de pedaços criados (um por thread).
 */
int partition_csv_by_device_threaded(const DeviceMappedCSV *csv,
                                     ThreadSafeQueue *queue, int num_threads);

/**
 * Particiona o arquivo CSV em pedaços menores.
 * @param csv Estrutura MappedCSV representando o arquivo CSV mapeado.
 * @param chunk_size Número de linhas por pedaço.
 * @param queue Fila de espera para armazenar os pedaços.
 * @return Número de pedaços criados.
 */
int partition_csv(const MappedCSV *csv, size_t chunk_size,
                  ThreadSafeQueue *queue);

/**
 * Gera um caminho UDS único para um dado ID de fatia.
 * @param slice_id O ID da fatia.
 * @param uds_info Ponteiro para a struct UDSInfo a ser preenchida.
 */
void generate_uds_path(int slice_id, UDSInfo *uds_info);

/**
 * Lança um processo Python para processar um pedaço de dados.
 * @param uds_info Ponteiro para a struct UDSInfo contendo o caminho UDS.
 * @param script_path Caminho para o script Python.
 * @return ID do processo Python lançado.
 */
pid_t launch_python_process(const UDSInfo *uds_info, const char *script_path);

/**
 * Estabelece uma conexão de servidor UDS.
 * @param uds_info Ponteiro para a struct UDSInfo contendo o caminho UDS.
 * @return Descritor de arquivo para o socket do servidor.
 */
int establish_uds_server(const UDSInfo *uds_info);

/**
 * Envia um pedaço de CSV por uma conexão UDS.
 * @param uds_info Ponteiro para a estrutura UDSInfo contendo o caminho UDS.
 * @param queue Ponteiro para a estrutura ThreadSafeQueue contendo os dados do
 * pedaço CSV.
 * @return 0 em caso de sucesso, -1 em caso de falha.
 */
int send_csv_chunk(const UDSInfo *uds_info, const ThreadSafeQueue *queue);

/**
 * Recebe dados processados do CSV por uma conexão UDS.
 * @param uds_info Ponteiro para a estrutura UDSInfo contendo o caminho UDS.
 * @param buffer Ponteiro para um buffer para armazenar os dados recebidos.
 * @param buffer_size Tamanho do buffer.
 * @return Número de bytes recebidos, ou -1 em caso de falha.
 */
int receive_processed_csv(const UDSInfo *uds_info, char *buffer,
                          size_t buffer_size);

/**
 * Limpa o arquivo UDS após a comunicação.
 * @param uds_info Ponteiro para a estrutura UDSInfo contendo o caminho UDS.
 */
void cleanup_uds(const UDSInfo *uds_info);

#endif  // DATA_ANALYSIS_H