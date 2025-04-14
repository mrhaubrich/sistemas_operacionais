#ifndef DATA_ANALYSIS_H
#define DATA_ANALYSIS_H

#include <stddef.h>      // Para size_t
#include <stdio.h>       // Para operações de FILE
#include <sys/socket.h>  // Para funções relacionadas a socket
#include <sys/un.h>      // Para sockaddr_un
#include <unistd.h>      // Para fork e exec

#include "file_mapping.h"  // Inclusão para MappedCSV
#include "thread_safe_queue.h"

// Estrutura para representar informações do UDS
typedef struct {
    char uds_path[108];  // Caminho para o UDS (comprimento máximo para
                         // sockaddr_un.sun_path)
    int socket_fd;       // Descritor de arquivo para o UDS
} UDSInfo;

// Protótipos de funções
/**
 * Particiona o arquivo CSV em pedaços menores.
 * @param csv Estrutura MappedCSV representando o arquivo CSV mapeado.
 * @param chunk_size Número de linhas ou bytes por pedaço.
 * @param queue Fila de espera para armazenar os pedaços.
 * @param max_chunks Número máximo de pedaços a serem gerados.
 * @return Número de pedaços criados.
 *
 * Implementação: Divida o arquivo CSV em pedaços menores com base no tamanho
 * especificado (chunk_size). Cada pedaço deve ser armazenado na estrutura
 * DataChunk fornecida no array chunks.
 */
int partition_csv(const MappedCSV *csv, size_t chunk_size,
                  ThreadSafeQueue *queue, size_t max_chunks);

/**
 * Gera um caminho UDS único para um dado ID de fatia.
 * @param slice_id O ID da fatia.
 * @param uds_info Ponteiro para a struct UDSInfo a ser preenchida.
 *
 * Implementação: Crie um caminho único para o socket UDS usando o ID da fatia.
 * O caminho deve ser armazenado no campo uds_path da estrutura UDSInfo.
 */
void generate_uds_path(int slice_id, UDSInfo *uds_info);

/**
 * Lança um processo Python para processar um pedaço de dados.
 * @param uds_info Ponteiro para a struct UDSInfo contendo o caminho UDS.
 * @param script_path Caminho para o script Python.
 * @return ID do processo Python lançado.
 *
 * Implementação: Use fork e exec para iniciar um processo Python que executará
 * o script especificado. Passe o caminho UDS como argumento para o script.
 */
pid_t launch_python_process(const UDSInfo *uds_info, const char *script_path);

/**
 * Estabelece uma conexão de servidor UDS.
 * @param uds_info Ponteiro para a struct UDSInfo contendo o caminho UDS.
 * @return Descritor de arquivo para o socket do servidor.
 *
 * Implementação: Crie um socket UDS, vincule-o ao caminho especificado e
 * coloque-o em modo de escuta para aceitar conexões.
 */
int establish_uds_server(const UDSInfo *uds_info);

/**
 * Envia um pedaço de CSV por uma conexão UDS.
 * @param uds_info Ponteiro para a estrutura UDSInfo contendo o caminho UDS.
 * @param chunk Ponteiro para a estrutura DataChunk contendo os dados do pedaço
 * CSV.
 * @return 0 em caso de sucesso, -1 em caso de falha.
 *
 * Implementação: Envie os dados do pedaço CSV para o cliente conectado ao
 * socket UDS. Certifique-se de que todos os dados sejam transmitidos
 * corretamente.
 */
int send_csv_chunk(const UDSInfo *uds_info, const ThreadSafeQueue *queue);

/**
 * Recebe dados processados do CSV por uma conexão UDS.
 * @param uds_info Ponteiro para a estrutura UDSInfo contendo o caminho UDS.
 * @param buffer Ponteiro para um buffer para armazenar os dados recebidos.
 * @param buffer_size Tamanho do buffer.
 * @return Número de bytes recebidos, ou -1 em caso de falha.
 *
 * Implementação: Leia os dados processados enviados pelo cliente conectado ao
 * socket UDS e armazene-os no buffer fornecido.
 */
int receive_processed_csv(const UDSInfo *uds_info, char *buffer,
                          size_t buffer_size);

/**
 * Limpa o arquivo UDS após a comunicação.
 * @param uds_info Ponteiro para a estrutura UDSInfo contendo o caminho UDS.
 *
 * Implementação: Feche o socket UDS e remova o arquivo do sistema de arquivos
 * para evitar acúmulo de arquivos temporários.
 */
void cleanup_uds(const UDSInfo *uds_info);

#endif  // DATA_ANALYSIS_H