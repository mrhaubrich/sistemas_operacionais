#ifndef DATA_ANALYSIS_H
#define DATA_ANALYSIS_H

#include <stddef.h>      // For size_t
#include <stdio.h>       // For FILE operations
#include <sys/socket.h>  // For socket-related functions
#include <sys/un.h>      // For sockaddr_un
#include <unistd.h>      // For fork and exec

#include "file_mapping.h"  // Include for MappedCSV

// Struct to represent a data chunk
typedef struct {
    char *data;   // Pointer to the chunk data
    size_t size;  // Size of the chunk
} DataChunk;

// Struct to represent UDS information
typedef struct {
    char
        uds_path[108];  // Path to the UDS (max length for sockaddr_un.sun_path)
    int socket_fd;      // File descriptor for the UDS
} UDSInfo;

// Function prototypes
/**
 * Particiona o arquivo CSV em pedaços menores.
 * @param csv Estrutura MappedCSV representando o arquivo CSV mapeado.
 * @param chunk_size Número de linhas ou bytes por pedaço.
 * @param chunks Array para armazenar os pedaços resultantes.
 * @param max_chunks Número máximo de pedaços a serem gerados.
 * @return Número de pedaços criados.
 */
int partition_csv(const MappedCSV *csv, size_t chunk_size, DataChunk *chunks,
                  size_t max_chunks);

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

#endif  // DATA_ANALYSIS_H