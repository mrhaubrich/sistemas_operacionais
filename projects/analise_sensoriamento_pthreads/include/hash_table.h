// filepath:
// /home/marhaubrich/git/sistemas_operacionais/projects/analise_sensoriamento_pthreads/include/hash_table.h
#ifndef HASH_TABLE_H
#define HASH_TABLE_H

#include <stddef.h>

/**
 * Estrutura para armazenar um conjunto de linhas para um único dispositivo
 */
typedef struct DeviceEntry {
    char *device_id;  // ID do dispositivo
    const char *
        *line_indices;  // Array de ponteiros para as linhas deste dispositivo
    int line_count;     // Número de linhas para este dispositivo
    int capacity;       // Capacidade alocada para o array line_indices
    struct DeviceEntry *next;  // Próxima entrada (para resolução de colisão)
} DeviceEntry;

/**
 * Tabela hash para mapear dispositivos para suas linhas
 */
typedef struct {
    DeviceEntry **buckets;  // Array de buckets da tabela hash
    int bucket_count;       // Número de buckets na tabela
    int device_count;       // Número total de dispositivos únicos
    int device_column;      // Índice da coluna que contém o ID do dispositivo
} DeviceHashTable;

/**
 * Cria uma nova tabela hash para dispositivos
 * @param bucket_count Número inicial de buckets
 * @param device_column Índice da coluna que contém o ID do dispositivo
 * @return Nova tabela hash ou NULL em caso de falha
 */
DeviceHashTable *device_hash_table_create(int bucket_count, int device_column);

/**
 * Libera todos os recursos associados à tabela hash
 * @param table A tabela hash a ser liberada
 */
void device_hash_table_destroy(DeviceHashTable *table);

/**
 * Adiciona uma linha ao dispositivo correspondente na tabela hash
 * @param table A tabela hash
 * @param line Ponteiro para a linha do CSV
 * @return 0 em caso de sucesso, -1 em caso de falha
 */
int device_hash_table_add_line(DeviceHashTable *table, const char *line);

/**
 * Obtém todas as linhas associadas a um dispositivo específico
 * @param table A tabela hash
 * @param device_id O ID do dispositivo a ser consultado
 * @param line_count_ptr Ponteiro para armazenar o número de linhas encontradas
 * @return Array de ponteiros para as linhas do dispositivo ou NULL se não
 * encontrado
 */
const char **device_hash_table_get_lines(DeviceHashTable *table,
                                         const char *device_id,
                                         int *line_count_ptr);

/**
 * Enumera todos os IDs de dispositivos únicos na tabela hash
 * @param table A tabela hash
 * @param count_ptr Ponteiro para armazenar o número de dispositivos
 * @return Array de strings contendo os IDs dos dispositivos (deve ser liberado
 * pelo chamador)
 */
char **device_hash_table_get_all_devices(DeviceHashTable *table,
                                         int *count_ptr);

/**
 * Extrai o ID do dispositivo de uma linha CSV
 * @param line A linha do CSV
 * @param device_column O índice da coluna que contém o ID do dispositivo
 * @return String alocada contendo o ID do dispositivo (deve ser liberada pelo
 * chamador)
 */
char *extract_device_id(const char *line, int device_column);

/**
 * Estrutura que estende MappedCSV para incluir a tabela hash de dispositivos
 */
typedef struct {
    const char *header;             // Ponteiro para o cabeçalho do CSV
    DeviceHashTable *device_table;  // Tabela hash de dispositivo -> linhas
    int data_count;                 // Número de linhas no arquivo CSV
    size_t size;                    // Tamanho dos dados mapeados em bytes
    void *mapped_data;              // Ponteiro original para a região mmap
} DeviceMappedCSV;

/**
 * Cria um DeviceMappedCSV a partir de um arquivo CSV
 * @param filepath Caminho para o arquivo CSV
 * @param device_column Índice da coluna que contém o ID do dispositivo
 * @return Estrutura DeviceMappedCSV preenchida ou estrutura zerada em caso de
 * falha
 */
DeviceMappedCSV map_device_csv(const char *filepath, int device_column);

/**
 * Libera os recursos associados a um DeviceMappedCSV
 * @param csv Ponteiro para a estrutura DeviceMappedCSV
 */
void unmap_device_csv(DeviceMappedCSV *csv);

#endif  // HASH_TABLE_H