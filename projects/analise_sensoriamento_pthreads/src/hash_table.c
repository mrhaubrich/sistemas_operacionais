// filepath:
// /home/marhaubrich/git/sistemas_operacionais/projects/analise_sensoriamento_pthreads/src/hash_table.c
#include "../include/hash_table.h"

#include <fcntl.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <unistd.h>

// Número inicial de buckets da tabela hash (deve ser primo para melhor
// distribuição) Aumento do número de buckets para melhor distribuição com
// grandes volumes de dados
#define DEFAULT_HASH_BUCKET_COUNT 10007
#define INITIAL_LINES_CAPACITY 256

// Fator de carga máximo antes do redimensionamento
#define MAX_LOAD_FACTOR 0.75

// Mutex para sincronização de acesso à tabela hash durante construção
// multithreaded
static pthread_mutex_t hash_mutex = PTHREAD_MUTEX_INITIALIZER;

/**
 * Função de hash simples para strings
 * Implementação da função FNV-1a (Fowler-Noll-Vo)
 */
static unsigned int hash_string(const char *str) {
    unsigned int hash = 2166136261u;  // Valor inicial FNV offsets

    while (*str) {
        hash ^= (unsigned int)*str++;
        hash *= 16777619;  // Primo FNV
    }

    return hash;
}

/**
 * Cria uma nova entrada para um dispositivo
 */
static DeviceEntry *create_device_entry(const char *device_id) {
    if (!device_id) return NULL;

    DeviceEntry *entry = malloc(sizeof(DeviceEntry));
    if (!entry) return NULL;

    entry->device_id = strdup(device_id);
    if (!entry->device_id) {
        free(entry);
        return NULL;
    }

    entry->capacity = INITIAL_LINES_CAPACITY;
    entry->lines = malloc(sizeof(const char *) * entry->capacity);
    if (!entry->lines) {
        free(entry->device_id);
        free(entry);
        return NULL;
    }

    entry->line_count = 0;
    entry->next = NULL;

    return entry;
}

/**
 * Libera todos os recursos de uma entrada de dispositivo
 */
static void free_device_entry(DeviceEntry *entry) {
    if (!entry) return;

    free(entry->device_id);
    free(entry->lines);
    free(entry);
}

/**
 * Cria uma nova tabela hash para dispositivos
 */
DeviceHashTable *device_hash_table_create(int bucket_count, int device_column) {
    if (bucket_count <= 0) bucket_count = DEFAULT_HASH_BUCKET_COUNT;
    if (device_column < 0) return NULL;

    DeviceHashTable *table = malloc(sizeof(DeviceHashTable));
    if (!table) return NULL;

    table->buckets = calloc(bucket_count, sizeof(DeviceEntry *));
    if (!table->buckets) {
        free(table);
        return NULL;
    }

    table->bucket_count = bucket_count;
    table->device_count = 0;
    table->device_column = device_column;

    return table;
}

/**
 * Libera todos os recursos associados à tabela hash
 */
void device_hash_table_destroy(DeviceHashTable *table) {
    if (!table) return;

    for (int i = 0; i < table->bucket_count; i++) {
        DeviceEntry *entry = table->buckets[i];
        while (entry) {
            DeviceEntry *next = entry->next;
            free_device_entry(entry);
            entry = next;
        }
    }

    free(table->buckets);
    free(table);
}

/**
 * Extrai o ID do dispositivo de uma linha CSV
 */
char *extract_device_id(const char *line, int device_column) {
    if (!line || device_column < 0) return NULL;

    // Procura a coluna correta do dispositivo
    const char *p = line;
    int column = 0;

    // Pular até a coluna desejada
    while (column < device_column) {
        p = strchr(p, '|');   // Pipe como delimitador CSV
        if (!p) return NULL;  // Não há coluna suficiente
        p++;                  // Avança depois do delimitador
        column++;
    }

    // Encontra o próximo delimitador ou fim da linha
    const char *end = strchr(p, '|');
    if (!end) {
        // Se não encontrar outro delimitador, vai até o fim da linha
        end = strchr(p, '\n');
        if (!end) end = p + strlen(p);
    }

    // Calcula o comprimento do ID do dispositivo
    size_t len = end - p;

    // Aloca e copia o ID do dispositivo
    char *device_id = malloc(len + 1);
    if (!device_id) return NULL;

    memcpy(device_id, p, len);
    device_id[len] = '\0';

    return device_id;
}

/**
 * Adiciona uma linha ao dispositivo correspondente na tabela hash
 */
int device_hash_table_add_line(DeviceHashTable *table, const char *line) {
    if (!table || !line) return -1;

    // Extrai o ID do dispositivo desta linha
    char *device_id = extract_device_id(line, table->device_column);
    if (!device_id) return -1;

    // Calcula o hash do ID do dispositivo
    unsigned int hash_value = hash_string(device_id) % table->bucket_count;

    pthread_mutex_lock(&hash_mutex);

    // Procura se este dispositivo já existe na tabela
    DeviceEntry *entry = table->buckets[hash_value];
    while (entry) {
        if (strcmp(entry->device_id, device_id) == 0) break;
        entry = entry->next;
    }

    // Se não encontrou, cria uma nova entrada
    if (!entry) {
        entry = create_device_entry(device_id);
        if (!entry) {
            pthread_mutex_unlock(&hash_mutex);
            free(device_id);
            return -1;
        }

        // Insere a nova entrada no início da lista encadeada deste bucket
        entry->next = table->buckets[hash_value];
        table->buckets[hash_value] = entry;
        table->device_count++;
    }

    // Verifica se precisa expandir o array de linhas
    if (entry->line_count >= entry->capacity) {
        int new_capacity = entry->capacity * 2;
        const char **new_line_indices =
            realloc(entry->lines, sizeof(const char *) * new_capacity);
        if (!new_line_indices) {
            pthread_mutex_unlock(&hash_mutex);
            free(device_id);
            return -1;
        }

        entry->lines = new_line_indices;
        entry->capacity = new_capacity;
    }

    // Adiciona a linha aos índices deste dispositivo
    entry->lines[entry->line_count++] = line;

    pthread_mutex_unlock(&hash_mutex);
    free(device_id);

    return 0;
}

/**
 * Obtém todas as linhas associadas a um dispositivo específico
 */
const char **device_hash_table_get_lines(DeviceHashTable *table,
                                         const char *device_id,
                                         int *line_count_ptr) {
    if (!table || !device_id || !line_count_ptr) {
        if (line_count_ptr) *line_count_ptr = 0;
        return NULL;
    }

    // Calcula o hash do ID do dispositivo
    unsigned int hash_value = hash_string(device_id) % table->bucket_count;

    // Procura o dispositivo na tabela
    DeviceEntry *entry = table->buckets[hash_value];
    while (entry) {
        if (strcmp(entry->device_id, device_id) == 0) {
            *line_count_ptr = entry->line_count;
            return entry->lines;
        }
        entry = entry->next;
    }

    // Dispositivo não encontrado
    *line_count_ptr = 0;
    return NULL;
}

/**
 * Enumera todos os IDs de dispositivos únicos na tabela hash
 */
char **device_hash_table_get_all_devices(DeviceHashTable *table,
                                         int *count_ptr) {
    if (!table || !count_ptr) {
        if (count_ptr) *count_ptr = 0;
        return NULL;
    }

    // Aloca espaço para todos os dispositivos
    char **result = malloc(sizeof(char *) * table->device_count);
    if (!result) {
        *count_ptr = 0;
        return NULL;
    }

    int count = 0;

    // Percorre todos os buckets
    for (int i = 0; i < table->bucket_count; i++) {
        DeviceEntry *entry = table->buckets[i];
        while (entry && count < table->device_count) {
            result[count] = strdup(entry->device_id);
            if (!result[count]) {
                // Em caso de erro, libera tudo que foi alocado até agora
                for (int j = 0; j < count; j++) {
                    free(result[j]);
                }
                free(result);
                *count_ptr = 0;
                return NULL;
            }

            count++;
            entry = entry->next;
        }
    }

    *count_ptr = count;
    return result;
}

/**
 * Cria um DeviceMappedCSV a partir de um arquivo CSV
 */
DeviceMappedCSV map_device_csv(const char *filepath, int device_column) {
    DeviceMappedCSV result = {NULL, NULL, 0, 0, NULL};
    struct timeval start_time;

    if (!filepath || device_column < 0) {
        fprintf(
            stderr,
            "Caminho do arquivo inválido ou coluna de dispositivo inválida\n");
        return result;
    }

    int fd = open(filepath, O_RDONLY);
    if (fd == -1) {
        perror("Erro ao abrir o arquivo para mapeamento");
        return result;
    }

    struct stat sb;
    if (fstat(fd, &sb) == -1) {
        perror("Erro ao obter o tamanho do arquivo");
        close(fd);
        return result;
    }

    if (sb.st_size == 0) {
        fprintf(stderr, "Arquivo está vazio\n");
        close(fd);
        return result;
    }

    char *data = mmap(NULL, sb.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
    if (data == MAP_FAILED) {
        perror("Erro ao mapear o arquivo");
        close(fd);
        return result;
    }

    close(fd);  // Fecha o descritor de arquivo após o mapeamento

    // Reinicia o timer para medir a extração do cabeçalho
    gettimeofday(&start_time, NULL);

    // Procura a primeira quebra de linha para separar o cabeçalho
    const char *header_end = strchr(data, '\n');
    if (!header_end) {
        munmap(data, sb.st_size);
        fprintf(stderr, "Arquivo CSV inválido: sem cabeçalho\n");
        return result;
    }

    // Cria uma cópia do cabeçalho
    size_t header_len = header_end - data;
    char *header_copy = malloc(header_len + 1);
    if (!header_copy) {
        perror("Erro ao alocar memória para o cabeçalho");
        munmap(data, sb.st_size);
        return result;
    }

    memcpy(header_copy, data, header_len);
    header_copy[header_len] = '\0';

    // Inicializa a tabela hash de dispositivos com tamanho maior para arquivos
    // grandes
    size_t estimated_lines =
        sb.st_size / 100;  // Estimativa grosseira de linhas
    int bucket_count =
        (estimated_lines > 1000000)
            ? 100003
            : DEFAULT_HASH_BUCKET_COUNT;  // Bucket size maior para arquivos
                                          // grandes

    DeviceHashTable *device_table =
        device_hash_table_create(bucket_count, device_column);
    if (!device_table) {
        free(header_copy);
        munmap(data, sb.st_size);
        fprintf(stderr, "Falha ao criar tabela hash de dispositivos\n");
        return result;
    }

    // Inicia do primeiro caractere após o final do cabeçalho (após \n)
    const char *curr = header_end + 1;
    const char *end = data + sb.st_size;
    int line_count = 0;
    int lines_processed = 0;

    // Processa todas as linhas diretamente para construir a tabela hash
    while (curr < end) {
        // Encontra o fim da linha atual
        const char *line_end = strchr(curr, '\n');
        if (!line_end) {
            // Se não encontrar \n, use o fim do arquivo
            line_end = end;
        }

        // Linha vazia, pula
        if (line_end > curr) {
            line_count++;

            // Adiciona a linha na tabela hash
            // Note: não precisamos duplicar a linha, apenas apontamos para ela
            // na memória mapeada
            device_hash_table_add_line(device_table, curr);

            // Mostra progresso a cada 1 milhão de linhas
            lines_processed++;
        }

        // Avança para a próxima linha (depois do \n)
        curr = line_end + 1;

        // Se chegamos ao fim do arquivo, saímos do loop
        if (line_end == end) {
            break;
        }
    }

    // Preenche a estrutura de resultado
    result.header = header_copy;
    result.device_table = device_table;
    result.data_count = line_count;
    result.size = sb.st_size;
    result.mapped_data = data;

    return result;
}

/**
 * Libera os recursos associados a um DeviceMappedCSV
 */
void unmap_device_csv(DeviceMappedCSV *csv) {
    if (!csv) return;

    // Libera o cabeçalho se não for parte da região mapeada
    if (csv->header) {
        free((void *)csv->header);
        csv->header = NULL;
    }

    // Libera a tabela hash de dispositivos
    if (csv->device_table) {
        device_hash_table_destroy(csv->device_table);
        csv->device_table = NULL;
    }

    // Desmapeia a região mapeada
    if (csv->mapped_data && csv->size > 0) {
        munmap(csv->mapped_data, csv->size);
        csv->mapped_data = NULL;
    }

    csv->data_count = 0;
    csv->size = 0;
}