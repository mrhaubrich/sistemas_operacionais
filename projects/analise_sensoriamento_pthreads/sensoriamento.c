#include <fcntl.h>  // Para flags de abertura de arquivos
#include <pthread.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>  // Para mapeamento de memória
#include <sys/stat.h>  // Para estatísticas de arquivos
#include <sys/sysinfo.h>
#include <unistd.h>  // Para close()

#define SEPARATOR '|'
#define EXPECTED_HEADERS \
    'id|device|contagem|data|temperatura|umidade|luminosidade|ruido|eco2|etvoc|latitude|longitude'
#define BUFFER_SIZE \
    65536  // Buffer de 64 KB (será usado para chunks em multithread)

// Estrutura para armazenar o arquivo mapeado em memória
typedef struct {
    char *data;          // Ponteiro para os dados mapeados
    size_t size;         // Tamanho dos dados mapeados
    size_t block_count;  // Total de blocos no arquivo
    int line_count;      // Total de linhas no arquivo
} MappedFile;

int get_available_number_of_processors(void);
bool validate_csv_extension(const char *filename);
bool validate_args(int argc, char *argv[]);
MappedFile map_file(const char *filepath);
void unmap_file(MappedFile *file);
void print_first_n_lines(MappedFile file, int n);
char *get_line_at_offset(MappedFile file, size_t *offset);
int count_lines_in_memory(const char *data, size_t size);
int count_lines_in_memory_parallel(const char *data, size_t size,
                                   size_t block_count);

// Estrutura de dados para passar informações às threads
typedef struct {
    const char *start;  // Ponteiro para o início do bloco
    size_t size;        // Tamanho do bloco a processar
    int line_count;     // Contagem local de linhas para este bloco
} ThreadData;

// Mutex para proteção do contador compartilhado
pthread_mutex_t line_count_mutex;

int main(int argc, char *argv[]) {
    if (!validate_args(argc, argv)) {
        return EXIT_FAILURE;
    }

    const char *filepath = argv[1];

    if (!validate_csv_extension(filepath)) {
        return EXIT_FAILURE;
    }

    int num_processors = get_available_number_of_processors();
    printf("Processadores disponíveis: %d\n", num_processors);

    // Mapeia o arquivo para a memória
    MappedFile mfile = map_file(filepath);
    if (mfile.data == NULL) {
        fprintf(stderr, "Falha ao mapear o arquivo\n");
        return EXIT_FAILURE;
    }

    printf("Arquivo mapeado com sucesso: %zu bytes\n", mfile.size);
    printf("Total de linhas no arquivo: %d\n", mfile.line_count);

    // Imprime as primeiras 10 linhas para verificação
    print_first_n_lines(mfile, 10);

    // Aqui você pode começar a montar a lógica de dividir o trabalho com
    // pthreads. Por exemplo: contar linhas, dividir range entre threads, etc.

    // Limpeza
    unmap_file(&mfile);
    return EXIT_SUCCESS;
}

int get_available_number_of_processors(void) { return get_nprocs(); }

bool validate_csv_extension(const char *filename) {
    const char *dot = strrchr(filename, '.');
    const bool is_valid = dot && strcmp(dot, ".csv") == 0;
    if (!is_valid) {
        fprintf(stderr, "Erro: O arquivo deve ter extensão .csv\n");
    }
    return is_valid;
}

bool validate_args(int argc, char *argv[]) {
    const bool is_valid = argc == 2;
    if (!is_valid) {
        fprintf(stderr, "Uso: %s <caminho_para_arquivo.csv>\n", argv[0]);
    }
    return is_valid;
}

/**
 * Conta o número de linhas em um bloco de memória
 * @param data Ponteiro para o início do bloco de memória
 * @param size Tamanho do bloco de memória
 * @return Número de linhas no bloco
 */
int count_lines_in_memory(const char *data, size_t size) {
    int line_count = 0;
    const char *p = data;
    const char *end = data + size;

    // Processando blocos maiores para melhor eficiência
    while (p < end) {
        // Busca caracteres de nova linha em blocos
        const char *nl = memchr(p, '\n', end - p);
        if (nl) {
            line_count++;
            p = nl + 1;
        } else {
            // Se não encontrou mais \n, mas ainda há dados, é a última linha
            if (p < end) {
                line_count++;
            }
            break;
        }
    }

    return line_count;
}

/**
 * Mapeia o arquivo inteiro para a memória usando mmap
 * @param filepath Caminho para o arquivo a ser mapeado
 * @return Estrutura MappedFile contendo os dados mapeados e o tamanho
 */
MappedFile map_file(const char *filepath) {
    MappedFile result = {NULL, 0, 0};

    int fd = open(filepath, O_RDONLY);
    if (fd == -1) {
        perror("Erro ao abrir o arquivo para mapeamento");
        return result;
    }

    // Obtém o tamanho do arquivo
    struct stat sb;
    if (fstat(fd, &sb) == -1) {
        perror("Erro ao obter o tamanho do arquivo");
        close(fd);
        return result;
    }

    // Mapeia o arquivo para a memória
    char *data = mmap(NULL, sb.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
    if (data == MAP_FAILED) {
        perror("Erro ao mapear o arquivo");
        close(fd);
        return result;
    }

    // Fecha o descritor de arquivo (o mapeamento permanece válido)
    close(fd);

    result.data = data;
    result.size = sb.st_size;
    result.block_count = sb.st_blocks;
    result.line_count =
        count_lines_in_memory_parallel(data, sb.st_size, sb.st_blocks);
    return result;
}

/**
 * Desmapeia o arquivo da memória
 * @param file Ponteiro para a estrutura MappedFile
 */
void unmap_file(MappedFile *file) {
    if (file && file->data) {
        munmap(file->data, file->size);
        file->data = NULL;
        file->size = 0;
        file->line_count = 0;
    }
}

/**
 * Obtém uma linha do arquivo mapeado começando no offset fornecido
 * @param file A estrutura do arquivo mapeado
 * @param offset Ponteiro para o offset atual (será atualizado)
 * @return Ponteiro para o início da linha (não terminada em null)
 */
char *get_line_at_offset(MappedFile file, size_t *offset) {
    if (*offset >= file.size) {
        return NULL;  // Fim do arquivo
    }

    char *line_start = file.data + *offset;

    // Encontra o fim da linha
    while (*offset < file.size && file.data[*offset] != '\n') {
        (*offset)++;
    }

    // Avança além do caractere de nova linha
    if (*offset < file.size) {
        (*offset)++;
    }

    return line_start;
}

/**
 * Imprime as primeiras n linhas do arquivo mapeado
 * @param file A estrutura do arquivo mapeado
 * @param n Número de linhas a serem impressas (se n <= 0, imprime todas as
 * linhas)
 */
void print_first_n_lines(MappedFile file, int n) {
    if (file.data == NULL || file.size == 0) {
        printf("Não há dados para exibir.\n");
        return;
    }

    size_t offset = 0;
    int count = 0;

    // Usamos o line_count já calculado durante o mapeamento
    int total_lines = file.line_count;

    // Imprime as primeiras n linhas
    while (offset < file.size && (n <= 0 || count < n)) {
        size_t line_start = offset;

        // Encontra o fim desta linha
        while (offset < file.size && file.data[offset] != '\n') {
            offset++;
        }

        // Imprime esta linha (cria uma string temporária terminada em null)
        int line_length = offset - line_start;
        char *temp_line = malloc(line_length + 1);
        if (temp_line) {
            memcpy(temp_line, file.data + line_start, line_length);
            temp_line[line_length] = '\0';
            printf("%s\n", temp_line);
            free(temp_line);
        }

        // Avança além da nova linha
        if (offset < file.size) {
            offset++;
        }

        count++;
    }

    if (n > 0 && total_lines > n) {
        printf("... (%d linhas adicionais não exibidas)\n", total_lines - n);
    }
}

/**
 * Função executada pelas threads para contar linhas em um bloco específico
 * @param arg Ponteiro para a estrutura ThreadData
 * @return NULL
 */
void *count_lines_worker(void *arg) {
    ThreadData *data = (ThreadData *)arg;
    int local_count = 0;
    const char *p = data->start;
    const char *end = p + data->size;

    // Conta linhas no bloco designado
    while (p < end) {
        const char *nl = memchr(p, '\n', end - p);
        if (nl) {
            local_count++;
            p = nl + 1;
        } else {
            // Se não encontrou mais \n, mas ainda há dados, é a última linha
            if (p < end) {
                local_count++;
            }
            break;
        }
    }

    // Armazena a contagem local no resultado da thread
    data->line_count = local_count;

    return NULL;
}

/**
 * Aloca recursos para threads e inicializa dados
 * @param num_threads Número de threads a serem criadas
 * @return Estrutura com ponteiros para threads e dados, NULL em caso de falha
 */
typedef struct {
    pthread_t *threads;
    ThreadData *thread_data;
    int num_threads;
} ThreadResources;

ThreadResources *allocate_thread_resources(int num_threads) {
    ThreadResources *res = malloc(sizeof(ThreadResources));
    if (!res) {
        return NULL;
    }

    res->num_threads = num_threads;
    res->threads = malloc(sizeof(pthread_t) * num_threads);
    res->thread_data = malloc(sizeof(ThreadData) * num_threads);

    if (!res->threads || !res->thread_data) {
        free(res->threads);
        free(res->thread_data);
        free(res);
        return NULL;
    }

    return res;
}

/**
 * Libera recursos alocados para threads
 * @param resources Ponteiro para a estrutura de recursos
 */
void free_thread_resources(ThreadResources *resources) {
    if (resources) {
        free(resources->threads);
        free(resources->thread_data);
        free(resources);
    }
}

/**
 * Calcula o tamanho do bloco para cada thread
 * @param thread_index Índice da thread
 * @param num_threads Número total de threads
 * @param total_size Tamanho total dos dados
 * @return Tamanho do bloco para esta thread
 */
size_t calculate_block_size(int thread_index, int num_threads,
                            size_t total_size) {
    size_t block_size = total_size / num_threads;
    size_t remaining = total_size % num_threads;

    return (thread_index == num_threads - 1) ? block_size + remaining
                                             : block_size;
}

/**
 * Inicializa os dados para uma thread
 * @param thread_data Array de dados de threads
 * @param index Índice da thread
 * @param data Início dos dados completos
 * @param block_size Tamanho do bloco para esta thread
 * @param block_offset Deslocamento do início do bloco
 */
void initialize_thread_data(ThreadData *thread_data, int index,
                            const char *data, size_t block_size,
                            size_t block_offset) {
    thread_data[index].start = data + block_offset;
    thread_data[index].size = block_size;
    thread_data[index].line_count = 0;
}

/**
 * Ajusta os limites dos blocos para garantir que cada thread comece no início
 * de uma linha
 * @param thread_data Array de dados de threads
 * @param i Índice da thread atual
 * @param data Ponteiro para o início de todos os dados
 */
void adjust_block_boundaries(ThreadData *thread_data, int i, const char *data) {
    if (i > 0) {
        const char *ptr = thread_data[i].start;
        // Avança o ponteiro até encontrar o caractere de nova linha que termina
        // a linha anterior
        while (ptr < data + thread_data[i].size && *ptr != '\n') {
            ptr++;
        }
        // Se encontrou uma nova linha, avança para depois dela para garantir
        // que o bloco comece no início de uma nova linha
        if (ptr < data + thread_data[i].size && *ptr == '\n') {
            ptr++;
        }
        size_t adjustment = ptr - thread_data[i].start;
        thread_data[i].start = ptr;
        thread_data[i].size -= adjustment;
        thread_data[i - 1].size += adjustment;
    }
}

/**
 * Corrige a contagem de linhas duplicadas nas fronteiras dos blocos
 * @param thread_data Array de dados de threads
 * @param num_threads Número total de threads
 * @param data Ponteiro para o início de todos os dados
 * @param size Tamanho total dos dados
 * @return Número de linhas duplicadas
 */
int correct_duplicate_lines(ThreadData *thread_data, int num_threads,
                            const char *data, size_t size) {
    int duplicates = 0;

    for (int i = 1; i < num_threads; i++) {
        // Verifica se uma linha foi dividida e contada duas vezes
        const char *prev_end =
            thread_data[i - 1].start + thread_data[i - 1].size - 1;

        if ((prev_end > data) && (*prev_end != '\n') &&
            (thread_data[i].start < data + size) &&
            (*(thread_data[i].start) != '\n')) {
            duplicates++;
        }
    }

    printf("Linhas duplicadas corrigidas: %d\n", duplicates);

    return duplicates;
}

/**
 * Cria e inicia as threads para processar blocos de dados
 * @param resources Recursos alocados para threads
 * @param data Ponteiro para os dados completos
 * @param size Tamanho total dos dados
 * @return 0 em caso de sucesso, -1 em caso de falha
 */
int start_threads(ThreadResources *resources, const char *data, size_t size) {
    size_t current_offset = 0;

    for (int i = 0; i < resources->num_threads; i++) {
        size_t block_size =
            calculate_block_size(i, resources->num_threads, size);

        // Initialize the thread data for this block using the current offset.
        initialize_thread_data(resources->thread_data, i, data, block_size,
                               current_offset);

        // Adjust the block so it starts from the beginning of the next line.
        adjust_block_boundaries(resources->thread_data, i, data);

        // Create the thread to count lines in this block.
        if (pthread_create(&resources->threads[i], NULL, count_lines_worker,
                           &resources->thread_data[i]) != 0) {
            fprintf(stderr, "Falha ao criar thread %d\n", i);
            return -1;
        }

        // Update current_offset to the adjusted end of the block.
        // Calculate the new offset as the difference between the thread's
        // starting pointer and the data pointer plus the adjusted size.
        current_offset = (resources->thread_data[i].start - data) +
                         resources->thread_data[i].size;
    }
    return 0;
}

/**
 * Aguarda todas as threads terminarem e coleta os resultados
 * @param resources Recursos das threads
 * @return Total de linhas contadas por todas as threads
 */
int join_threads_and_collect_results(ThreadResources *resources) {
    int total_line_count = 0;

    for (int i = 0; i < resources->num_threads; i++) {
        pthread_join(resources->threads[i], NULL);
        total_line_count += resources->thread_data[i].line_count;
    }

    return total_line_count;
}

/**
 * Conta o número de linhas em um bloco de memória (em paralelo)
 * @param data Ponteiro para o início do bloco de memória
 * @param size Tamanho do bloco de memória
 * @return Número de linhas no bloco
 */
int count_lines_in_memory_parallel(const char *data, size_t size,
                                   size_t block_count) {
    int total_line_count = 0;
    const int num_threads = get_available_number_of_processors();

    // Aloca recursos para threads
    ThreadResources *resources = allocate_thread_resources(num_threads);
    if (!resources) {
        fprintf(stderr,
                "Falha na alocação de memória para threads\nUsando fallback "
                "para versão serial\n");
        return count_lines_in_memory(data,
                                     size);  // Fallback para versão serial
    }

    // Inicializa o mutex
    if (pthread_mutex_init(&line_count_mutex, NULL) != 0) {
        fprintf(stderr,
                "Falha na inicialização do mutex\nUsando fallback "
                "para versão serial\n");
        free_thread_resources(resources);
        return count_lines_in_memory(data,
                                     size);  // Fallback para versão serial
    }

    printf("Contando linhas em paralelo com %d threads\n", num_threads);

    // Inicia as threads
    if (start_threads(resources, data, size) != 0) {
        // Ainda será possível coletar resultados das threads criadas com
        // sucesso
    }

    // Coleta resultados
    total_line_count = join_threads_and_collect_results(resources);

    // Corrige possíveis linhas duplicadas nas fronteiras dos blocos
    int duplicates = correct_duplicate_lines(resources->thread_data,
                                             num_threads, data, size);
    total_line_count -= duplicates;

    // Libera os recursos
    pthread_mutex_destroy(&line_count_mutex);
    free_thread_resources(resources);

    return total_line_count;
}
