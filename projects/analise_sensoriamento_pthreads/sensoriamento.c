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
    char *data;      // Ponteiro para os dados mapeados
    size_t size;     // Tamanho dos dados mapeados
    int line_count;  // Total de linhas no arquivo
} MappedFile;

int get_available_number_of_processors(void);
bool validate_csv_extension(const char *filename);
bool validate_args(int argc, char *argv[]);
MappedFile map_file(const char *filepath);
void unmap_file(MappedFile *file);
void print_first_n_lines(MappedFile file, int n);
char *get_line_at_offset(MappedFile file, size_t *offset);
int count_lines_in_memory(const char *data, size_t size);

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
    result.line_count = count_lines_in_memory(data, sb.st_size);
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