#include "../include/utils.h"

#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/sysinfo.h>

/**
 * Valida que o nome do arquivo possui extensão .csv
 * @param filename O nome do arquivo para validar
 * @return true se o nome do arquivo termina com .csv, false caso contrário
 */
bool validate_csv_extension(const char *filename) {
    if (!filename) {
        fprintf(stderr, "Erro: Nome do arquivo inválido (NULL)\n");
        return false;
    }

    const char *dot = strrchr(filename, '.');
    const bool is_valid = dot && strcmp(dot, ".csv") == 0;

    if (!is_valid) {
        fprintf(stderr, "Erro: O arquivo deve ter extensão .csv\n");
    }
    return is_valid;
}

/**
 * Valida os argumentos da linha de comando
 * @param argc Contagem de argumentos
 * @param argv Vetor de argumentos
 * @return true se os argumentos são válidos, false caso contrário
 */
bool validate_args(int argc, char *argv[]) {
    const bool is_valid = argc > 1;

    if (!is_valid) {
        fprintf(stderr, "Uso: %s <caminho_para_arquivo_csv>\n", argv[0]);
    }
    return is_valid;
}

/**
 * Obtém o número de processadores disponíveis no sistema
 * @return Número de processadores disponíveis
 */
int get_available_number_of_processors(void) {
    int nprocs = get_nprocs();
    return nprocs > 0 ? nprocs : 1;  // Sempre retorna pelo menos 1
}

/**
 * Imprime um intervalo de linhas de um arquivo mapeado
 * @param file O arquivo mapeado
 * @param start_line A primeira linha a ser impressa (base 0)
 * @param num_lines Número de linhas a serem impressas
 */
void print_lines_range(MappedFile file, int start_line, int num_lines) {
    if (file.data == NULL || file.size == 0) {
        printf("Sem dados para exibir.\n");
        return;
    }

    if (start_line < 0) start_line = 0;

    // Calcula quantas linhas imprimir
    int total_lines = file.line_count;
    int end_line = start_line + num_lines;
    if (end_line > total_lines) end_line = total_lines;

    int lines_to_print = end_line - start_line;

    if (lines_to_print <= 0) {
        printf("Nenhuma linha para exibir no intervalo especificado.\n");
        return;
    }

    printf("Exibindo linhas %d a %d (total de linhas: %d)\n", start_line + 1,
           end_line, total_lines);

    // Usa nossa função aprimorada get_line para recuperar linhas
    for (int i = start_line; i < end_line; i++) {
        int line_length = 0;
        char *line = get_line(&file, i, &line_length);

        if (line) {
            printf("Linha %d: %s\n", i + 1, line);
            free(line);
        } else {
            printf("Linha %d: <erro ao recuperar linha>\n", i + 1);
        }
    }

    if (end_line < total_lines) {
        printf("... (%d linhas adicionais não exibidas)\n",
               total_lines - end_line);
    }
}

/**
 * Imprime as primeiras n linhas de um arquivo mapeado
 * @param file O arquivo mapeado
 * @param n Número de linhas a serem impressas (se n <= 0, imprime todas as
 * linhas)
 */
void print_first_n_lines(MappedFile file, int n) {
    print_lines_range(file, 0, n <= 0 ? file.line_count : n);
}

/**
 * Cria uma representação formatada de um tamanho de memória
 * @param size Tamanho em bytes
 * @param buffer Buffer para armazenar o resultado
 * @param buffer_size Tamanho do buffer
 */
void format_size(size_t size, char *buffer, size_t buffer_size) {
    if (size < 1024) {
        snprintf(buffer, buffer_size, "%zu bytes", size);
    } else if (size < 1024 * 1024) {
        snprintf(buffer, buffer_size, "%.2f KB", size / 1024.0);
    } else if (size < 1024 * 1024 * 1024) {
        snprintf(buffer, buffer_size, "%.2f MB", size / (1024.0 * 1024.0));
    } else {
        snprintf(buffer, buffer_size, "%.2f GB",
                 size / (1024.0 * 1024.0 * 1024.0));
    }
}

/**
 * Imprime informações sobre um arquivo mapeado
 * @param file O arquivo mapeado
 */
void print_file_info(const MappedFile *file) {
    if (!file || !file->data) {
        printf("Arquivo não mapeado ou inválido\n");
        return;
    }

    char size_str[32];
    format_size(file->size, size_str, sizeof(size_str));

    printf("Informações do Arquivo:\n");
    printf("- Tamanho: %s (%zu bytes)\n", size_str, file->size);
    printf("- Linhas: %d\n", file->line_count);
    printf("- Linhas indexadas: %d\n", file->total_indexed_lines);
    printf("- Contagem de blocos: %zu\n", file->block_count);

    // Para análise de eficiência
    if (file->line_indices) {
        printf("- Indexação de linhas: Disponível (acesso rápido)\n");
    } else {
        printf(
            "- Indexação de linhas: Não disponível (varredura sequencial)\n");
    }
}
