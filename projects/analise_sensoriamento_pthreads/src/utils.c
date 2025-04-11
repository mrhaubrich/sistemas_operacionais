#include "../include/utils.h"

#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/sysinfo.h>

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

int get_available_number_of_processors(void) { return get_nprocs(); }

void print_first_n_lines(MappedFile file, int n) {
    if (file.data == NULL || file.size == 0) {
        printf("Não há dados para exibir.\n");
        return;
    }

    // Use the total line count from the file structure
    int total_lines = file.line_count;

    // Determine how many lines to print
    int lines_to_print = (n <= 0 || n > total_lines) ? total_lines : n;

    // Check if we have a valid line index array
    if (file.line_indices != NULL && file.total_indexed_lines > 0) {
        // Fast path: use the line index for direct access
        printf("Usando índice global para acesso direto às linhas.\n");

        // Make sure we don't try to access beyond the available indices
        int available_lines = file.total_indexed_lines < lines_to_print
                                  ? file.total_indexed_lines
                                  : lines_to_print;

        for (int i = 0; i < available_lines; i++) {
            const char *line_start = file.line_indices[i];

            // Find the end of this line (next newline or end of file)
            const char *line_end =
                memchr(line_start, '\n', file.data + file.size - line_start);
            if (!line_end) line_end = file.data + file.size;

            // Calculate line length and print
            int line_length = line_end - line_start;
            char *temp_line = malloc(line_length + 1);
            if (temp_line) {
                memcpy(temp_line, line_start, line_length);
                temp_line[line_length] = '\0';
                printf("%s\n", temp_line);
                free(temp_line);
            }
        }
    } else {
        // Slow path: fallback to scanning through the file (original method)
        printf(
            "Índice de linhas não disponível, usando método de varredura.\n");

        size_t offset = 0;
        int count = 0;

        // Imprime as primeiras n linhas
        while (offset < file.size && count < lines_to_print) {
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
    }

    if (n > 0 && total_lines > n) {
        printf("... (%d linhas adicionais não exibidas)\n", total_lines - n);
    }
}
