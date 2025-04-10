#include <pthread.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/sysinfo.h>

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

int main(int argc, char *argv[]) {
    if (!validate_args(argc, argv)) {
        return EXIT_FAILURE;
    }

    const char *filepath = argv[1];

    if (!validate_csv_extension(filepath)) {
        return EXIT_FAILURE;
    }

    FILE *file = fopen(filepath, "r");
    if (!file) {
        perror("Erro ao abrir o arquivo");
        return EXIT_FAILURE;
    }

    int num_processors = get_available_number_of_processors();
    printf("Processadores disponíveis: %d\n", num_processors);

    // Aqui você pode começar a montar a lógica de dividir o trabalho com
    // pthreads Por exemplo: contar linhas, dividir range entre threads, etc.

    fclose(file);
    return EXIT_SUCCESS;
}
