#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "../include/file_mapping.h"
#include "../include/utils.h"

/**
 * Ponto de entrada principal para o programa de processamento de arquivos CSV
 */
int main(int argc, char *argv[]) {
    printf(
        "Processador de Arquivos CSV - Usando pthreads para processamento "
        "paralelo\n");
    printf("----------------------------------------------------------\n");

    // Validar argumentos da linha de comando
    if (!validate_args(argc, argv)) {
        return EXIT_FAILURE;
    }

    const char *filepath = argv[1];

    // Validar extensão do arquivo
    if (!validate_csv_extension(filepath)) {
        return EXIT_FAILURE;
    }

    // Imprimir informações do sistema
    int num_processors = get_available_number_of_processors();
    printf("Processadores disponíveis: %d\n", num_processors);
    printf("Processando arquivo: %s\n\n", filepath);

    // Mapear arquivo na memória
    MappedCSV mappedCsv = map_csv(filepath);
    if (mappedCsv.header == NULL) {
        fprintf(stderr, "Falha ao mapear o arquivo\n");
        return EXIT_FAILURE;
    }

    // Imprimir informações do arquivo
    print_csv_info(&mappedCsv);

    // Imprimir uma amostra do conteúdo do arquivo
    printf("\nAmostra do Conteúdo do Arquivo:\n");
    printf("-------------------\n");
    for (int i = 0; i < 10 && i < mappedCsv.data_count; i++) {
        int line_length = 0;
        char *line = get_line(&mappedCsv, i, &line_length);
        if (line) {
            printf("%s\n", line);
            free(line);
        }
    }

    // Adicionar funcionalidade adicional de comando: permitir exibir intervalos
    // específicos de linhas
    if (argc > 3 && strcmp(argv[2], "--range") == 0) {
        int start_line = atoi(argv[3]);
        int num_lines = 10;  // Padrão

        if (argc > 4) {
            num_lines = atoi(argv[4]);
        }

        printf("\nExibindo Intervalo Personalizado de Linhas:\n");
        print_lines_range(mappedCsv, start_line - 1,
                          num_lines);  // Converter para base 0
    }

    // Limpeza
    unmap_csv(&mappedCsv);
    printf("\nArquivo desmapeado e recursos liberados\n");

    return EXIT_SUCCESS;
}