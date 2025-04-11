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

    // Registrar o tempo de início
    clock_t start_time = clock();

    // Mapear arquivo na memória
    MappedFile mfile = map_file(filepath);
    if (mfile.data == NULL) {
        fprintf(stderr, "Falha ao mapear o arquivo\n");
        return EXIT_FAILURE;
    }

    // Calcular o tempo decorrido
    clock_t end_time = clock();
    double elapsed_time = (double)(end_time - start_time) / CLOCKS_PER_SEC;

    // Imprimir informações do arquivo
    printf("\nProcessamento concluído em %.3f segundos\n", elapsed_time);
    print_file_info(&mfile);

    // Imprimir uma amostra do conteúdo do arquivo
    printf("\nAmostra do Conteúdo do Arquivo:\n");
    printf("-------------------\n");
    print_first_n_lines(mfile, 10);

    // Adicionar funcionalidade adicional de comando: permitir exibir intervalos
    // específicos de linhas
    if (argc > 3 && strcmp(argv[2], "--range") == 0) {
        int start_line = atoi(argv[3]);
        int num_lines = 10;  // Padrão

        if (argc > 4) {
            num_lines = atoi(argv[4]);
        }

        printf("\nExibindo Intervalo Personalizado de Linhas:\n");
        print_lines_range(mfile, start_line - 1,
                          num_lines);  // Converter para base 0
    }

    // Limpeza
    unmap_file(&mfile);
    printf("\nArquivo desmapeado e recursos liberados\n");

    return EXIT_SUCCESS;
}