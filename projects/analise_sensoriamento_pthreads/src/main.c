#include <stdio.h>
#include <stdlib.h>

#include "../include/file_mapping.h"
#include "../include/utils.h"

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

    MappedFile mfile = map_file(filepath);
    if (mfile.data == NULL) {
        fprintf(stderr, "Falha ao mapear o arquivo\n");
        return EXIT_FAILURE;
    }

    printf("Arquivo mapeado com sucesso: %zu bytes\n", mfile.size);
    printf("Total de linhas no arquivo: %d\n", mfile.line_count);

    print_first_n_lines(mfile, 10);

    unmap_file(&mfile);
    return EXIT_SUCCESS;
}