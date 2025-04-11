#ifndef UTILS_H
#define UTILS_H

#include <stdbool.h>

#include "file_mapping.h"

bool validate_csv_extension(const char *filename);
bool validate_args(int argc, char *argv[]);
int get_available_number_of_processors(void);

/**
 * Imprime as primeiras n linhas do arquivo mapeado
 * @param file A estrutura do arquivo mapeado
 * @param n Número de linhas a serem impressas (se n <= 0, imprime todas as
 * linhas)
 */
void print_first_n_lines(MappedFile file, int n);

#endif  // UTILS_H