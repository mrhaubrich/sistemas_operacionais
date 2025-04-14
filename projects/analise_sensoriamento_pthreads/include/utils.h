#ifndef UTILS_H
#define UTILS_H

#include <stdbool.h>

#include "file_mapping.h"

/**
 * Valida que um nome de arquivo possui a extensão .csv
 * @param filename O nome do arquivo para validar
 * @return true se o nome do arquivo termina com .csv, false caso contrário
 */
bool validate_csv_extension(const char *filename);

/**
 * Valida os argumentos da linha de comando
 * @param argc Contagem de argumentos
 * @param argv Vetor de argumentos
 * @return true se os argumentos são válidos, false caso contrário
 */
bool validate_args(int argc, char *argv[]);

/**
 * Obtém o número de processadores disponíveis no sistema
 * @return Número de processadores disponíveis
 */
int get_available_number_of_processors(void);

/**
 * Imprime as primeiras n linhas de um arquivo mapeado
 * @param file O arquivo mapeado
 * @param n Número de linhas a serem impressas (se n <= 0, imprime todas as
 * linhas)
 */
void print_first_n_lines(MappedCSV csv, int n);

/**
 * Imprime um intervalo de linhas de um arquivo CSV mapeado
 * @param csv O arquivo CSV mapeado
 * @param start_line A primeira linha a ser impressa (base 0)
 * @param num_lines Número de linhas a serem impressas
 */
void print_lines_range(MappedCSV csv, int start_line, int num_lines);

/**
 * Cria uma representação formatada de um tamanho de memória
 * @param size Tamanho em bytes
 * @param buffer Buffer para armazenar o resultado
 * @param buffer_size Tamanho do buffer
 */
void format_size(size_t size, char *buffer, size_t buffer_size);

/**
 * Imprime informações sobre um arquivo CSV mapeado
 * @param csv O arquivo CSV mapeado
 */
void print_csv_info(const MappedCSV *csv);

#endif  // UTILS_H