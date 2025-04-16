#include "../include/csv_data.h"

#include <fcntl.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include "../include/thread_utils.h"

// Helper: Parse a single CSV line into a CSVLine struct (fields separated by
// '|')
static CSVLine parse_csv_line(const char *start, const char *end,
                              const char *base) {
    CSVLine line;
    const char *field_start = start;
    const char *field_end = start;
    CSVField *fields[] = {&line.device,  &line.data,         &line.temperatura,
                          &line.umidade, &line.luminosidade, &line.ruido,
                          &line.eco2,    &line.etvoc};
    size_t num_fields = sizeof(fields) / sizeof(fields[0]);

    for (size_t i = 0; i < num_fields; ++i) {
        // Find next '|' or end of line
        field_end = field_start;
        while (field_end < end && *field_end != '|') field_end++;
        fields[i]->start_offset = (uint32_t)(field_start - base);
        fields[i]->end_offset = (uint32_t)(field_end - base);
        if (field_end < end && *field_end == '|')
            field_start = field_end + 1;
        else
            field_start = end;
    }
    return line;
}

typedef struct {
    CSVLine *lines;
    int current;
    int max;
    const char *base;
} CsvLineCollector;

static void csv_line_callback(const char *line_start, size_t line_len,
                              void *user_data) {
    CsvLineCollector *collector = (CsvLineCollector *)user_data;
    if (collector->current >= collector->max) return;
    const char *end = line_start + line_len;
    collector->lines[collector->current++] =
        parse_csv_line(line_start, end, collector->base);
}

pthread_mutex_t count_mutex = PTHREAD_MUTEX_INITIALIZER;

void line_count_callback(const char *line_start, size_t line_len,
                         void *user_data) {
    int *count = (int *)user_data;
    pthread_mutex_lock(&count_mutex);
    (*count)++;
    pthread_mutex_unlock(&count_mutex);
}

CSVFile csvfile_map(const char *filepath) {
    CSVFile csv = {0};
    if (!filepath) return csv;

    int fd = open(filepath, O_RDONLY);
    if (fd == -1) {
        perror("Erro ao abrir o arquivo para mapeamento");
        return csv;
    }

    struct stat sb;
    if (fstat(fd, &sb) == -1) {
        perror("Erro ao obter o tamanho do arquivo");
        close(fd);
        return csv;
    }

    if (sb.st_size == 0) {
        fprintf(stderr, "Arquivo está vazio\n");
        close(fd);
        return csv;
    }

    char *data = mmap(NULL, sb.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
    if (data == MAP_FAILED) {
        perror("Erro ao mapear o arquivo");
        close(fd);
        return csv;
    }
    close(fd);

    csv.mapped_data = data;
    csv.size = sb.st_size;
    csv.base = data;

    // Descobrir número de linhas (contar '\n') usando parallel_process_lines
    int num_threads = get_available_number_of_processors();
    if (num_threads < 1) num_threads = 1;
    int line_count = 0;
    parallel_process_lines(data, sb.st_size, num_threads, line_count_callback,
                           &line_count, NULL, NULL);

    // Ajuste para não perder a última linha se não terminar com '\n'
    if (sb.st_size > 0 && data[sb.st_size - 1] != '\n') line_count++;

    if (line_count <= 0) {
        munmap(data, sb.st_size);
        csv.mapped_data = NULL;
        csv.size = 0;
        return csv;
    }

    // Header: first line
    const char *buf_end = data + sb.st_size;
    const char *header_start = data;
    const char *header_end = header_start;
    while (header_end < buf_end && *header_end != '\n') header_end++;
    csv.header.start_offset = (uint32_t)(header_start - data);
    csv.header.end_offset = (uint32_t)(header_end - data);

    // Data lines: skip header
    int data_lines = line_count - 1;
    csv.line_count = data_lines;
    // Debug: print struct sizes and allocation
    printf("[DEBUG] sizeof(CSVLine): %zu bytes\n", sizeof(CSVLine));
    printf("[DEBUG] sizeof(CSVField): %zu bytes\n", sizeof(CSVField));
    printf("[DEBUG] data_lines: %d\n", data_lines);
    printf("[DEBUG] Total CSVLine array size: %.2f MB\n",
           (data_lines * sizeof(CSVLine)) / (1024.0 * 1024.0));
    printf("[DEBUG] mmap file size: %.2f MB\n", (csv.size) / (1024.0 * 1024.0));
    if (data_lines > 0) {
        csv.lines = (CSVLine *)malloc(sizeof(CSVLine) * data_lines);
        if (!csv.lines) {
            munmap(data, sb.st_size);
            csv.mapped_data = NULL;
            csv.size = 0;
            csv.line_count = 0;
            return csv;
        }
        // Prepare collector for parallel processing
        CsvLineCollector collector = {csv.lines, 0, data_lines, data};

        // Find start of first data line
        const char *first_data = header_end;
        if (first_data < buf_end && *first_data == '\n') first_data++;
        size_t data_offset = first_data - data;
        size_t data_size = sb.st_size - data_offset;

        // Process each data line in parallel (excluding header)
        int num_threads = (data_size < 102400) ? 1
                          : (data_size < 1048576)
                              ? 2
                              : get_available_number_of_processors();
        // Debug: measure time to run parallel_process_lines
        struct timespec start_time, end_time;
        clock_gettime(CLOCK_MONOTONIC, &start_time);

        parallel_process_lines(first_data, data_size, num_threads,
                               csv_line_callback, &collector, NULL, NULL);

        clock_gettime(CLOCK_MONOTONIC, &end_time);
        double elapsed_sec = (end_time.tv_sec - start_time.tv_sec) +
                             (end_time.tv_nsec - start_time.tv_nsec) / 1e9;
        printf("[DEBUG] parallel_process_lines took %.6f seconds\n",
               elapsed_sec);
    }

    // // press enter to continue
    // printf("Pressione Enter para continuar...\n");
    // getchar();
    return csv;
}

void csvfile_unmap(CSVFile *csv) {
    if (!csv) return;
    if (csv->lines) {
        free(csv->lines);
        csv->lines = NULL;
    }
    if (csv->mapped_data && csv->size > 0) {
        munmap(csv->mapped_data, csv->size);
        csv->mapped_data = NULL;
    }
    csv->size = 0;
    csv->line_count = 0;
}

const CSVLine *csvfile_get_line(const CSVFile *csv, int line_number) {
    if (!csv || !csv->lines || line_number < 0 ||
        line_number >= csv->line_count)
        return NULL;
    return &csv->lines[line_number];
}

char *csvfield_to_string(const CSVFile *csv, const CSVField *field) {
    if (!csv || !field || field->end_offset <= field->start_offset) {
        char *empty = (char *)malloc(1);
        if (empty) empty[0] = '\0';
        return empty;
    }
    size_t len = field->end_offset - field->start_offset;
    char *str = (char *)malloc(len + 1);
    if (!str) return NULL;
    memcpy(str, csv->base + field->start_offset, len);
    str[len] = '\0';
    return str;
}

void csvfile_print_lines(const CSVFile *csv, int start_line, int num_lines) {
    if (!csv || !csv->lines || csv->line_count == 0) {
        printf("Sem dados para exibir.\n");
        return;
    }
    int end_line = start_line + num_lines;
    if (start_line < 0) start_line = 0;
    if (end_line > csv->line_count) end_line = csv->line_count;
    printf("Exibindo linhas %d a %d (total de linhas: %d)\n", start_line + 1,
           end_line, csv->line_count);
    for (int i = start_line; i < end_line; ++i) {
        // char *id = csvfield_to_string(csv, &csv->lines[i].id);
        char *device = csvfield_to_string(csv, &csv->lines[i].device);
        // char *contagem = csvfield_to_string(csv, &csv->lines[i].contagem);
        char *data = csvfield_to_string(csv, &csv->lines[i].data);
        printf("%s|%s|...\n", device, data);
        // free(id);
        free(device);
        // free(contagem);
        free(data);
    }
    if (end_line < csv->line_count) {
        printf("... (%d linhas adicionais não exibidas)\n",
               csv->line_count - end_line);
    }
}

void csvfile_print_header(const CSVFile *csv) {
    if (!csv) return;
    char *header = csvfield_to_string(csv, &csv->header);
    printf("Cabeçalho: %s\n", header ? header : "(null)");
    free(header);
}

void csvfile_print_info(const CSVFile *csv) {
    if (!csv) {
        printf("Arquivo CSV não mapeado ou inválido\n");
        return;
    }
    printf("Informações do CSV:\n");
    printf("- Tamanho: %zu bytes\n", csv->size);
    printf("- Linhas de dados: %d\n", csv->line_count);
    csvfile_print_header(csv);
}
