#include <pthread.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/sysinfo.h>

#define SEPARATOR '|'
#define EXPECTED_HEADERS \
    'id|device|contagem|data|temperatura|umidade|luminosidade|ruido|eco2|etvoc|latitude|longitude'
#define BUFFER_SIZE \
    65536  // 64 KB buffer (will be used for chunks for multithreading)

typedef struct Line {
    char *line;
    struct Line *next;
} Line;

int get_available_number_of_processors(void);
bool validate_csv_extension(const char *filename);
bool validate_args(int argc, char *argv[]);
Line *read_file(FILE *file);  // Changed return type to Line*

// Linked List management functions
Line *create_line(char *line_content);
void add_line(Line **head, char *line_content);
void free_lines(Line *head);
int count_lines(Line *head);
void print_first_n_lines(Line *head, int n);  // Add this line

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

    Line *lines;
    lines = read_file(file);

    print_first_n_lines(lines, 10);

    fclose(file);
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
 * Reads the contents of a file and converts it to a linked list of lines
 * @param file The file pointer to read from
 * @return Pointer to the head of a linked list of lines, or NULL if error
 */
Line *read_file(FILE *file) {
    if (!file) return NULL;

    // Reset file position to the beginning
    rewind(file);

    Line *head = NULL;
    char buffer[BUFFER_SIZE];
    char line_buffer[BUFFER_SIZE];
    size_t bytes_read;
    size_t line_pos = 0;

    while ((bytes_read = fread(buffer, 1, sizeof(buffer), file)) > 0) {
        for (size_t i = 0; i < bytes_read; i++) {
            if (buffer[i] == '\n') {
                // Found end of line, add it to our linked list
                line_buffer[line_pos] = '\0';  // Null-terminate the string

                // Skip empty lines
                if (line_pos > 0) {
                    add_line(&head, line_buffer);
                }

                line_pos = 0;  // Reset for the next line
            } else if (line_pos < sizeof(line_buffer) - 1) {
                // Add character to current line
                line_buffer[line_pos++] = buffer[i];
            }
            // Else: line buffer overflow, we'll truncate the line
        }
    }

    // Handle the last line if it doesn't end with a newline
    if (line_pos > 0) {
        line_buffer[line_pos] = '\0';
        add_line(&head, line_buffer);
    }

    return head;
}

/**
 * Creates a new Line node with the given content
 * @param line_content The content to store in the line (will be copied)
 * @return Pointer to the newly created Line or NULL if memory allocation failed
 */
Line *create_line(char *line_content) {
    Line *new_line = (Line *)malloc(sizeof(Line));
    if (new_line == NULL) {
        return NULL;
    }

    new_line->line = strdup(line_content);
    if (new_line->line == NULL) {
        free(new_line);
        return NULL;
    }

    new_line->next = NULL;
    return new_line;
}

/**
 * Adds a new line at the end of the linked list
 * @param head Pointer to the head pointer of the list
 * @param line_content The content to store in the new line
 */
void add_line(Line **head, char *line_content) {
    Line *new_line = create_line(line_content);
    if (new_line == NULL) {
        return;  // Memory allocation failed
    }

    if (*head == NULL) {
        *head = new_line;
        return;
    }

    Line *current = *head;
    while (current->next != NULL) {
        current = current->next;
    }

    current->next = new_line;
}

/**
 * Frees all memory allocated for the linked list
 * @param head Pointer to the head of the list
 */
void free_lines(Line *head) {
    Line *current = head;
    Line *next;

    while (current != NULL) {
        next = current->next;
        free(current->line);
        free(current);
        current = next;
    }
}

/**
 * Counts the number of lines in the linked list
 * @param head Pointer to the head of the list
 * @return Number of lines in the list
 */
int count_lines(Line *head) {
    int count = 0;
    Line *current = head;

    while (current != NULL) {
        count++;
        current = current->next;
    }

    return count;
}

/**
 * Prints the first n lines from the linked list
 * @param head Pointer to the head of the list
 * @param n Number of lines to print (if n <= 0, prints all lines)
 */
void print_first_n_lines(Line *head, int n) {
    if (head == NULL) {
        printf("No lines to display.\n");
        return;
    }

    Line *current = head;
    int count = 0;

    while (current != NULL && (n <= 0 || count < n)) {
        printf("%s\n", current->line);
        current = current->next;
        count++;
    }

    int total = count_lines(head);
    if (n > 0 && total > n) {
        printf("... (%d more lines not shown)\n", total - n);
    }
}