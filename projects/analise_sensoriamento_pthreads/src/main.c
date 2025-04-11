#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "../include/file_mapping.h"
#include "../include/utils.h"

/**
 * Main entry point for the CSV file processing program
 */
int main(int argc, char *argv[]) {
    printf("CSV File Processor - Using pthreads for parallel processing\n");
    printf("----------------------------------------------------------\n");

    // Validate command line arguments
    if (!validate_args(argc, argv)) {
        return EXIT_FAILURE;
    }

    const char *filepath = argv[1];

    // Validate file extension
    if (!validate_csv_extension(filepath)) {
        return EXIT_FAILURE;
    }

    // Print system information
    int num_processors = get_available_number_of_processors();
    printf("Available processors: %d\n", num_processors);
    printf("Processing file: %s\n\n", filepath);

    // Record the start time
    clock_t start_time = clock();

    // Map file into memory
    MappedFile mfile = map_file(filepath);
    if (mfile.data == NULL) {
        fprintf(stderr, "Failed to map file\n");
        return EXIT_FAILURE;
    }

    // Calculate elapsed time
    clock_t end_time = clock();
    double elapsed_time = (double)(end_time - start_time) / CLOCKS_PER_SEC;

    // Print file information
    printf("\nProcessing completed in %.3f seconds\n", elapsed_time);
    print_file_info(&mfile);

    // Print a sample of the file content
    printf("\nFile Content Sample:\n");
    printf("-------------------\n");
    print_first_n_lines(mfile, 10);

    // Add additional command functionality: allow displaying specific line
    // ranges
    if (argc > 3 && strcmp(argv[2], "--range") == 0) {
        int start_line = atoi(argv[3]);
        int num_lines = 10;  // Default

        if (argc > 4) {
            num_lines = atoi(argv[4]);
        }

        printf("\nDisplaying Custom Line Range:\n");
        print_lines_range(mfile, start_line - 1,
                          num_lines);  // Convert to 0-based
    }

    // Cleanup
    unmap_file(&mfile);
    printf("\nFile unmapped and resources freed\n");

    return EXIT_SUCCESS;
}