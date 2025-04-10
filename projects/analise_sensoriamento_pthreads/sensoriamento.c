#include <pthread.h>
#include <stdio.h>
#include <sys/sysinfo.h>

int get_available_number_of_processors(void) {
    return (get_nprocs());
}

int main(int argc, char *argv[]) {
    int number_of_processors = get_available_number_of_processors();

    printf("%i", number_of_processors);
    return (0);
}
