#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>

#define NUM_THREADS 2
#define NUM_INCREMENTOS 1000

int contador = 0;  

void *incrementa(void *arg) {
    for (int i = 0; i < NUM_INCREMENTOS; i++) {
        contador++; 
    }
    pthread_exit(NULL);
}

int main() {
    pthread_t threads[NUM_THREADS];

    for (int i = 0; i < NUM_THREADS; i++) {
        pthread_create(&threads[i], NULL, incrementa, NULL);
    }

    for (int i = 0; i < NUM_THREADS; i++) {
        pthread_join(threads[i], NULL);
    }

    printf("Valor final esperado do contador: %d\n", NUM_THREADS * NUM_INCREMENTOS);
    printf("Valor final do contador: %d\n", contador);

    return 0;
}
