#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

// Função de thread
void *funcao_da_thread(void *num)
{
    long numero = (long) num;
    printf("Thread nº %ld em execução\n",numero);
    //sleep(1);
    pthread_exit(NULL);
}

int main(int argc, char **argv)
{
    // Cria thread
    pthread_t thread;
    // identifica parametro
    long numero_da_thread = 1;
    // Inicia thread
    pthread_create(&thread, NULL, funcao_da_thread, (void *)numero_da_thread);

    printf("Término da execução do programa (thread) principal\n");
    pthread_exit(NULL);    
    return 0;
}