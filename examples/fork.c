#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <unistd.h>

int main(void)
{
    int i;
    pid_t pid;

    //Cria o fork. Em caso de erro, aborta o programa
    if ((pid = fork()) < 0)
    {
        perror("Erro ao executar fork.");
        exit(1);
    }
    if (pid == 0)
    {
        //O código aqui dentro será executado no processo filho
        printf("Sou o processo filho. Meu PID é: %d\n", getpid());
    }
    else
    {
        //O código neste trecho será executado no processo pai
        printf("Sou o processo Pai. Meu PID é: %d\n", getpid());
    }


    printf("Ambos processos executam este trecho.\n\n");
    //scanf("%d", &i);
    exit(0);
}
