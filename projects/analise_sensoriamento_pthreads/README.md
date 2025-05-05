# Análise de Sensoriamento com Pthreads

Este projeto implementa um sistema multithread para análise de dados de sensores utilizando a biblioteca POSIX Threads (Pthreads) em C. O sistema simula o processamento paralelo de dados de múltiplos sensores de um arquivo CSV, demonstrando conceitos de programação concorrente e sincronização de threads.

## Funcionalidades

- Acesso ao arquivo utilizando mmap
- Processamento paralelo de dados de sensores usando threads
- Sincronização de acesso a recursos compartilhados usando mutex
- Simulação de diferentes cargas de trabalho para análise de desempenho
- Implementação de condições de corrida controladas para fins educacionais

## Requisitos

- Sistema operacional Linux
- Compilador GCC
- Biblioteca POSIX Threads
- Make (para compilação do projeto)

## Compilação

Para compilar o projeto, execute:

```bash
make all
```

## Execução

Para executar o programa:

```bash
bin/sensoriamento [caminho-do-arquivo]
```

Exemplo:
```bash
bin/sensoriamento /home/marhaubrich/Downloads/devices_mqtt_20250402/devices.csv
```

## Estrutura do Projeto

- `src/` - Arquivos fonte
- `include/` - Arquivos de cabeçalho
- `obj/` - Objetos compilados
- `bin/` - Executáveis

## Licença

Este projeto é distribuído sob a licença MIT.