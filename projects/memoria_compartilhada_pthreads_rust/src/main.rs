// Importação de bibliotecas necessárias:
// Arc (Atomic Reference Counter) permite compartilhar dados entre threads de forma segura
// Mutex (Mutual Exclusion) garante acesso exclusivo aos dados compartilhados
use std::sync::{Arc, Mutex};
// Biblioteca para criação e gerenciamento de threads
use std::thread;

// Constante que define o número de incrementos que cada thread realizará
const NUM_INCREMENTOS: i32 = 10000000;

// Função que incrementa um contador protegido por mutex
// Recebe uma referência ao contador compartilhado entre as threads
fn incrementa(contador: &Arc<Mutex<i32>>) {
    // Loop que executa o número definido de incrementos
    for _ in 0..NUM_INCREMENTOS {
        // Obtém o bloqueio do mutex, garantindo acesso exclusivo ao contador
        // unwrap() é usado para tratar o Result retornado por lock()
        let mut valor = contador.lock().unwrap();
        // Incrementa o valor do contador em 1
        *valor += 1;
    }
}

fn main() {
    // Cria um contador protegido por mutex e o envolve em Arc para permitir propriedade compartilhada
    // O contador é inicializado com valor 0
    let contador = Arc::new(Mutex::new(0));
    // Vetor para armazenar os handles das threads
    let mut handles = vec![];
    // Determina o número de threads baseado no número de núcleos disponíveis no sistema
    let num_threads: usize = std::thread::available_parallelism().unwrap().get();

    // Criação das threads
    for _ in 0..num_threads {
        // Clona o Arc para que cada thread tenha sua própria referência ao contador
        let contador_clone = Arc::clone(&contador);
        // Cria uma nova thread e move o contador clonado para dentro dela
        let handle = thread::spawn(move || {
            // Chama a função incrementa passando a referência ao contador
            incrementa(&contador_clone);
        });
        // Armazena o handle da thread no vetor para poder aguardar seu término posteriormente
        handles.push(handle);
    }

    // Aguarda todas as threads terminarem
    for handle in handles {
        // join() bloqueia a thread principal até que a thread correspondente termine
        // unwrap() é usado para tratar o Result retornado por join()
        handle.join().unwrap();
    }

    // Exibe os resultados
    // Calcula o valor esperado do contador (número de threads * número de incrementos)
    println!(
        "Valor final esperado do contador: {}",
        num_threads as i32 * NUM_INCREMENTOS
    );
    // Obtém o valor final do contador e o exibe
    println!("Valor final do contador: {}", *contador.lock().unwrap());
}
