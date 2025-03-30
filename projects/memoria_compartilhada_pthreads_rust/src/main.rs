use std::sync::{Arc, Mutex};
use std::thread;

const NUM_INCREMENTOS: i32 = 10000000;

fn incrementa(contador: &Arc<Mutex<i32>>) {
    for _ in 0..NUM_INCREMENTOS {
        let mut valor = contador.lock().unwrap();
        *valor += 1;
    }
}

fn main() {
    // Create mutex-protected counter and wrap in Arc for shared ownership
    let contador = Arc::new(Mutex::new(0));
    let mut handles = vec![];
    let num_threads: usize = std::thread::available_parallelism().unwrap().get();

    // Spawn threads
    for _ in 0..num_threads {
        let contador_clone = Arc::clone(&contador);
        let handle = thread::spawn(move || {
            incrementa(&contador_clone);
        });
        handles.push(handle);
    }

    // Join all threads
    for handle in handles {
        handle.join().unwrap();
    }

    // Print results
    println!(
        "Valor final esperado do contador: {}",
        num_threads as i32 * NUM_INCREMENTOS
    );
    println!("Valor final do contador: {}", *contador.lock().unwrap());
}
