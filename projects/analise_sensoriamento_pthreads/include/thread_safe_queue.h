#ifndef THREAD_SAFE_QUEUE_H
#define THREAD_SAFE_QUEUE_H

#include <pthread.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

// Each node represents a slice from the mapped CSV.
typedef struct thread_safe_queue_node {
    const char *slice;  // Full string pointer to the slice.
    struct thread_safe_queue_node *next;
} ThreadSafeQueueNode;

// Queue structure for CSV slices.
typedef struct thread_safe_queue {
    ThreadSafeQueueNode *head;
    ThreadSafeQueueNode *tail;
    pthread_mutex_t mutex;
    size_t count;  // Number of slices in the queue.
} ThreadSafeQueue;

// Creates and initializes a new thread-safe queue.
ThreadSafeQueue *thread_safe_queue_create(void);

// Enqueues a slice (full string pointer) into the queue.
// Returns 0 on success or -1 on failure.
int thread_safe_queue_enqueue(ThreadSafeQueue *q, const char *slice);

// Dequeues a slice from the queue. Returns the slice pointer, or NULL if the
// queue is empty.
const char *thread_safe_queue_dequeue(ThreadSafeQueue *q);

// Peeks at the front slice without removing it. Returns the slice pointer or
// NULL if empty.
const char *thread_safe_queue_peek(ThreadSafeQueue *q);

// Returns the current number of slices in the queue.
size_t thread_safe_queue_get_count(ThreadSafeQueue *q);

// Destroys the queue and frees all its allocated resources.
void thread_safe_queue_destroy(ThreadSafeQueue *q);

#ifdef __cplusplus
}
#endif

#endif  // THREAD_SAFE_QUEUE_H
