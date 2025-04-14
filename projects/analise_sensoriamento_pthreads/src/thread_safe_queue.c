#include "../include/thread_safe_queue.h"

#include <stdio.h>
#include <stdlib.h>

ThreadSafeQueue *thread_safe_queue_create(void) {
    ThreadSafeQueue *q = malloc(sizeof(ThreadSafeQueue));
    if (!q) {
        perror("Failed to allocate thread_safe_queue");
        return NULL;
    }
    q->head = q->tail = NULL;
    q->count = 0;
    if (pthread_mutex_init(&q->mutex, NULL) != 0) {
        perror("Mutex init failed");
        free(q);
        return NULL;
    }
    return q;
}

int thread_safe_queue_enqueue(ThreadSafeQueue *q, const char *slice) {
    if (!q) return -1;

    ThreadSafeQueueNode *node = malloc(sizeof(ThreadSafeQueueNode));
    if (!node) {
        perror("Failed to allocate queue node");
        return -1;
    }
    node->slice = slice;
    node->next = NULL;

    pthread_mutex_lock(&q->mutex);
    if (q->tail == NULL) {
        // The queue is empty.
        q->head = q->tail = node;
    } else {
        q->tail->next = node;
        q->tail = node;
    }
    q->count++;
    pthread_mutex_unlock(&q->mutex);

    return 0;
}

const char *thread_safe_queue_dequeue(ThreadSafeQueue *q) {
    if (!q) return NULL;

    pthread_mutex_lock(&q->mutex);
    ThreadSafeQueueNode *node = q->head;
    if (!node) {
        pthread_mutex_unlock(&q->mutex);
        return NULL;
    }
    const char *slice = node->slice;
    q->head = node->next;
    if (q->head == NULL) q->tail = NULL;
    q->count--;
    pthread_mutex_unlock(&q->mutex);

    free(node);
    return slice;
}

const char *thread_safe_queue_peek(ThreadSafeQueue *q) {
    if (!q) return NULL;

    pthread_mutex_lock(&q->mutex);
    const char *slice = (q->head ? q->head->slice : NULL);
    pthread_mutex_unlock(&q->mutex);

    return slice;
}

size_t thread_safe_queue_get_count(ThreadSafeQueue *q) {
    if (!q) return 0;

    pthread_mutex_lock(&q->mutex);
    size_t count = q->count;
    pthread_mutex_unlock(&q->mutex);
    return count;
}

void thread_safe_queue_destroy(ThreadSafeQueue *q) {
    if (!q) return;

    ThreadSafeQueueNode *node = q->head;
    while (node) {
        ThreadSafeQueueNode *temp = node;
        node = node->next;
        free(temp);
    }
    pthread_mutex_destroy(&q->mutex);
    free(q);
}
