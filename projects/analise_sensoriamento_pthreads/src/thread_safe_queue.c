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

    printf("[QUEUE] Created new queue %p\n", (void *)q);
    return q;
}

int thread_safe_queue_enqueue(ThreadSafeQueue *q, const char *slice,
                              size_t slice_len, const char *header,
                              size_t header_len) {
    if (!q) return -1;

    ThreadSafeQueueNode *node = malloc(sizeof(ThreadSafeQueueNode));
    if (!node) {
        perror("Failed to allocate queue node");
        return -1;
    }
    node->slice = slice;
    node->slice_len = slice_len;
    node->header = header;
    node->header_len = header_len;
    node->next = NULL;

    pthread_mutex_lock(&q->mutex);
    if (q->tail == NULL) {
        q->head = q->tail = node;
    } else {
        q->tail->next = node;
        q->tail = node;
    }
    q->count++;
    printf(
        "[QUEUE-%p] Enqueued node %p with slice %p (len=%zu), new count: %zu\n",
        (void *)q, (void *)node, slice, slice_len, q->count);
    pthread_mutex_unlock(&q->mutex);

    return 0;
}

int thread_safe_queue_dequeue(ThreadSafeQueue *q, const char **slice,
                              size_t *slice_len, const char **header,
                              size_t *header_len) {
    if (!q || !slice || !slice_len || !header || !header_len) return -1;

    pthread_mutex_lock(&q->mutex);
    ThreadSafeQueueNode *node = q->head;
    if (!node) {
        pthread_mutex_unlock(&q->mutex);
        return -1;
    }
    *slice = node->slice;
    *slice_len = node->slice_len;
    *header = node->header;
    *header_len = node->header_len;
    q->head = node->next;
    if (q->head == NULL) q->tail = NULL;
    q->count--;
    printf(
        "[QUEUE-%p] Dequeued node %p with slice %p (len=%zu), remaining count: "
        "%zu\n",
        (void *)q, (void *)node, *slice, *slice_len, q->count);
    pthread_mutex_unlock(&q->mutex);

    free(node);
    return 0;
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

    printf("[QUEUE] Destroying queue %p\n", (void *)q);

    ThreadSafeQueueNode *node = q->head;
    while (node) {
        ThreadSafeQueueNode *temp = node;
        node = node->next;
        // Note: We're NOT freeing temp->slice here, as it should be
        // freed by the consumer of the queue data
        printf(
            "[QUEUE-%p] Freeing node %p during destruction (not freeing slice "
            "%p)\n",
            (void *)q, (void *)temp, temp->slice);
        free(temp);
    }
    pthread_mutex_destroy(&q->mutex);
    free(q);
}
