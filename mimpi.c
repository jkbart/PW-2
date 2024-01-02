/**
 * This file is for implementation of MIMPI library.
 * */
#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include "channel.h"
#include "mimpi.h"
#include "mimpi_common.h"

int world_rank = -1;
int world_size = -1;

bool deadlock_detection;

enum operation_type {
    PtP_send,
    PtP_recv
};

struct meta_data_t {
    operation_type op;
    int from;
    int count;
    int tag;
};

struct messege {
    struct meta_data_t info;
    void *data;
    struct messege *next_messege;
}

pthread_t messege_handler_thread;
pthread_mutex_t mutex;

struct messege *first = NULL;
struct messege *last = NULL;

struct meta_data_t *wait_line = NULL;

volatile bool mimpi_ended = false;

void messege_handler(void *arg) {
    while (true) {
        // Part 1 - reading metadata
        struct messege *data = NULL;

    }
}

void MIMPI_Init(bool enable_deadlock_detection) {
    channels_init();

    deadlock_detection = enable_deadlock_detection;
    ASSERT_ZERO(pthread_mutex_init(&b->mutex, NULL));

    // Reading back env variables.
    char envvar_name[ENVVAR_LEN];
    sprintf(envvar_name, "MIMPI_WORLD_SIZE");
    world_size = string_to_no(getenv(envvar_name));

    sprintf(envvar_name, "MIMPI_%d", getpid());
    world_rank = string_to_no(getenv(envvar_name));

    ASSERT_ZERO(pthread_create(&messege_handler_thread, NULL, messege_handler, NULL));
}

void MIMPI_Finalize() {
    // Closing opened descriptors: signal to messege_handler to exit. 
    for (int i1 = 0; i1 < world_size; i1++) {
        for (int i2 = 0; i2 < world_size; i2++) {
            if (!(i2 != world_rank))
                ASSERT_SYS_OK(close(OUT + i1 * n + i2));
            if (!((i1 == world_rank && i2 != world_rank) || (i1 == i2 && i1 != world_rank)))
                ASSERT_SYS_OK(close(IN + i1 * n + i2));
        }
    }

    mimpi_ended = true;

    ASSERT_ZERO(pthread_join(messege_handler_thread, NULL));
    ASSERT_ZERO(pthread_mutex_destroy(&b->mutex));
    channels_finalize();
}

int MIMPI_World_size() {
    return world_size;
}

int MIMPI_World_rank() {
    return world_rank;
}

MIMPI_Retcode MIMPI_Send(
    void const *data,
    int count,
    int destination,
    int tag
) {
    TODO
}

MIMPI_Retcode MIMPI_Recv(
    void *data,
    int count,
    int source,
    int tag
) {
    TODO
}

MIMPI_Retcode MIMPI_Barrier() {
    TODO
}

MIMPI_Retcode MIMPI_Bcast(
    void *data,
    int count,
    int root
) {
    TODO
}

MIMPI_Retcode MIMPI_Reduce(
    void const *send_data,
    void *recv_data,
    int count,
    MIMPI_Op op,
    int root
) {
    TODO
}