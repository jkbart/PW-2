/**
 * This file is for implementation of MIMPI library.
 * */
#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <stdint.h>

#include "channel.h"
#include "mimpi.h"
#include "mimpi_common.h"

#define META_DATA_MINI_BUFOR_SIZE 320

static int pow2[8] = {1, 2, 4, 8, 16, 32, 64, 128};

static int highest_pow2(int x) {
    for (int i = 0; i < 7; i++)
        if (pow2[i + 1] >= x)
            return i;
    return 0;
}

static int lowest_used_bit(int x) {
    for (int i = 0; i < 7; i++)
        if ((x & pow2[i]) != 0)
            return i;
    return 10000;
}

// static int max(int a, int b) { return a > b ? a : b; }
static int min(int a, int b) { return b > a ? a : b; }

static bool has_ended[N];

static int world_rank = -1;
static int world_size = -1;

static bool deadlock_detection;
static int deadlock_detection_messege_cnt = 0;

// ADD DEADLOCK TYPE (CURRENT DEADLOCK OS INCORRECT FOR PROCCESS THAT RECEIVES Deadlock_check (NOT FIRST SENDER))
enum messege_type_t {
    PtP_messege,
    Process_ended,
    Deadlock_check,
    Deadlock,
    Barrier,
    Bcast,
    Reduction,
};

struct meta_data_t {
    enum messege_type_t messege_type;
    int from;
    int count;
    int tag;
    int deadlock_cnt1;
    int deadlock_cnt2;
};

struct meta_data_being_send_t {
    enum messege_type_t messege_type;
    int from;
    int count_here;
    int count_not_here;
    int tag;
    int deadlock_cnt1;
    int deadlock_cnt2;
    uint8_t mini_bufor[META_DATA_MINI_BUFOR_SIZE];
};

struct messege_t {
    struct meta_data_t info;
    void *data;
    struct messege_t *next_messege;
    struct messege_t *prev_messege;
};

// ---- BEGIN Implementation of list of messeges

static struct messege_t *first_messege = NULL;
static struct messege_t *last_messege = NULL;

static void add_messege_to_list(struct messege_t *messege) {
    if (first_messege == NULL) {
        first_messege = last_messege = messege;
        messege->prev_messege = NULL;
        messege->next_messege = NULL;
    } else {
        last_messege->next_messege = messege;
        messege->prev_messege = last_messege;
        messege->next_messege = NULL;
        last_messege = messege;
    }
}

static void remove_messege_from_list(struct messege_t *messege) {
    if (messege->prev_messege == NULL) {
        first_messege = messege->next_messege;
    } else {
        messege->prev_messege->next_messege = messege->next_messege;
    }

    if (messege->next_messege == NULL) {
        last_messege = messege->prev_messege;
    } else {
        messege->next_messege->prev_messege = messege->prev_messege;
    }
    free(messege->data);
    free(messege);
}

// ---- END Implementation of list of messeges

// messege_handel_thread - thread that saves to bufor all incoming messeges
// mutex - blocks access to shared data (messege list and waitline)
static pthread_t messege_handler_thread;
static pthread_mutex_t mutex;

// wait_line_mutex - mutex to hold main program if wanted messege has not come yet
// wait_line - holds type of messege that main program is waiting for
// wanted_messege - place to save messege that main program was waiting for
static pthread_mutex_t wait_line_mutex;
static struct meta_data_t *wait_line = NULL;
static struct messege_t *wanted_messege = NULL;

// volatile bool mimpi_ended = false;

static int read_loop(int desc, void *data, int size) {
    int bytes_read = 0;
    int bytes_left_to_read = size;
    int read_result;

    while (bytes_left_to_read > 0) {
        read_result = chrecv(desc, data + bytes_read, bytes_left_to_read);
        // printf("(%d) read_loop result: %d\n", world_rank, read_result);

        if (read_result == -1 || read_result == 0)
            return -1;

        bytes_read += read_result;
        bytes_left_to_read -= read_result;
    }

    return 0;
}

static int write_loop(int desc, const void *data, int size) {
    if (data == NULL || size == 0)
        return 0;

    int bytes_wrote = 0;
    int bytes_left_to_write = size;
    int write_result;

    while (bytes_left_to_write > 0) {
        write_result = chsend(desc, data + bytes_wrote, bytes_left_to_write);

        if (write_result == -1)
            return -1;

        bytes_wrote += write_result;
        bytes_left_to_write -= write_result;
    }

    return 0;
}

static int send_messege(int where_to_rank, struct meta_data_t *info, const void *data) {
    if (has_ended[where_to_rank])
        return -1;

    struct meta_data_being_send_t info2 = {
        .messege_type   = info->messege_type,
        .from           = info->from,
        .count_here     =               min(info->count, META_DATA_MINI_BUFOR_SIZE),
        .count_not_here = info->count - min(info->count, META_DATA_MINI_BUFOR_SIZE),
        .tag            = info->tag,
        .deadlock_cnt1  = info->deadlock_cnt1,
        .deadlock_cnt2  = info->deadlock_cnt2
    };
        // printf("(%d) sending messege to %d -> %d %d %d %d\n", 
        //     world_rank, where_to_rank,
        //     info->messege_type,
        //     info->from,
        //     info->count,
        //     info->tag);
        // fflush(stdout);

    if (data != NULL)
        memcpy(&info2.mini_bufor, data, info2.count_here);

    int send_return = chsend(OUT + where_to_rank * world_size + where_to_rank, &info2, sizeof(struct meta_data_being_send_t));

    if (send_return == -1) return -1;
    ASSERT_ZERO(send_return - sizeof(struct meta_data_being_send_t));

    return write_loop(
        OUT + world_rank * world_size + where_to_rank, 
        data + info2.count_here,
        info2.count_not_here);
}

static bool messege_match(struct meta_data_t *messege, struct meta_data_t *waiting) {
    if (messege == NULL || waiting == NULL)
        return false;
    // printf("(%d) messege_match m1: %d %d %d %d\n",
    //     world_rank, 
    //     messege->messege_type,
    //     messege->from,
    //     messege->count,
    //     messege->tag);
    // printf("(%d) messege_match m2: %d %d %d %d\n", 
    //     world_rank, 
    //     waiting->messege_type,
    //     waiting->from,
    //     waiting->count,
    //     waiting->tag);
    // printf("(%d) messege_match: %d %d %d %d\n", 
    //     world_rank, 
    //     (messege->messege_type == waiting->messege_type),
    //     (messege->from == waiting->from),
    //     (messege->count == waiting->count),
    //     (messege->tag == 0 || waiting->tag == 0 || messege->tag == waiting->tag));
    // fflush(stdout);

    if (messege->messege_type == waiting->messege_type &&
        messege->from == waiting->from &&
        messege->count == waiting->count &&
        (messege->tag == 0 || waiting->tag == 0 || messege->tag == waiting->tag)) {
        return true;
    }

    return false;
}

static struct messege_t * find_match(struct meta_data_t *info) {
    struct messege_t *messege = first_messege;
    while (messege != NULL) {
        if (messege_match(&messege->info, info)) {
            // printf("(%d) find_match: messege was found\n", world_rank);
            return messege;
        }
        messege = messege->next_messege;
    }
    // printf("(%d) find_match: messege was not found\n", world_rank);
    return NULL;
}

static void handle_default_messege(struct messege_t *messege) {
    ASSERT_ZERO(pthread_mutex_lock(&mutex));

    // printf("(%d) PtP: messege status %d %d\n", world_rank, messege_match(&messege->info, wait_line), (wanted_messege == NULL));

    if (messege_match(&messege->info, wait_line) && wanted_messege == NULL) {
        // printf("(%d) PtP: messege was wanted\n", world_rank);
        wanted_messege = messege;
        ASSERT_ZERO(pthread_mutex_unlock(&wait_line_mutex));
    } else {
        // printf("(%d) PtP: messege added to list\n", world_rank);
        add_messege_to_list(messege);

    }

    ASSERT_ZERO(pthread_mutex_unlock(&mutex));

}

static void handle_PtP_messege(struct messege_t *messege) {
    handle_default_messege(messege);
}

static void handle_Process_ended(struct messege_t *messege) {
    ASSERT_ZERO(pthread_mutex_lock(&mutex));

    has_ended[messege->info.from] = true;

    if (wait_line != NULL && wanted_messege == NULL && (wait_line->from == messege->info.from)) {
        wanted_messege = messege;
        ASSERT_ZERO(pthread_mutex_unlock(&wait_line_mutex));
    }

    ASSERT_ZERO(pthread_mutex_unlock(&mutex));
}

static void handle_Deadlock_check(struct messege_t *messege) {
    ASSERT_ZERO(pthread_mutex_lock(&mutex));

    if (wait_line == NULL || wanted_messege != NULL) {
        // Deadlock is not possible.
    } else if (messege->info.from != world_rank) {
        // If messege was originally sent by other proccess
        if (wait_line->from == messege->info.from && wait_line->messege_type == PtP_messege) {
            // Deadlock could occure.
            // chsend(OUT + messege->info.from * world_size + messege->info.from, &messege->info, sizeof(struct meta_data_t));
            messege->info.deadlock_cnt2 = wait_line->deadlock_cnt1;
            send_messege(messege->info.from, &messege->info, NULL);
        }
    } else if (wait_line->deadlock_cnt1 == messege->info.deadlock_cnt1) {
        // Our deadlock messege got back ==> deadlock occured.
        // We first send messege about deadlock to other process.
        struct messege_t *deadlock_messege = malloc(sizeof(struct messege_t));
        deadlock_messege->info.messege_type = Deadlock;
        deadlock_messege->info.from = world_rank;
        deadlock_messege->info.count = 0;
        deadlock_messege->info.deadlock_cnt1 = messege->info.deadlock_cnt2;
        deadlock_messege->data = NULL;

        send_messege(wait_line->from, &deadlock_messege->info, NULL);
        wanted_messege = deadlock_messege;
        ASSERT_ZERO(pthread_mutex_unlock(&wait_line_mutex));
    }

    free(messege->data);
    free(messege);

    ASSERT_ZERO(pthread_mutex_unlock(&mutex));
}

static void handle_Deadlock(struct messege_t *messege) {
    ASSERT_ZERO(pthread_mutex_lock(&mutex));

    if (wait_line != NULL && wanted_messege == NULL && wait_line->deadlock_cnt1 == messege->info.deadlock_cnt1) {
        wanted_messege = messege;
        ASSERT_ZERO(pthread_mutex_unlock(&wait_line_mutex));
    }

    ASSERT_ZERO(pthread_mutex_unlock(&mutex));
}

static void handle_Barrier(struct messege_t *messege) {
    handle_default_messege(messege);
}

static void handle_Bcast(struct messege_t *messege) {
    handle_default_messege(messege);
}

static void handle_Reduction(struct messege_t *messege) {
    handle_default_messege(messege);
}

static void *messege_handler(void *arg) {
    while (true) {

        // Part 1 - reading metadata
        struct messege_t *messege = malloc(sizeof(struct messege_t));
        struct meta_data_being_send_t info;
        
        // printf("(%d) handler: waiting for messege\n", world_rank);
        if (read_loop(IN + world_rank * world_size + world_rank, &info, sizeof(struct meta_data_being_send_t)) == -1) {
            free(messege);
            // printf("(%d) handler ending\n", world_rank);
            return NULL;
        }

        messege->info.messege_type  = info.messege_type;
        messege->info.from          = info.from;
        messege->info.count         = info.count_here + info.count_not_here;
        messege->info.tag           = info.tag;
        messege->info.deadlock_cnt1 = info.deadlock_cnt1;
        messege->info.deadlock_cnt2 = info.deadlock_cnt2;

        // printf("(%d) handler: got messege -> %d %d %d %d\n", 
        //     world_rank, 
        //     messege->info.messege_type,
        //     messege->info.from,
        //     messege->info.count,
        //     messege->info.tag);
        // fflush(stdout);

        // Part 2 - reading actual data
        if (messege->info.count > 0) {
            messege->data = malloc(messege->info.count);
            memcpy(messege->data, &info.mini_bufor, info.count_here);

            if (read_loop(IN + (info.from) * world_size + world_rank, messege->data + info.count_here, info.count_not_here) == -1) {
                free(messege->data);
                free(messege);
                // printf("(%d) handler ending\n", world_rank);
                return NULL;
            }
        } else {
            messege->data = NULL;
        }
        // Part 3 - handling diffrent operations
        switch (messege->info.messege_type) {
        case PtP_messege:
            handle_PtP_messege(messege);
            break;
        case Process_ended:
            if (messege->info.from == world_rank) {
                free(messege->data);
                free(messege);
                // printf("(%d) handler ending\n", world_rank);
                return NULL;
            }
            handle_Process_ended(messege);
            break;
        case Deadlock_check:
            handle_Deadlock_check(messege);
            break;
        case Deadlock:
            handle_Deadlock(messege);
            break;
        case Barrier:
            handle_Barrier(messege);
            break;
        case Bcast:
            handle_Bcast(messege);
            break;
        case Reduction:
            handle_Reduction(messege);
            break;
        }
    }
}

void MIMPI_Init(bool enable_deadlock_detection) {
    channels_init();

    deadlock_detection = enable_deadlock_detection;
    ASSERT_ZERO(pthread_mutex_init(&mutex, NULL));
    ASSERT_ZERO(pthread_mutex_init(&wait_line_mutex, NULL));
    ASSERT_ZERO(pthread_mutex_lock(&wait_line_mutex));

    // Reading back env variables.
    char envvar_name[ENVVAR_LEN];
    sprintf(envvar_name, "MIMPI_WORLD_SIZE");
    world_size = string_to_no(getenv(envvar_name));

    sprintf(envvar_name, "MIMPI_%d", getpid());
    world_rank = string_to_no(getenv(envvar_name));

    // printf("(%d) MIMPI_Init\n", world_rank);

    ASSERT_ZERO(pthread_create(&messege_handler_thread, NULL, messege_handler, NULL));
}

void MIMPI_Finalize() {
    // printf("(%d) MIMPI_Finalize\n", world_rank);
    // fflush(stdout);

    struct meta_data_t info = {.messege_type = Process_ended, .from = world_rank, .count = 0, .tag = 0};
    // chsend(OUT + world_rank * world_size + world_rank, &self_info, sizeof(struct meta_data_t));
    send_messege(world_rank, &info, NULL);

    // printf("(%d) MIMPI_Finalize: signaled to handler\n", world_rank);
    // fflush(stdout);

    ASSERT_ZERO(pthread_join(messege_handler_thread, NULL));

    for (int i = 0; i < world_size; i++) {
        if (i != world_rank) {
            // printf("(%d) MIMPI_Finalize sending end code to %d\n", world_rank, i);
            // chsend(OUT + i * world_size + i, &info, sizeof(struct meta_data_t));
            send_messege(i, &info, NULL);
        }
    }

    // TODO: check if (i -> i) descriptors dont have to be closed first.
    // Closing opened descriptors: signal to messege_handler to exit. 
    for (int i1 = 0; i1 < world_size; i1++) {
        for (int i2 = 0; i2 < world_size; i2++) {
            // if ((i2 != world_rank && i1 == world_rank) || (i1 == i2 && i2 != world_rank)) {
            if ((i2 != world_rank && i1 == world_rank) || (i1 == i2)) {
                // printf("%d DESC IN (%d -> %d) %d\n", getpid(), i1, i2, world_rank);
                ASSERT_SYS_OK(close(OUT + i1 * world_size + i2));
            }

            if (i2 == world_rank) {
                // printf("%d DESC OUT (%d -> %d) %d\n", getpid(), i1, i2, world_rank);
                ASSERT_SYS_OK(close(IN + i1 * world_size + i2));
            }
        }
    }

    // mimpi_ended = true;

    ASSERT_ZERO(pthread_mutex_unlock(&wait_line_mutex));
    ASSERT_ZERO(pthread_mutex_destroy(&wait_line_mutex));
    ASSERT_ZERO(pthread_mutex_destroy(&mutex));

    // Cleaning bufor
    struct messege_t *next_to_erase = first_messege;
    while (next_to_erase != NULL) {
        struct messege_t *current_to_erase = next_to_erase;
        next_to_erase = current_to_erase->next_messege;

        free(current_to_erase->data);
        free(current_to_erase);
    }

    channels_finalize();

    // print_open_descriptors();
    // printf("(%d) MIMPI_Finalize ENDED\n", world_rank);
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
    if (destination == world_rank)
        return MIMPI_ERROR_ATTEMPTED_SELF_OP;

    if (destination < 0 || world_size <= destination)
        return MIMPI_ERROR_NO_SUCH_RANK;

    struct meta_data_t info = {
        .messege_type = PtP_messege, 
        .from         = world_rank, 
        .count        = count, 
        .tag          = tag
    };

    // printf("(%d) SEND: %d %d %d %d\n",
    //     world_rank, 
    //     info.messege_type,
    //     info.from,
    //     info.count,
    //     info.tag);
    // fflush(stdout);

    pthread_mutex_lock(&mutex);
    int has_dest_ended = has_ended[destination];
    pthread_mutex_unlock(&mutex);

    if (has_dest_ended || send_messege(destination, &info, data) == -1){
        return MIMPI_ERROR_REMOTE_FINISHED;
    }

    return MIMPI_SUCCESS;
}

MIMPI_Retcode MIMPI_Recv(
    void *data,
    int count,
    int source,
    int tag
) {
    if (source == world_rank)
        return MIMPI_ERROR_ATTEMPTED_SELF_OP;

    if (source < 0 || world_size <= source)
        return MIMPI_ERROR_NO_SUCH_RANK;

    deadlock_detection_messege_cnt++;
    struct meta_data_t info = {
        .messege_type  = PtP_messege, 
        .from          = source, 
        .count         = count, 
        .tag           = tag, 
        .deadlock_cnt1 = deadlock_detection_messege_cnt
    };

    // printf("(%d) RECV: %d %d %d %d\n",
    //     world_rank, 
    //     info.messege_type,
    //     info.from,
    //     info.count,
    //     info.tag);
    // fflush(stdout);

    pthread_mutex_lock(&mutex);

    // Check if answer to request can be determined now
    struct messege_t *ans = find_match(&info);
    if (ans != NULL) {
        memcpy(data, ans->data, count);
        remove_messege_from_list(ans);
        pthread_mutex_unlock(&mutex);
        return MIMPI_SUCCESS;
    } else if (has_ended[source]) {
        pthread_mutex_unlock(&mutex);
        return MIMPI_ERROR_REMOTE_FINISHED;
    }

    if (deadlock_detection) {
        struct meta_data_t d_info = {
            .messege_type  = Deadlock_check, 
            .from          = world_rank, 
            .count         = 0, 
            .tag           = tag, 
            .deadlock_cnt1 = deadlock_detection_messege_cnt
        };

        send_messege(source, &d_info, NULL);
    }

    // Wait until answer can be determined
    wait_line = &info;
    wanted_messege = NULL;

    pthread_mutex_unlock(&mutex);

    pthread_mutex_lock(&wait_line_mutex);
    pthread_mutex_lock(&mutex);

    MIMPI_Retcode return_code;

    if (wanted_messege->info.messege_type == PtP_messege) {
        memcpy(data, wanted_messege->data, count);
        return_code = MIMPI_SUCCESS;
    } else if (wanted_messege->info.messege_type == Process_ended) {
        return_code = MIMPI_ERROR_REMOTE_FINISHED;
    } else if (wanted_messege->info.messege_type == Deadlock) {
        return_code = MIMPI_ERROR_DEADLOCK_DETECTED;
    }

    free(wanted_messege->data);
    free(wanted_messege);

    wanted_messege = NULL;
    wait_line = NULL;

    pthread_mutex_unlock(&mutex);

    return return_code;
}

MIMPI_Retcode MIMPI_Barrier() {
    // printf("(%d) MIMPI_Barrier enter\n", world_rank);
    // int level = 0;

    // while (pow2[level] < world_size) {
    for (int level = 0; pow2[level] < world_size; level++) {
        /// sending messege forward
        int where_to = (world_size + world_rank - pow2[level]) % world_size;

        struct meta_data_t info_to_send = {
            .messege_type = Barrier, 
            .from         = world_rank, 
            .count        = 0, 
            .tag          = 0,
        };
        // int send_return = chsend(OUT + where_to * world_size + where_to, &info, sizeof(struct meta_data_t));
        // chsend(OUT + where_to * world_size + where_to, &info, sizeof(struct meta_data_t));
        send_messege(where_to, &info_to_send, NULL);

        // if (send_return != sizeof(struct meta_data_t)) {
        //     return MIMPI_ERROR_REMOTE_FINISHED;
        // }

        // Waiting for messege from back
        pthread_mutex_lock(&mutex);


        int where_from = (world_rank + pow2[level]) % world_size;

        struct meta_data_t info_to_recv = {
            .messege_type = Barrier, 
            .from         = where_from, 
            .count        = 0, 
            .tag          = 0,
        };

        struct messege_t *ans = find_match(&info_to_recv);

        if (ans != NULL) {
            remove_messege_from_list(ans);
            pthread_mutex_unlock(&mutex);
        } else {
            if (has_ended[where_from]) {
                pthread_mutex_unlock(&mutex);
                return MIMPI_ERROR_REMOTE_FINISHED;
            }

            wait_line = &info_to_recv;
            wanted_messege = NULL;
            pthread_mutex_unlock(&mutex);

            pthread_mutex_lock(&wait_line_mutex);
            pthread_mutex_lock(&mutex);

            if (wanted_messege->info.messege_type == Process_ended) {
                free(wanted_messege->data);
                free(wanted_messege);
                pthread_mutex_unlock(&mutex);
                return MIMPI_ERROR_REMOTE_FINISHED;
            } // else {;}

            free(wanted_messege->data);
            free(wanted_messege);

            wait_line = NULL;
            wanted_messege = NULL;

            pthread_mutex_unlock(&mutex);
        }
    }

    return MIMPI_SUCCESS;
}

int get_level_of_getting_data(int rank, int root) {
    return lowest_used_bit((world_size + root - rank) % world_size);
}

MIMPI_Retcode MIMPI_Bcast(
    void *data,
    int count,
    int root
) {
    // int level = highest_pow2(world_size);
    void *data_cpy = malloc(count);

    if (world_rank == root)
        memcpy(data_cpy, data, count);

    int level_of_getting_data = get_level_of_getting_data(world_rank, root);

    // printf("(%d) BCAST entry : %d \n",
    //     world_rank,
    //     level_of_getting_data);
    // fflush(stdout);

    // while (level >= 0) {
    for (int level = highest_pow2(world_size); level >= 0; level--) {
        // Sending messege
        int where_to = (world_size + world_rank - pow2[level]) % world_size;
        struct meta_data_t info_to_send = {
            .messege_type = Bcast, 
            .from = world_rank, 
            .count = (level == get_level_of_getting_data(where_to, root)) ? count : 0, 
            .tag = 0, 
        };

        // chsend(OUT + where_to * world_size + where_to, &info_to_send, sizeof(struct meta_data_t));
        // if (will_send_data)
        //     write_loop(OUT + world_rank * world_size + where_to, data_cpy, count);

        send_messege(where_to, &info_to_send, data_cpy);

        // printf("(%d) BCAST sending to (%d) -> %d %d %d %d\n", 
        //     world_rank, where_to,
        //     info_to_send.messege_type,
        //     info_to_send.from,
        //     info_to_send.count,
        //     info_to_send.tag);
        // fflush(stdout);

        // Reading messege
        int where_from = (world_rank + pow2[level]) % world_size;
        struct meta_data_t info_to_recv = {
            .messege_type = Bcast, 
            .from = where_from, 
            .count = (level == level_of_getting_data) ? count : 0, 
            .tag = 0, 
        };

        // printf("(%d) BCAST waiting -> %d %d %d %d\n", 
        //     world_rank, 
        //     info_to_recv.messege_type,
        //     info_to_recv.from,
        //     info_to_recv.count,
        //     info_to_recv.tag);
        // fflush(stdout);

        pthread_mutex_lock(&mutex);

        struct messege_t *ans = find_match(&info_to_recv);

        if (ans != NULL) {
            if (level == level_of_getting_data)
                memcpy(data_cpy, ans->data, count);

            remove_messege_from_list(ans);

            pthread_mutex_unlock(&mutex);
        } else {
            if (has_ended[where_from]){
                pthread_mutex_unlock(&mutex);
                return MIMPI_ERROR_REMOTE_FINISHED;
            }

            wait_line = &info_to_recv;
            wanted_messege = NULL;
            pthread_mutex_unlock(&mutex);

            pthread_mutex_lock(&wait_line_mutex);
            pthread_mutex_lock(&mutex);

            if (wanted_messege->info.messege_type == Process_ended) {
                free(wanted_messege->data);
                free(wanted_messege);
                pthread_mutex_unlock(&mutex);
                return MIMPI_ERROR_REMOTE_FINISHED;
            }

            if (level == level_of_getting_data)
                memcpy(data_cpy, wanted_messege->data, count);

            free(wanted_messege->data);
            free(wanted_messege);

            wait_line = NULL;
            wanted_messege = NULL;

            pthread_mutex_unlock(&mutex);
        }
    }

    if (world_rank != root)
        memcpy(data, data_cpy, count);

    return MIMPI_SUCCESS;
}

void perform_operation(uint8_t *data, uint8_t *new_data, int length, MIMPI_Op op) {
    // printf("(%d) PO start\n", world_rank);
    //     fflush(stdout);
    for (int i = 0; i < length; i++) {
        // uint8_t a = data[i];
        // uint8_t b = new_data[i];

        switch(op) {
        case(MIMPI_MAX):
            if (new_data[i] > data[i])
                data[i] = new_data[i];
            break;
        case(MIMPI_MIN):
            if (new_data[i] < data[i])
                data[i] = new_data[i];
            break;
        case(MIMPI_SUM):
            data[i] += new_data[i];
            break;
        case(MIMPI_PROD):
            data[i] *= new_data[i];
            break;
        }
        // if (i < 5) printf("(%d) PO %d op %d = %d\n", world_rank, a, b, data[i]);
    }
    // printf("(%d) PO end\n", world_rank);
    //     fflush(stdout);
}

int get_sending_level(int rank, int root) {
    if (root == rank)
        return -1;
    return lowest_used_bit((world_size - root + rank) % world_size);
}

MIMPI_Retcode MIMPI_Reduce(
    void const *send_data,
    void *recv_data,
    int count,
    MIMPI_Op op,
    int root
) {
    uint8_t *data_cpy = malloc(count);
    int array_length = count;
    memcpy(data_cpy, send_data, count);

    int level_of_sending_data = get_sending_level(world_rank, root);

    // printf("(%d) REDUCE entry : %d \n",
    //     world_rank,
    //     level_of_sending_data);
    // fflush(stdout);

    for (int level = 0; level <= highest_pow2(world_size); level++) {
        bool will_send_data = (level == level_of_sending_data);
        // Sending messege
        int where_to = (world_size + world_rank - pow2[level]) % world_size;
        struct meta_data_t info_to_send = {
            .messege_type = Reduction, 
            .from = world_rank, 
            .count = will_send_data ? count : 0, 
            .tag = op + 1, 
        };

        send_messege(where_to, &info_to_send, data_cpy);


        // printf("(%d) REDUCE sending to (%d) -> %d %d %d %d\n", 
        //     world_rank, where_to,
        //     info_to_send.messege_type,
        //     info_to_send.from,
        //     info_to_send.count,
        //     info_to_send.tag);
        // fflush(stdout);

        // Reading messege
        int where_from = (world_rank + pow2[level]) % world_size;
        bool will_receive_data = (get_sending_level(where_from, root) == level);
        struct meta_data_t info_to_recv = {
            .messege_type = Reduction, 
            .from = where_from, 
            .count = will_receive_data ? count : 0, 
            .tag = op + 1, 
        };

        // printf("(%d) REDUCE waiting -> %d %d %d %d\n", 
        //     world_rank, 
        //     info_to_recv.messege_type,
        //     info_to_recv.from,
        //     info_to_recv.count,
        //     info_to_recv.tag);
        // fflush(stdout);

        pthread_mutex_lock(&mutex);

        struct messege_t *ans = find_match(&info_to_recv);

        // printf("(%d) reduce on 1\n", world_rank);

        if (ans != NULL) {
            if (will_receive_data)
                perform_operation(data_cpy, ans->data, array_length, op);

            remove_messege_from_list(ans);

            pthread_mutex_unlock(&mutex);
        } else {
            if (has_ended[where_from]) {
                pthread_mutex_unlock(&mutex);
                return MIMPI_ERROR_REMOTE_FINISHED;
            }

            wait_line = &info_to_recv;
            wanted_messege = NULL;
            pthread_mutex_unlock(&mutex);

        // printf("(%d) reduce on 2\n", world_rank);
            pthread_mutex_lock(&wait_line_mutex);
            pthread_mutex_lock(&mutex);
        // printf("(%d) reduce on 3\n", world_rank);

            if (wanted_messege->info.messege_type == Process_ended) {
                free(wanted_messege->data);
                free(wanted_messege);
                pthread_mutex_unlock(&mutex);
        // printf("(%d) reduce on 4\n", world_rank);
                return MIMPI_ERROR_REMOTE_FINISHED;
            }

            if (will_receive_data) {
        // printf("(%d) reduce on 5\n", world_rank);
                perform_operation(data_cpy, wanted_messege->data, array_length, op);
        // printf("(%d) reduce on 5.5 %d\n", world_rank, data_cpy[0]);
            }

        // printf("(%d) reduce on 6\n", world_rank);
            free(wanted_messege->data);
            free(wanted_messege);

            wait_line = NULL;
            wanted_messege = NULL;

            pthread_mutex_unlock(&mutex);
        }
    }
    
    if (world_rank == root)
        memcpy(recv_data, data_cpy, count);

    return MIMPI_SUCCESS;
}