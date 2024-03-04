/**
 * This file is for implementation of mimpirun program.
 * */
#include "mimpi_common.h"
#include "channel.h"

#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#define TEMP_DESC_1 1000
#define TEMP_DESC_2 1001

int main(int argc, char* argv[]) {
    if (argc < 3)
        fatal("Arguments are in wrong format\n");

    int n = string_to_no(argv[1]);
    if (n < 1 || 16 < n)
        fatal("Argument n is in wrong format\n");

    for (int i = 0; i < n; i++) {
        for (int j = 0; j < n; j++) {

            int pipefd[2];
            ASSERT_SYS_OK(channel(pipefd));

            if (pipefd[0] != TEMP_DESC_1) {
                ASSERT_SYS_OK(dup2(pipefd[0], TEMP_DESC_1));
                ASSERT_SYS_OK(close(pipefd[0]));
            }

            if (pipefd[1] != TEMP_DESC_2) {
                ASSERT_SYS_OK(dup2(pipefd[1], TEMP_DESC_2));
                ASSERT_SYS_OK(close(pipefd[1]));
            }

            ASSERT_SYS_OK(dup2(TEMP_DESC_1, IN + i * n + j));
            ASSERT_SYS_OK(close(TEMP_DESC_1));

            ASSERT_SYS_OK(dup2(TEMP_DESC_2, OUT + i * n + j));
            ASSERT_SYS_OK(close(TEMP_DESC_2));
        }
    }

    char envvar_name[ENVVAR_LEN];
    char envvar_value[ENVVAR_LEN];

    sprintf(envvar_name, "MIMPI_WORLD_SIZE");
    sprintf(envvar_value, "%d", n);
    
    ASSERT_SYS_OK(setenv(envvar_name, envvar_value, 1));

    for (int i = 0; i < n; i++) {
        pid_t pid;
        ASSERT_SYS_OK(pid = fork());
        if (!pid) {
            for (int i1 = 0; i1 < n; i1++) {
                for (int i2 = 0; i2 < n; i2++) {
                    if (!((i2 != i && i1 == i) || (i1 == i2))) {
                        ASSERT_SYS_OK(close(OUT + i1 * n + i2));
                    }

                    if (!(i2 == i)) {
                        ASSERT_SYS_OK(close(IN + i1 * n + i2));
                    }
                }
            }

            sprintf(envvar_name, "MIMPI_%d", getpid());
            sprintf(envvar_value, "%d", i);

            ASSERT_SYS_OK(setenv(envvar_name, envvar_value, 1));

            ASSERT_SYS_OK(execvp(argv[2], argv + 2));
        }
    }

    for (int i1 = 0; i1 < n; i1++) {
        for (int i2 = 0; i2 < n; i2++) {
            ASSERT_SYS_OK(close(IN + i1 * n + i2));
            ASSERT_SYS_OK(close(OUT + i1 * n + i2));
        }
    }

    for (int i = 0; i < n; i++)
        wait(NULL);
}
