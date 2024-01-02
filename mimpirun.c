/**
 * This file is for implementation of mimpirun program.
 * */
#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include "mimpi_common.h"
#include "channel.h"

int main(int argc, char* argv[]) {
    // Check arg correctness
    if (argc < 3)
        fatal("Arguments are in wrong format\n");
    for (int i = 0; i < argc; i++) {
        printf("%d -> %s\n", i, argv[i]);
    }

    int n = string_to_no(argv[1]);
    if (n < 1 || 16 < n)
        fatal("Argument n is in wrong format\n");

    for (int i = 0; i < n; i++) {
        for (int j = 0; j < n; j++) {
            if (i == j)
                continue;

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

            ASSERT_SYS_OK(dup2(TEMP_DESC_1, OUT + i * n + j));
            ASSERT_SYS_OK(close(TEMP_DESC_1));

            ASSERT_SYS_OK(dup2(TEMP_DESC_2, IN + i * n + j));
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
                    if (!(i2 != i))
                        ASSERT_SYS_OK(close(OUT + i1 * n + i2));
                    if (!((i1 == i && i2 != i) || (i1 == i2 && i1 != i)))
                        ASSERT_SYS_OK(close(IN + i1 * n + i2));
                }
            }

            sprintf(envvar_name, "MIMPI_%d", getpid());
            sprintf(envvar_value, "%d", i);

            // printf("MIMPI_%d\n", getpid());
            // printf("env_name  -> %s\n", envvar_name);
            // printf("env_value -> %s\n", envvar_value);
            ASSERT_SYS_OK(setenv(envvar_name, envvar_value, 1));
            // printf("return    -> %s\n", getenv(envvar_name));
            ASSERT_SYS_OK(execvp(argv[2], argv + 2));
            // exit(0);
        }
    }
    for (int i = 0; i < n; i++)
        wait(NULL);
    
    // printf("exit\n");
}