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

// Converts and checks correctness of argument n.
int prog_cnt(char* arg) {
    int arg_converted = 0;
    while (*arg != '\0') {
        if ((int)(*arg) < (int)'0' || (int)'9' < (int)(*arg))
            fatal("Argument n is in wrong format\n");

        arg_converted = 10 * arg_converted + ((int)(*arg) - (int)'0');

        if (arg_converted > 16)
            fatal("Argument n is in wrong format\n");

        arg++;
    }
    return arg_converted;
}

int main(int argc, char* argv[]) {
    // Check arg correctness
    if (argc < 3)
        fatal("Arguments are in wrong format\n");
    for (int i = 0; i < argc; i++) {
        printf("%d -> %s\n", i, argv[i]);
    }

    int n = prog_cnt(argv[1]);

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

            ASSERT_SYS_OK(dup2(TEMP_DESC_1, OUT + i * n + j));
            ASSERT_SYS_OK(close(TEMP_DESC_1));

            ASSERT_SYS_OK(dup2(TEMP_DESC_2, IN + i * n + j));
            ASSERT_SYS_OK(close(TEMP_DESC_2));
        }
    }

    for (int i = 0; i < n; i++) {
        pid_t pid;
        ASSERT_SYS_OK(pid = fork());
        if (!pid) {
            char envvar_name[50];
            char envvar_value[50];
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