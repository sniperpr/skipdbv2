#include <stdlib.h>
#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <sys/fcntl.h> // fcntl
#include <unistd.h> // close
#include <sys/types.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <sys/time.h>
#include <time.h>
#include <limits.h> // [LONG|INT][MIN|MAX]
#include <errno.h>  // errno
#include <unistd.h>
#include "skipd.h"

typedef struct _dbclient {
    int remote_fd;
    char command[DELAY_KEY_LEN+1];
    char key[DELAY_KEY_LEN+1];
    time_t timeout;

    char* buf;
    int buf_max;
    int buf_len;
    int buf_pos;
} dbclient;

int create_client_fd(char* sock_path) {
    int len, remote_fd;
    struct sockaddr_un remote;

    if(-1 == (remote_fd = socket(AF_UNIX, SOCK_STREAM, 0))) {
        perror("socket");
        return -1;
    }

    remote.sun_family = AF_UNIX;
    strcpy(remote.sun_path, sock_path);
    len = strlen(remote.sun_path) + sizeof(remote.sun_family);
    if(-1 == connect(remote_fd, (struct sockaddr*)&remote, len)) {
        perror("connect");
        close(remote_fd);
        return -1;
    }

    return remote_fd;
}

int setnonblock(int fd) {
    int flags;

    flags = fcntl(fd, F_GETFL);
    flags |= O_NONBLOCK;
    return fcntl(fd, F_SETFL, flags);
}

static void check_buf(dbclient* client, int len) {
    int clen = _max(len, BUF_MAX);

    if((NULL != client->buf) && (client->buf_max < clen)) {
        free(client->buf);
        client->buf = NULL;
    }

    if(NULL == client->buf) {
        client->buf = (char*)malloc(clen+1);
        client->buf_max = clen;
    }
}

int read_util(dbclient* client, int len) {
    int clen, n;
    time_t now;

    check_buf(client, len);

    for(;;) {
        clen = len - client->buf_pos;
        n = recv(client->remote_fd, client->buf + client->buf_pos, clen, 0);
        if(n < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                now = time(NULL);
                if(now > client->timeout) {
                    break;
                }

                usleep(50);
                continue;
            }
            //timeout
            return -2;
        } else if(n == 0) {
            //socket closed
            return -1;
        } else {
            client->buf_pos += n;
            if(client->buf_pos == len) {
                //read ok
                return 0;
            }
        }
    }

    //unkown error
    return -3;
}

int parse_common_result(dbclient *client) {
    int n1, n2;
    char* magic = MAGIC;

    do {
        client->timeout = time(NULL) + 110;

        client->buf_pos = 0;
        n1 = read_util(client, HEADER_PREFIX);
        if(n1 < 0) {
            return n1;
        }

        if(0 != memcmp(client->buf, magic, MAGIC_LEN)) {
            //message error
            return -3;
        }

        client->buf[HEADER_PREFIX-1] = '\0';
        if(S2ISUCCESS != str2int(&n2, client->buf+MAGIC_LEN, 10)) {
            //message error
            return -4;
        }

        client->buf_pos = 0;
        client->timeout = time(NULL) + 510;
        n1 = read_util(client, n2);
        if(n1 < 0) {
            return n1;
        }

        client->buf[n2] = '\0';

        if(client->buf[n2-1] != '\n') {
            //TODO fix hear
            printf("%s\n", client->buf);
        } else {
            printf("%s", client->buf);
        }
    } while(0);

    return 0;
}

int parse_list_result(dbclient *client) {
    int n1, n2;
    char* magic = MAGIC;

    for(;;) {
        client->timeout = time(NULL) + 110;

        client->buf_pos = 0;
        n1 = read_util(client, HEADER_PREFIX);
        if(n1 < 0) {
            return n1;
        }

        if(0 != memcmp(client->buf, magic, MAGIC_LEN)) {
            //message error
            return -3;
        }

        client->buf[HEADER_PREFIX-1] = '\0';
        if(S2ISUCCESS != str2int(&n2, client->buf+MAGIC_LEN, 10)) {
            //message error
            return -4;
        }

        client->buf_pos = 0;
        client->timeout = time(NULL) + 510;
        n1 = read_util(client, n2);
        if(n1 < 0) {
            return n1;
        }

        client->buf[n2] = '\0';

        if(NULL != strstr(client->buf, "__end__")) {
            break;
        }
        if(client->buf[n2-1] != '\n') {
            //TODO fix hear
            printf("%s\n", client->buf);
        } else {
            printf("%s", client->buf);
        }
    }

    return 0;
}

int parse_script_result(dbclient *client) {
    int n1, n2;
    char *p1, *p2;
    char* magic = MAGIC;

    for(;;) {
        client->timeout = time(NULL) + 110;

        client->buf_pos = 0;
        n1 = read_util(client, HEADER_PREFIX);
        if(n1 < 0) {
            return n1;
        }

        if(0 != memcmp(client->buf, magic, MAGIC_LEN)) {
            //message error
            return -3;
        }

        client->buf[HEADER_PREFIX-1] = '\0';
        if(S2ISUCCESS != str2int(&n2, client->buf+MAGIC_LEN, 10)) {
            //message error
            return -4;
        }

        client->buf_pos = 0;
        client->timeout = time(NULL) + 510;
        n1 = read_util(client, n2);
        if(n1 < 0) {
            return n1;
        }

        client->buf[n2] = '\0';
        if(NULL != strstr(client->buf, "__end__")) {
            break;
        }

        p1 = client->buf + 5;
        p2 = strstr(p1, " ");
        *p2 = '\0';
        p2++;
        if(client->buf[n2-1] == '\n') {
            client->buf[n2-1] = '\0';
        } 
        printf("export %s=%s;", p1, p2);
    }

    return 0;
}

static void update_key(dbclient* client, char* prefix, char* envp[]) {
    char** env;
    char *p1, *p2;
    int n1, n2, nenv, nc, nkey, np = strlen(prefix);

    strcpy(client->command, "replace");
    nc = strlen(client->command);
    for (env = envp; *env != 0; env++) {
        p1 = *env;
        //if(strncmp(p1, prefix, np)) {
        //    continue;
        //}

        nenv = strlen(p1);
        n1 = nc + nenv + 2;
        check_buf(client, n1 + HEADER_PREFIX);

        n2 = sprintf(client->buf, "%s%07d %s ", MAGIC, n1, client->command);
        
        p2 = strstr(p1, "=");
        nkey = (p2-p1);
        memcpy(client->buf+n2, p1, nkey);
        p2++;
        client->buf[n2+nkey] = ' ';
        strcpy(client->buf+n2+nkey+1, p2);

        client->buf[n1+HEADER_PREFIX] = '\0';
        client->buf[n1+HEADER_PREFIX-1] = '\n';
        write(client->remote_fd, client->buf, n1 + HEADER_PREFIX);
    }
}

static void help() {
    printf("help:\n");
    printf("dbus set key value\n");
    printf("dbus ram key value\n");
    printf("dbus replace key value\n");
    printf("dbus get key\n");
    printf("dbus list key\n");
    printf("dbus delay key tick path_of_shell.sh\n");
    printf("dbus time key H:M:S path_of_shell.sh\n");
    printf("dbus export key\n");
    printf("dbus update key\n");
}

dbclient* gclient;
int main(int argc, char **argv, char * envp[])
{ 
    int n1, n2, err = 0;
    struct tm tm1;
    dbclient* client;
    int remote_fd = create_client_fd("/tmp/.skipd_server_sock");
    if(-1 == remote_fd) {
        return -1;
    }
    gclient = (dbclient*)calloc(1, sizeof(dbclient));
    gclient->remote_fd = remote_fd;
    client = gclient;

    do {
        if(argc < 2) {
            err = -11;
            break;
        }
        if(!strcmp("list", argv[1])) {
            if(argc < 3) {
                err = -12;
                break;
            }
            strcpy(client->command, argv[1]);
            n1 = strlen(argv[2]) + 2 + strlen(client->command);
            check_buf(client, n1 + HEADER_PREFIX);
            n2 = snprintf(client->buf, client->buf_max, "%s%07d %s %s\n", MAGIC, n1, client->command, argv[2]);
            write(remote_fd, client->buf, n2);

            setnonblock(remote_fd);
            n1 = parse_list_result(gclient);
        } else if(!strcmp("export", argv[1])) {
            strcpy(client->command, "list");
            n1 = strlen(argv[2]) + 2 + strlen(client->command);
            check_buf(client, n1 + HEADER_PREFIX);
            n2 = snprintf(client->buf, client->buf_max, "%s%07d %s %s\n", MAGIC, n1, client->command, argv[2]);
            write(remote_fd, client->buf, n2);

            setnonblock(remote_fd);
            n1 = parse_script_result(gclient);
        } else if(!strcmp("update", argv[1])) {
            if(argc < 3) {
                err = -13;
                break;
            }
            update_key(gclient, argv[2], envp);
        } else if(!strcmp("get", argv[1])) {
            if(argc < 3) {
                err = -13;
                break;
            }
            strcpy(client->command, argv[1]);
            n1 = strlen(argv[2]) + 2 + strlen(client->command);
            check_buf(client, n1 + HEADER_PREFIX);
            n2 = snprintf(client->buf, client->buf_max, "%s%07d %s %s\n", MAGIC, n1, client->command, argv[2]);
            write(remote_fd, client->buf, n2);

            setnonblock(remote_fd);
            n1 = parse_common_result(gclient);
        } else if(!strcmp("remove", argv[1])) {
            if(argc < 3) {
                err = -14;
                break;
            }
            strcpy(client->command, argv[1]);
            n1 = strlen(argv[2]) + 2 + strlen(client->command);
            check_buf(client, n1 + HEADER_PREFIX);
            n2 = snprintf(client->buf, client->buf_max, "%s%07d %s %s\n", MAGIC, n1, client->command, argv[2]);
            write(remote_fd, client->buf, n2);

            setnonblock(remote_fd);
            n1 = parse_common_result(gclient);
        } else if(!strcmp("fire", argv[1])) {
            if(argc < 3) {
                err = -15;
                break;
            }
            strcpy(client->command, argv[1]);
            n1 = strlen(argv[2]) + 2 + strlen(client->command);
            check_buf(client, n1 + HEADER_PREFIX);
            n2 = snprintf(client->buf, client->buf_max, "%s%07d %s %s\n", MAGIC, n1, client->command, argv[2]);
            write(remote_fd, client->buf, n2);

            setnonblock(remote_fd);
            n1 = parse_common_result(gclient);
        } else if(!strcmp("set", argv[1])) {
            if(argc < 4) {
                err = -16;
                break;
            }
            strcpy(client->command, argv[1]);
            n1 = strlen(client->command) + strlen(argv[2]) + strlen(argv[3]) + 3;
            check_buf(client, n1 + HEADER_PREFIX);
            n2 = snprintf(client->buf, client->buf_max, "%s%07d %s %s %s\n", MAGIC, n1, client->command, argv[2], argv[3]);
            write(remote_fd, client->buf, n2);

            setnonblock(remote_fd);
            n1 = parse_common_result(client);
        } else if(!strcmp("ram", argv[1])) {
            if(argc < 4) {
                err = -17;
                break;
            }
            strcpy(client->command, argv[1]);
            n1 = strlen(client->command) + strlen(argv[2]) + strlen(argv[3]) + 3;
            check_buf(client, n1 + HEADER_PREFIX);
            n2 = snprintf(client->buf, client->buf_max, "%s%07d %s %s %s\n", MAGIC, n1, client->command, argv[2], argv[3]);
            write(remote_fd, client->buf, n2);

            setnonblock(remote_fd);
            n1 = parse_common_result(client);
        } else if(!strcmp("replace", argv[1])) {
            if(argc < 4) {
                err = -18;
                break;
            }
            strcpy(client->command, argv[1]);
            n1 = strlen(client->command) + strlen(argv[2]) + strlen(argv[3]) + 3;
            check_buf(client, n1 + HEADER_PREFIX);
            n2 = snprintf(client->buf, client->buf_max, "%s%07d %s %s %s\n", MAGIC, n1, client->command, argv[2], argv[3]);
            write(remote_fd, client->buf, n2);

            setnonblock(remote_fd);
            n1 = parse_common_result(client);
        } else if(!strcmp("event", argv[1])) {
            strcpy(client->command, "set");
            n1 = strlen(client->command) + strlen(argv[2]) + strlen(argv[3]) + 3 + 9;
            check_buf(client, n1 + HEADER_PREFIX);
            n2 = snprintf(client->buf, client->buf_max, "%s%07d %s __event__%s %s\n", MAGIC, n1, client->command, argv[2], argv[3]);
            write(remote_fd, client->buf, n2);

            setnonblock(remote_fd);
            n1 = parse_common_result(client);
        } else if(!strcmp("delay", argv[1])) {
            if(argc < 5) {
                err = -19;
                break;
            }
            if(S2ISUCCESS != str2int(&n2, argv[3], 10)) {
                err = -20;
                break;
            }

            strcpy(client->command, argv[1]);
            n1 = strlen(client->command) + strlen(argv[2]) + strlen(argv[3]) + strlen(argv[4]) + 4;
            check_buf(client, n1 + HEADER_PREFIX);
            n2 = snprintf(client->buf, client->buf_max, "%s%07d %s %s %s %s\n", MAGIC, n1, client->command, argv[2], argv[3], argv[4]);
            write(remote_fd, client->buf, n2);

            setnonblock(remote_fd);
            n1 = parse_common_result(client);
        } else if(!strcmp("time", argv[1])) {
            if(argc < 5) {
                err = -21;
                break;
            }

            if(NULL == strptime(argv[3], "%H:%M:%S", &tm1)) {
                err = -22;
                break;
            }

            strcpy(client->command, argv[1]);
            n1 = strlen(client->command) + strlen(argv[2]) + strlen(argv[3]) + strlen(argv[4]) + 4;
            check_buf(client, n1 + HEADER_PREFIX);
            n2 = snprintf(client->buf, client->buf_max, "%s%07d %s %s %s %s\n", MAGIC, n1, client->command, argv[2], argv[3], argv[4]);
            write(remote_fd, client->buf, n2);

            setnonblock(remote_fd);
            n1 = parse_common_result(client);
            err = 0;
        }
    } while(0);
    if(err < 0) {
        help();
    }

    close(remote_fd);
    if(NULL != gclient->buf) {
        free(gclient->buf);
    }
    return 0;
}

