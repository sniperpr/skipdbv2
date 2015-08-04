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

    char* buf;
    int buf_max;
    int buf_len;
    int buf_pos;
} dbclient;

static int write_util(dbclient* client, int len, unsigned int delay) {
    int n, writed_len = 0;
    unsigned int now, timeout;
    struct timeval tv, tv2;

    gettimeofday(&tv2, NULL);
    timeout = (tv2.tv_sec*1000) + (tv2.tv_usec/1000) + delay;
    while(writed_len < len) {
        n = write(client->remote_fd, client->buf + writed_len, len - writed_len);
        if(n < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                gettimeofday(&tv, NULL);
                now = (tv.tv_sec*1000) + (tv.tv_usec/1000);
                if(now > timeout) {
                    break;
                }

                usleep(50);
            }
        }

        writed_len += n;
    }

    if(writed_len != len) {
        return -1;
    }
}

//TODO better for code reused
static int create_client_fd(char* sock_path) {
    int len, remote_fd;
    struct sockaddr_un remote;

    if(-1 == (remote_fd = socket(PF_UNIX, SOCK_STREAM, 0))) {
        //perror("socket");
        return -1;
    }

    remote.sun_family = AF_UNIX;
    strcpy(remote.sun_path, sock_path);
    len = strlen(remote.sun_path) + sizeof(remote.sun_family);
    if(-1 == connect(remote_fd, (struct sockaddr*)&remote, len)) {
        //perror("connect");
        close(remote_fd);
        return -1;
    }

    return remote_fd;
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

int dbclient_start(dbclient* client) {
    char* socket_path = "/tmp/.skipd_server_sock";

    memset(client, 0, sizeof(dbclient));

    client->remote_fd = create_client_fd(socket_path);
    if(-1 == client->remote_fd) {
        system("service start_skipd >/dev/null 2>&1 &");
        sleep(1);

        client->remote_fd = create_client_fd(socket_path);
        if(-1 == client->remote_fd) {
            return -1;
        }
    }
    return 0;
}

int dbclient_bulk(dbclient* client, char* command, char* key, char* value) {
    char *p1, *p2;
    int n1,n2,nc,nk,nv;

    nc = strlen(command);
    nk = strlen(key);
    nv = strlen(value);

    n1 = nc + nk + nv + 3;// replace key value\n
    check_buf(client, n1 + HEADER_PREFIX);
    n2 = sprintf(client->buf, "%s%07d %s %s %s\n", MAGIC, n1, command, key, value);

    return write_util(client, n1 + HEADER_PREFIX, 200);
}

int dbclient_end(dbclient* client) {
    if(NULL != client->buf) {
        free(client->buf);
    }
    if((-1 != client->remote_fd) || (0 != client->remote_fd)) {
        close(client->remote_fd);
    }
}

