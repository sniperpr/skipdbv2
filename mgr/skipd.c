#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>
#include <unistd.h>
#include <getopt.h>
#include <limits.h> // [LONG|INT][MIN|MAX]
#include <errno.h>  // errno
#include <string.h>
#include <signal.h>
#include <assert.h>
#include <time.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/un.h>
#include <ev.h>

#include "coroutine.h"
#include "SkipDB.h"

#define offsetof2(TYPE, MEMBER) ((size_t) &((TYPE *)0)->MEMBER)
#define container_of(ptr, type, member) ({                      \
        const typeof( ((type *)0)->member ) *__mptr = (const typeof( ((type *)0)->member )*)(ptr);    \
        (type *)( (char *)__mptr - offsetof2(type,member) );})

#define MAGIC "magicv1 "
#define MAGIC_LEN 8
#define HEADER_LEN 8
#define HEADER_PREFIX (MAGIC_LEN + HEADER_LEN)
#define PATH_MAX 128
#define BUF_MAX 2048
#define READ_MAX 65536

#define _min(a,b) \
   ({ __typeof__ (a) _a = (a); \
       __typeof__ (b) _b = (b); \
     _a > _b ? _b : _a; })

#define _max(a,b) \
   ({ __typeof__ (a) _a = (a); \
       __typeof__ (b) _b = (b); \
     _a > _b ? _a : _b; })

typedef struct _skipd_server {
    ev_io io;
    int fd;
    struct sockaddr_un socket;
    int socket_len;

    SkipDB *db;

    int daemon;
    char db_path[PATH_MAX];
    char sock_path[PATH_MAX];
    char pid_path[PATH_MAX];
} skipd_server;

/* enum skipd_client_state {
    client_state_read = 0,
    client_state_write,
    client_state_closing
}; */

enum ccr_break_state {
    ccr_break_continue = 0,
    ccr_break_curr,
    ccr_break_all,
    ccr_break_killed
};

enum ccr_error_code {
    ccr_error_err1 = -1,
    ccr_error_err2 = -2,
    ccr_error_err3 = -3,
    ccr_error_ok = 0,
    ccr_error_ok1 = 1,
    ccr_error_ok2 = 2
};

typedef struct _skipd_client {
    ev_io io_read;
    ev_io io_write;

    struct ccrContextTag ccr_read;
    struct ccrContextTag ccr_write;
    struct ccrContextTag ccr_process;
    //struct ccrContextTag ccr_runcmd;
    int break_level;

    int fd;

    char* command;
    char* key;
    int data_len;

    char* origin;
    int origin_len;
    int read_len;
    int read_pos;

    char* send;
    int send_max;
    int send_len;
    int send_pos;

    skipd_server* server;
} skipd_client;

/* declare */
extern int skipd_daemonize(char *_lock_path);
static int setnonblock(int fd);
static void not_blocked(EV_P_ ev_periodic *w, int revents);
static int client_ccr_process(EV_P_ skipd_client* client);
static int client_ccr_write(EV_P_ skipd_client* client);

char* global_magic = MAGIC;
skipd_server global_server;
ev_signal signal_watcher;

static void sigint_cb (EV_P_ ev_signal *w, int revents) {
    fprintf(stderr, "exit..\n");
    SkipDB_close(&global_server.db);
    ev_break(EV_A_ EVBREAK_ALL);
}

typedef enum {
    S2ISUCCESS = 0,
    S2IOVERFLOW,
    S2IUNDERFLOW,
    S2IINCONVERTIBLE
} STR2INT_ERROR;

STR2INT_ERROR str2int(int *i, char *s, int base) {
  char *end;
  long  l;
  errno = 0;
  l = strtol(s, &end, base);

  if ((errno == ERANGE && l == LONG_MAX) || l > INT_MAX) {
    return S2IOVERFLOW;
  }
  if ((errno == ERANGE && l == LONG_MIN) || l < INT_MIN) {
    return S2IUNDERFLOW;
  }
  if (*s == '\0' || *end != '\0') {
    return S2IINCONVERTIBLE;
  }
  *i = l;
  return S2ISUCCESS;
}

static void client_release(EV_P_ skipd_client* client) {
    fprintf(stderr, "closing client\n");

    ev_io_stop(EV_A_ &client->io_read);
    ev_io_stop(EV_A_ &client->io_write);
    close(client->fd);

    if(NULL != client->send) {
        free(client->send);
    }

    if(NULL != client->origin) {
        free(client->origin);
    }

    free(client);
}

// This callback is called when client data is available
static void client_read(EV_P_ ev_io *w, int revents) {
    skipd_client* client = container_of(w, skipd_client, io_read);
    int rt;

    //process command
    assert(client->break_level == 0);

    rt = client_ccr_process(EV_A_ client);
    fprintf(stderr, "process:%d\n", rt);
    if(ccr_error_err1 == rt) {
        //Close it directly
        client_release(EV_A_ client);
    } else if(ccr_error_ok == rt) {
        //Just continue
        client->break_level = 0;
    } else if(rt < 0) {
        if(ccr_break_killed == client->break_level) {
            //wait for waiting completely
            if(0 == client->send_len) {
                //close it
                client_release(EV_A_ client);
            } else {
                memset(&client->ccr_process, 0, sizeof(struct ccrContextTag));
            }
        }
    } else if(rt > 0) {
        //OK and than reset
        memset(&client->ccr_process, 0, sizeof(struct ccrContextTag));
        client->break_level = 0;
    }
}

static void client_write(EV_P_ ev_io *w, int revents) {
    skipd_client* client = container_of(w, skipd_client, io_write);
    int rt;

    fprintf(stderr, "begin for writing\n");

    rt = client_ccr_write(EV_A_ client);
    if(rt < 0) {
        client_release(EV_A_ client);
    } else if(0 == rt) {
        if(ccr_break_killed == client->break_level) {
            client_release(EV_A_ client);
        } 
    } else {
            // rt > 0, So reset it
            memset(&client->ccr_write, 0, sizeof(struct ccrContextTag));
            client->break_level = 0;
    }

    //TODO void event_active (struct event *ev, int res, short ncalls)
}

#define UNKNOWN_LEN 7
static int client_send(EV_P_ skipd_client* client, char* buf, int len) {
    static char* unknown = "unknown";
    char pref_buf[HEADER_PREFIX], *pc, *pk;
    int n, clen = (NULL == client->command ? 0 : strlen(client->command));
    int klen = (NULL == client->key ? 0 : strlen(client->key));

    memcpy(pref_buf, global_magic, MAGIC_LEN);

    int resp_len = len + 2;
    if(0 == clen) {
        pc = unknown;
        resp_len += UNKNOWN_LEN;
    } else {
        pc = client->command;
        resp_len += clen;
    }
    if(0 == klen) {
        pk = unknown;
        resp_len += UNKNOWN_LEN;
    } else {
        pk = client->key;
        resp_len += klen;
    }
    sprintf(pref_buf + MAGIC_LEN, "%07d ", resp_len);
    resp_len += HEADER_PREFIX;

    if(NULL == client->send) {
        client->send = (char*)malloc(BUF_MAX+1);
        client->send_max = BUF_MAX;
        client->send_len = 0;
    }

    if(resp_len > client->send_max) {
        free(client->send);
        client->send = (char*)malloc(resp_len);
        client->send_max = resp_len;
        client->send_len = 0;
    }

    memcpy(client->send, pref_buf, HEADER_PREFIX);
    n = sprintf(client->send + HEADER_PREFIX, "%s %s ", pc, pk);
    memcpy(client->send + HEADER_PREFIX + n, buf, len);
    client->send[resp_len] = '\0';
    client->send_len = resp_len;
    client->send_pos = 0;

    //switch to read state
    memset(&client->ccr_write, 0, sizeof(struct ccrContextTag));
    ev_io_stop(EV_A_ &client->io_read);
    ev_io_start(EV_A_ &client->io_write);

    return 0;
}

static int client_ccr_write(EV_P_ skipd_client* client) {
    struct ccrContextTag* ctx = &client->ccr_write;

    //stack
    ccrBeginContext
    int n;
    ccrEndContext(ctx);

    ccrBegin(ctx);
    while(client->send_pos < client->send_len) {
        CS->n = write(client->fd, client->send + client->send_pos, client->send_len - client->send_pos);
        if(CS->n < 0) {
            if(errno == EINTR || errno == EAGAIN) {
                ccrReturn(ctx, ccr_error_ok);
            } else {
                fprintf(stderr, "write sock error, line=%d\n", __LINE__);
                ccrStop(ctx, ccr_error_err1);
            }
        }

        if((client->send_len -= CS->n) > 0) {
            //write again
            client->send_pos += CS->n;
            ccrReturn(ctx, ccr_error_ok);
        } else {
            //Finished write
            client->send_len = 0;
            client->send_pos = 0;
            ev_io_stop(EV_A_ &client->io_write);
            ev_io_start(EV_A_ &client->io_read);
            ccrReturn(ctx, ccr_error_ok1);
        }
    }
    ccrFinish(ctx, ccr_error_ok1);
}

/* static void client_read_fix(skipd_client* client) {
    if(client->read_len > client->read_pos) {
        memmove(client->origin, client->origin + client->read_pos + 1, client->read_len - client->read_pos - 1);
    }
    client->read_len -= client->read_pos + 1;
    client->read_pos = 0;
} */

static int client_ccr_read_util(skipd_client* client, int step_len) {
    int buf_len;
    struct ccrContextTag* ctx = &client->ccr_read;

    //stack
    ccrBeginContext
    int n;
    int left;
    ccrEndContext(ctx);

    ccrBegin(ctx);

    if(step_len > READ_MAX) {
        ccrReturn(ctx, ccr_error_err2);
    }

    buf_len = _max(step_len, BUF_MAX);
    if(buf_len > client->origin_len) {
        if(NULL != client->origin) {
            free(client->origin);
        }
        client->origin = (char*)malloc(buf_len + 1);
        client->origin_len = buf_len;
        client->origin[client->origin_len] = '\0';
    }
    client->read_pos = 0;
    client->read_len = 0;

    for(;;) {
        CS->left = step_len - client->read_len;
        assert(0 != CS->left);

        CS->n = recv(client->fd, client->origin + client->read_len, CS->left, 0);
        if (0 == CS->n) {
            //client closed
            ccrReturn(ctx, ccr_error_err1);
        } else if(CS->n < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                client->break_level = ccr_break_all;
                ccrReturn(ctx, ccr_error_ok);
            } else {
                ccrReturn(ctx, ccr_error_err1);
            }
        } else {
            client->read_len += CS->n;
            if(client->read_len == step_len) {
                break;
            }
        }
    }
    ccrFinish(ctx, ccr_error_ok1);
}

static int client_run_command(EV_P_ skipd_client* client) {
    char *p1, *p2;
    Datum dkey, dvalue;

    p1 = client->origin;
    p2 = strstr(p1, " ");
    if(NULL == p2) {
        return ccr_error_err2;
    }
    *p2 = '\0';
    client->command = p1;

    if(!strcmp(client->command, "set")) {
        p1 = p2+1;
        p2 = strstr(p1, " ");
        if(NULL == p2) {
            return ccr_error_err2;
        }
        *p2 = '\0';
        client->key = p1;
        p1 = p2+1;

        dkey = Datum_FromCString_(client->key);
        dvalue = Datum_FromData_length_((unsigned char*)p1, client->data_len - (p1 - client->origin));
	SkipDB_beginTransaction(client->server->db);
        SkipDB_at_put_(client->server->db, dkey, dvalue);
	SkipDB_commitTransaction(client->server->db);

        p1 = "ok\n";
        client_send(EV_A_ client, p1, strlen(p1));
        fprintf(stderr, "resp: %s\n", client->send);
        return ccr_error_ok1;
    } else if(!strcmp(client->command, "get")) {
        p1 = p2+1;
        p2 = strstr(p1, "\n");
        if(NULL == p2) {
            return ccr_error_err2;
        }
        *p2 = '\0';
        client->key = p1;
        p1 = p2+1;

        dkey = Datum_FromCString_(client->key);
	dvalue = SkipDB_at_(client->server->db, dkey);
        if(NULL == dvalue.data) {
            p1 = "none\n";
            client_send(EV_A_ client, p1, strlen(p1));
        } else {
            client_send(EV_A_ client, dvalue.data, dvalue.size);
        }
        return ccr_error_ok1;
    } else if(!strcmp(client->command, "replace")) {
    } else if(!strcmp(client->command, "list")) {
    } else if(!strcmp(client->command, "delay")) {
    } else if(!strcmp(client->command, "time")) {
    }

    return ccr_error_err2;
}

static int client_ccr_process(EV_P_ skipd_client* client) {
    char* p;
    struct ccrContextTag* ctx = &client->ccr_process;

    //stack
    ccrBeginContext
    int rt;
    int n;
    int left;
    ccrEndContext(ctx);

    ccrBegin(ctx);

    for(;;) {

        /* Sub routine */
        memset(&client->ccr_read, 0, sizeof(struct ccrContextTag));
        for(;;) {
            CS->rt = client_ccr_read_util(client, HEADER_PREFIX);
            if((ccr_error_err1 != CS->rt) && (CS->rt < 0)) {
                /* socket not closed but has some other error*/
                p = "command no found\n";
                client_send(EV_A_ client, p, strlen(p));
                client->break_level = ccr_break_killed;
                ccrStop(ctx, CS->rt);
            }

            /* TODO if(client->break_level >= ccr_break_all) {
                ccrReturn(ctx, CS->rt);
            } else if(ccr_break_curr == client->break_level) {
                client->break_level = ccr_break_continue;
                ccrReturn(ctx, CS->rt);
            } */

            /* OK */
            if(CS->rt > 0) {
                break;
            }
            ccrReturn(ctx, CS->rt);
        }

        assert(CS->rt > 0);
        if(0 != memcmp(client->origin, global_magic, MAGIC_LEN)) {
            p = "magic not found\n";
            client_send(EV_A_ client, p, strlen(p));
            client->break_level = ccr_break_killed;
            ccrStop(ctx, ccr_error_err2);
        }

        if(client->origin[HEADER_PREFIX-1] != ' ') {
            p = "header prefix error\n";
            client_send(EV_A_ client, p, strlen(p));
            client->break_level = ccr_break_killed;
            ccrStop(ctx, ccr_error_err2);
        }
        client->origin[HEADER_PREFIX-1] = '\0';

        if(S2ISUCCESS != str2int(&client->data_len, client->origin+MAGIC_LEN, 10)) {
            p = "data len error\n";
            client_send(EV_A_ client, p, strlen(p));
            client->break_level = ccr_break_killed;
            ccrStop(ctx, ccr_error_err2);
        }

        /* Sub routine */
        memset(&client->ccr_read, 0, sizeof(struct ccrContextTag));
        for(;;) {
            CS->rt = client_ccr_read_util(client, client->data_len);
            if((ccr_error_err1 != CS->rt) && (CS->rt < 0)) {
                p = "read data error\n";
                client_send(EV_A_ client, p, strlen(p));
                client->break_level = ccr_break_killed;
                ccrStop(ctx, CS->rt);
            }

            if(CS->rt > 0) {
                break;
            }
            ccrReturn(ctx, CS->rt);
        }

        assert(CS->rt > 0);
        client->origin[client->data_len] = '\0';
        CS->rt = client_run_command(EV_A_ client);
        if(CS->rt < 0) {
            p = "run command error\n";
            client_send(EV_A_ client, p, strlen(p));
            client->break_level = ccr_break_killed;
            ccrReturn(ctx, CS->rt);
        }

        /* Reset the read_len */
        client->command = NULL;
        client->key = NULL;
        if(client->origin_len > BUF_MAX) {
            free(client->origin);
            client->origin = NULL;
            client->origin_len = 0;
        }
    }
    ccrFinish(ctx, ccr_error_ok1);
}

static skipd_client* client_new(int fd) {
    int opt = 0;
    skipd_client* client = (skipd_client*)calloc(1, sizeof(skipd_client));
    client->fd = fd;
    client->send = NULL;
    client->origin = NULL;

    //setsockopt(client->fd, IPPROTO_TCP, TCP_NODELAY, &opt, sizeof(opt));
    setnonblock(client->fd);
    ev_io_init(&client->io_read, client_read, client->fd, EV_READ);
    ev_io_init(&client->io_write, client_write, client->fd, EV_WRITE);

    return client;
}

// This callback is called when data is readable on the unix socket.
static void server_cb(EV_P_ ev_io *w, int revents) {
    puts("unix stream socket has become readable");

    int client_fd;
    skipd_client* client;

    skipd_server* server = container_of(w, skipd_server, io);

    while (1) {
        client_fd = accept(server->fd, NULL, NULL);
        if( client_fd == -1 ) {
            if( errno != EAGAIN && errno != EWOULDBLOCK ) {
                fprintf(stderr, "accept() failed errno=%i (%s)",  errno, strerror(errno));
                exit(EXIT_FAILURE);
            }
            break;
        }

        puts("accepted a client");
        client = client_new(client_fd);
        client->server = server;
        ev_io_start(EV_A_ &client->io_read);
    }
}

// Simply adds O_NONBLOCK to the file descriptor of choice
int setnonblock(int fd) {
    int flags;

    flags = fcntl(fd, F_GETFL);
    flags |= O_NONBLOCK;
    return fcntl(fd, F_SETFL, flags);
}

int unix_socket_init(struct sockaddr_un* socket_un, char* sock_path, int max_queue) {
    int fd;
    int opt = 0;

    unlink(sock_path);

    // Setup a unix socket listener.
    fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (-1 == fd) {
        perror("echo server socket");
        exit(EXIT_FAILURE);
    }

    //SOL_TCP
    //setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &opt, sizeof(opt));

    // Set it non-blocking
    if (-1 == setnonblock(fd)) {
        perror("echo server socket nonblock");
        exit(EXIT_FAILURE);
    }

    // Set it as unix socket
    socket_un->sun_family = AF_UNIX;
    strcpy(socket_un->sun_path, sock_path);

    return fd;
}

int server_init(skipd_server* server, int max_queue) {
    int count;

    server->db = SkipDB_new();
    SkipDB_setPath_(server->db, server->db_path);
    SkipDB_open(server->db);

    count = SkipDB_count(server->db);
    fprintf(stderr, "count=%d\n", count);

    server->fd = unix_socket_init(&server->socket, server->sock_path, max_queue);
    server->socket_len = sizeof(server->socket.sun_family) + strlen(server->socket.sun_path);

    if (-1 == bind(server->fd, (struct sockaddr*) &server->socket, server->socket_len)) {
        perror("echo server bind");
        exit(EXIT_FAILURE);
    }

    if (-1 == listen(server->fd, max_queue)) {
        perror("listen");
        exit(EXIT_FAILURE);
    }
    return 0;
}

static struct option options[] = {
    { "help",	no_argument,		NULL, 'h' },
    { "db_path", required_argument,	NULL, 'd' },
    { "sock_path", required_argument,	NULL, 's' },
    { "daemon", 	required_argument,	NULL, 'D' },
    { NULL, 0, 0, 0 }
};

int main(int argc, char **argv)
{
    int n = 0, daemon = 0, max_queue = -1;
    skipd_server *server = &global_server;
    struct ev_periodic every_few_seconds;
    EV_P  = ev_default_loop(0);

    strcpy(server->sock_path, "/tmp/.skipd_server_sock");
    while (n >= 0) {
            n = getopt_long(argc, argv, "hd:D:s:", options, NULL);
            if (n < 0)
                    continue;
            switch (n) {
            case 'D':
                //TODO fix me if optarg is bigger than PATH_MAX
                strcpy(server->pid_path, optarg);
                daemon = 1;
                break;
            case 'd':
                strcpy(server->db_path, optarg);
                break;
            case 's':
                strcpy(server->sock_path, optarg);
                break;
            case 'h':
                fprintf(stderr, "Usage: skipd xxx todo\n");
                exit(1);
                break;
            }
    }

    if(daemon && skipd_daemonize(server->pid_path)) {
        fprintf(stderr, "Failed to daemonize\n");
        return 1;
    }

    ev_signal_init (&signal_watcher, sigint_cb, SIGINT);
    ev_signal_start (EV_A_ &signal_watcher);

    // Create unix socket in non-blocking fashion
    server_init(server, max_queue);

    // To be sure that we aren't actually blocking
    ev_periodic_init(&every_few_seconds, not_blocked, 0, 5, 0);
    ev_periodic_start(EV_A_ &every_few_seconds);

    // Get notified whenever the socket is ready to read
    ev_io_init(&server->io, server_cb, server->fd, EV_READ);
    ev_io_start(EV_A_ &server->io);

    // Run our loop, ostensibly forever
    puts("unix-socket-echo starting...\n");
    ev_loop(EV_A_ 0);

    // This point is only ever reached if the loop is manually exited
    close(server->fd);
    return EXIT_SUCCESS;
}

static void not_blocked(EV_P_ ev_periodic *w, int revents) {
    puts("I'm not blocked");
}
