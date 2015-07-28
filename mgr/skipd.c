#define _XOPEN_SOURCE

#include <stdlib.h>
#include <stdio.h>
#include <fcntl.h>
#include <unistd.h>
#include <getopt.h>
#include <limits.h> // [LONG|INT][MIN|MAX]
#include <errno.h>  // errno
#include <string.h>
#include <signal.h>
#include <assert.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>
#include <sys/un.h>
#include <syslog.h>
#include <ev.h>

#include "coroutine.h"
#include "SkipDB.h"
#include "skipd.h"

typedef struct _skipd_server {
    ev_io io;
    int fd;
    struct sockaddr_un socket;
    int socket_len;

    SkipDB *db;

    int daemon;
    char db_path[SK_PATH_MAX];
    char sock_path[SK_PATH_MAX];
    char pid_path[SK_PATH_MAX];
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
    ccr_error_ok = 0, /* 此命令暂时中断，需等待再次执行 */
    ccr_error_ok1 = 1, /* 此命令成功结束执行 */
    ccr_error_ok2 = 2
};

typedef struct _skipd_client {
    ev_io io_read;
    ev_io io_write;

    struct ccrContextTag ccr_read;
    struct ccrContextTag ccr_write;
    struct ccrContextTag ccr_process;
    struct ccrContextTag ccr_runcmd;
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

typedef struct _delay_cmd {
    ev_timer watcher;
    char key[DELAY_KEY_LEN + DELAY_PREFIX_LEN];
    int tick;

    skipd_server* server;
} delay_cmd;

typedef struct _time_cmd {
    ev_timer watcher;
    char key[DELAY_KEY_LEN + DELAY_PREFIX_LEN];

    skipd_server* server;
} time_cmd;

/* declare */
extern SkipDBRecord* SkipDB_list_first(SkipDB* self, Datum k, SkipDBCursor** pcur);
extern SkipDBRecord* SkipDB_list_next(SkipDB* self, Datum k, SkipDBCursor* cursor);

extern int skipd_daemonize(char *_lock_path);
static int setnonblock(int fd);
static int client_ccr_process(EV_P_ skipd_client* client);
static int client_ccr_write(EV_P_ skipd_client* client);

char* global_magic = MAGIC;
skipd_server* global_server;
ev_signal signal_watcher;
ev_signal signal_watcher2;
char static_buffer[512];

static void sigint_cb (EV_P_ ev_signal *w, int revents) {
    ev_signal_stop (EV_A_ w);
    ev_break(EV_A_ EVBREAK_ALL);
}

void sys_script(char *cmd) {
    snprintf(static_buffer, sizeof(static_buffer), "%s > /tmp/skipd.log 2>&1 &\n", cmd);
    system(static_buffer);
    strcpy(static_buffer, ""); // Ensure we don't re-execute it again
}

int log_level = LOG_DEBUG;
void emit_log(int level, char* line) {
    //TODO for level
    int syslog_level = LOG_DEBUG;
    syslog(syslog_level, "%s", line);
    //printf("%s", line);
}

void _skipd_logv(int filter, const char *format, va_list vl)
{
	char buf[256];

        //TODO for log_evel
	/* if (!(log_level & filter)) {
	    return;
        } */

	vsnprintf(buf, sizeof(buf), format, vl);
	buf[sizeof(buf) - 1] = '\0';

	emit_log(filter, buf);
}

void skipd_log(int filter, const char *format, ...)
{
	va_list ap;

	va_start(ap, format);
	_skipd_logv(filter, format, ap);
	va_end(ap);
}

static void client_release(EV_P_ skipd_client* client) {
    skipd_log(SKIPD_DEBUG, "closing client\n");

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

static void client_read_inner(EV_P_ skipd_client* client, int revents) {
    int rt;

    //process command
    assert(client->break_level == 0);

    rt = client_ccr_process(EV_A_ client);
    skipd_log(SKIPD_DEBUG, "process:%d\n", rt);
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

// This callback is called when client data is available
static void client_read(EV_P_ ev_io *w, int revents) {
    skipd_client* client = container_of(w, skipd_client, io_read);
    client_read_inner(EV_A_ client, revents);
}

static void client_write(EV_P_ ev_io *w, int revents) {
    skipd_client* client = container_of(w, skipd_client, io_write);
    int rt;

    skipd_log(SKIPD_DEBUG, "begin for writing\n");

    rt = client_ccr_write(EV_A_ client);
    if(ccr_break_killed == client->break_level) {
        client_release(EV_A_ client);
    } else {
            // rt > 0, So reset it
            memset(&client->ccr_write, 0, sizeof(struct ccrContextTag));
            client->break_level = 0;

            //change to read state directly,
            //如果read过程还有资源未释放，跳回read进行资源释放
            //event_active(&client->io_read, EV_READ, 0);
            client_read_inner(EV_A_ client, 0);
    }

    //TODO void event_active (struct event *ev, int res, short ncalls)
}

static void delay_cmd_cb(EV_P_ ev_timer* watcher, int revents) {
    char* p1;
    int tmpi;
    delay_cmd* delay_obj = container_of(watcher, delay_cmd, watcher);
    Datum dkey = Datum_FromCString_(delay_obj->key);
    Datum dvalue = SkipDB_at_(delay_obj->server->db, dkey);
    do {
        if(NULL == dvalue.data) {
            //TODO log hear, Not exists
            break;
        }

        p1 = strstr((char*)dvalue.data, " ");
        if(NULL == p1) {
            break;
        }

        tmpi = (unsigned char*)p1 - dvalue.data;
        memcpy(static_buffer, dvalue.data, tmpi);
        static_buffer[tmpi] = '\0';
        if(S2ISUCCESS != str2int(&tmpi, static_buffer, 10)) {
            break;
        }

        p1++;
        sys_script(p1);
        if(0 == tmpi) {
            break;
        }

        delay_obj->tick = tmpi;
        watcher->repeat = tmpi;
        ev_timer_again(EV_A_ watcher);
        return;

    } while(0);

    ev_timer_stop(EV_A_ watcher);
    free(delay_obj);
}

static void time_cmd_cb(EV_P_ ev_timer* watcher, int revents) {
    char* p1;
    time_cmd* time_obj = container_of(watcher, time_cmd, watcher);
    Datum dkey = Datum_FromCString_(time_obj->key);
    Datum dvalue = SkipDB_at_(time_obj->server->db, dkey);
    do {
        if(NULL == dvalue.data) {
            skipd_log(SKIPD_DEBUG, "time %s not found\n", dkey.data);
            ev_timer_stop(EV_A_ watcher);
            free(time_obj);
            break;
        }

        p1 = strstr((char*)dvalue.data, " ");
        if(NULL == p1) {
            skipd_log(SKIPD_DEBUG, "time %s value error\n", dkey.data);
            ev_timer_stop(EV_A_ watcher);
            free(time_obj);
            break;
        }
        p1++;

        sys_script(p1);
        ev_timer_again(EV_A_ watcher);
    } while(0);

}

static int client_send_key(EV_P_ skipd_client* client, char* cmd, char* key, char* buf, int len) {
    char pref_buf[HEADER_PREFIX+1];
    int n, resp_len = (len + 2);

    memcpy(pref_buf, global_magic, MAGIC_LEN);

    resp_len += strlen(cmd);
    resp_len += strlen(key);

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
    n = sprintf(client->send + HEADER_PREFIX, "%s %s ", cmd, key);
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

static int client_send(EV_P_ skipd_client* client, char* buf, int len) {
    return client_send_key(EV_A_ client
            , (NULL == client->command ? "errcmd": client->command)
            , (NULL == client->key ? "errkey": client->key)
            , buf, len);
}

static int client_ccr_write(EV_P_ skipd_client* client) {
    struct ccrContextTag* ctx = &client->ccr_write;

    //stack
    ccrBeginContext
    int n;
    ccrEndContext(ctx);

    ccrBegin(ctx);
    while(client->send_pos < client->send_len) {
        //use send instead of write
        CS->n = write(client->fd, client->send + client->send_pos, client->send_len - client->send_pos);
        //CS->n = send(client->fd, client->send + client->send_pos, client->send_len - client->send_pos, 0);
        if(CS->n <= 0) {
            if(errno == EINTR || errno == EAGAIN) {
                ccrReturn(ctx, ccr_error_ok);
            } else {
                //TODO fixme
                client->send_len = 0;
                client->send_pos = 0;
                ev_io_stop(EV_A_ &client->io_write);
                ev_io_start(EV_A_ &client->io_read);

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

static int client_run_command(EV_P_ skipd_client* client)
{
    char *p1, *p2;
    time_t t1, t2, epoch;
    int tmpi, tmp2;
    struct tm tm1, tm2, *tnow;
    Datum dkey, dvalue;
    delay_cmd* delay_obj;
    time_cmd* time_obj;
    struct ccrContextTag* ctx = &client->ccr_read;

    //stack
    ccrBeginContext
    int n;
    SkipDBCursor* cursor;
    SkipDBRecord* record;
    Datum skey;
    ccrEndContext(ctx);

    ccrBegin(ctx);

    p1 = client->origin;
    p2 = strstr(p1, " ");
    if(NULL == p2) {
        ccrStop(ctx, ccr_error_err2);
    }
    *p2 = '\0';
    client->command = p1;

    if(!strcmp(client->command, "set")) {
        p1 = p2+1;
        p2 = strstr(p1, " ");
        if(NULL == p2) {
            ccrStop(ctx, ccr_error_err2);
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
        ccrReturn(ctx, ccr_error_ok1);
    } else if(!strcmp(client->command, "ram")) {
        p1 = p2+1;
        p2 = strstr(p1, " ");
        if(NULL == p2) {
            ccrStop(ctx, ccr_error_err2);
        }
        *p2 = '\0';
        client->key = p1;
        p1 = p2+1;

        dkey = Datum_FromCString_(client->key);
        dvalue = Datum_FromData_length_((unsigned char*)p1, client->data_len - (p1 - client->origin));
        SkipDB_at_put_(client->server->db, dkey, dvalue);

        p1 = "ok\n";
        client_send(EV_A_ client, p1, strlen(p1));
        ccrReturn(ctx, ccr_error_ok1);
    } else if(!strcmp(client->command, "get")) {
        p1 = p2+1;
        p2 = strstr(p1, "\n");
        if(NULL == p2) {
            ccrStop(ctx, ccr_error_err2);
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
            client_send(EV_A_ client, (char*)dvalue.data, dvalue.size);
        }
        ccrReturn(ctx, ccr_error_ok1);
    } else if(!strcmp(client->command, "replace")) {
        p1 = p2+1;
        p2 = strstr(p1, " ");
        if(NULL == p2) {
            ccrStop(ctx, ccr_error_err2);
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
            //exists replace it
            client_send(EV_A_ client, (char*)dvalue.data, dvalue.size);
            dvalue = Datum_FromData_length_((unsigned char*)p1, client->data_len - (p1 - client->origin));
            SkipDB_beginTransaction(client->server->db);
            SkipDB_at_put_(client->server->db, dkey, dvalue);
            SkipDB_commitTransaction(client->server->db);
        }
        ccrReturn(ctx, ccr_error_ok1);
    } else if(!strcmp(client->command, "list")) {
        p1 = p2+1;
        p2 = strstr(p1, "\n");
        if(NULL == p2) {
            ccrStop(ctx, ccr_error_err2);
        }
        *p2 = '\0';
        client->key = p1;
        p1 = p2+1;

        CS->skey = Datum_FromCString_(client->key);
        CS->record = SkipDB_list_first(client->server->db, CS->skey, &CS->cursor);
        while(NULL != CS->record) {
            dkey = SkipDBRecord_keyDatum(CS->record);
            dvalue = SkipDBRecord_valueDatum(CS->record);
            client_send_key(EV_A_ client, client->command, (char*)dkey.data, (char*)dvalue.data, dvalue.size);
            ccrReturn(ctx, ccr_error_ok);

            CS->record = SkipDB_list_next(client->server->db, CS->skey, CS->cursor);
        }

        //send end
        p1 = "__end__\n";
        client_send(EV_A_ client, p1, strlen(p1));
        ccrReturn(ctx, ccr_error_ok);

        if(NULL != CS->cursor) {
            SkipDBCursor_release(CS->cursor);
        }

        ccrReturn(ctx, ccr_error_ok1);
    } else if(!strcmp(client->command, "remove")) {
        p1 = p2+1;
        p2 = strstr(p1, "\n");
        if(NULL == p2) {
            ccrStop(ctx, ccr_error_err2);
        }
        *p2 = '\0';
        client->key = p1;
        p1 = p2+1;

        dkey = Datum_FromCString_(client->key);

        SkipDB_beginTransaction(client->server->db);
        SkipDB_removeAt_(client->server->db, dkey);
        SkipDB_commitTransaction(client->server->db);
        p1 = "ok\n";
        client_send(EV_A_ client, p1, strlen(p1));
        ccrReturn(ctx, ccr_error_ok1);
    } else if(!strcmp(client->command, "delay")) {
        p1 = p2+1;
        p2 = strstr(p1, " ");
        if(NULL == p2) {
            ccrStop(ctx, ccr_error_err2);
        }
        *p2 = '\0';
        client->key = p1;
        p1 = p2+1;

        if(strlen(client->key) >= DELAY_KEY_LEN) {
            p1 = "key too big\n";
            client_send(EV_A_ client, p1, strlen(p1));
            ccrReturn(ctx, ccr_error_ok1);
        }

        dvalue = Datum_FromData_length_((unsigned char*)p1, client->data_len - (p1 - client->origin));
        //dvalue.data[dvalue.size-1] = '\0';
        //FIXEM hard code hear
        if(strlen((char*)dvalue.data) >= 300) {
            p1 = "value too big\n";
            client_send(EV_A_ client, p1, strlen(p1));
            ccrReturn(ctx, ccr_error_ok1);
        }

        p2  = strstr(p1, " ");
        if(NULL == p2) {
            p1 = "value param error\n";
            client_send(EV_A_ client, p1, strlen(p1));
            ccrReturn(ctx, ccr_error_ok1);
        }
        tmpi = (int)(p2 - p1);
        memcpy(static_buffer, p1, tmpi);
        static_buffer[tmpi] = '\0';
        if(S2ISUCCESS != str2int(&tmpi, static_buffer, 10)) {
            p1 = "delay tick error\n";
            client_send(EV_A_ client, p1, strlen(p1));
            ccrReturn(ctx, ccr_error_ok1);
        }

        //All ok, we create a delay object
        delay_obj = calloc(1, sizeof(delay_cmd));
        sprintf(delay_obj->key, "__delay__%s", client->key);
        client->key = delay_obj->key;
        delay_obj->tick = tmpi;
        delay_obj->server = client->server;

        dkey = Datum_FromCString_(client->key);
        if(SkipDB_exists(client->server->db, dkey)) {
            p1 = "exists\n";
            free(delay_obj);
            client_send(EV_A_ client, p1, strlen(p1));

            //Just replace the old
            SkipDB_beginTransaction(client->server->db);
            SkipDB_at_put_(client->server->db, dkey, dvalue);
            SkipDB_commitTransaction(client->server->db);
            ccrReturn(ctx, ccr_error_ok1);
        }

        SkipDB_beginTransaction(client->server->db);
        SkipDB_at_put_(client->server->db, dkey, dvalue);
        SkipDB_commitTransaction(client->server->db);

        ev_timer_init(&delay_obj->watcher, delay_cmd_cb, delay_obj->tick, delay_obj->tick);
        ev_timer_start(EV_A_ &delay_obj->watcher);

        p1 = "delay_ok\n";
        client_send(EV_A_ client, p1, strlen(p1));

        ccrReturn(ctx, ccr_error_ok1);
    } else if(!strcmp(client->command, "time")) {
        p1 = p2+1;
        p2 = strstr(p1, " ");
        if(NULL == p2) {
            ccrStop(ctx, ccr_error_err2);
        }
        *p2 = '\0';
        client->key = p1;
        p1 = p2+1;

        if(strlen(client->key) >= DELAY_KEY_LEN) {
            p1 = "key too big\n";
            client_send(EV_A_ client, p1, strlen(p1));
            ccrReturn(ctx, ccr_error_ok1);
        }

        p2 = strstr(p1, " ");
        if(NULL == p2) {
            p1 = "value error\n";
            client_send(EV_A_ client, p1, strlen(p1));
            ccrReturn(ctx, ccr_error_ok1);
        }
        tmpi = (int)(p2 - p1);
        memcpy(static_buffer, p1, tmpi);
        static_buffer[tmpi] = '\0';

        if (strptime(static_buffer, "%H:%M:%S", &tm1) != NULL) {
            epoch = mktime(&tm1);
        } else {
            p1 = "time error\n";
            client_send(EV_A_ client, p1, strlen(p1));
            ccrReturn(ctx, ccr_error_ok1);
        }

        dvalue = Datum_FromData_length_((unsigned char*)p1, client->data_len - (p1 - client->origin));
        //dvalue.data[dvalue.size-1] = '\0';
        //FIXEM hard code hear
        if(strlen((char*)dvalue.data) >= 300) {
            p1 = "value too big\n";
            client_send(EV_A_ client, p1, strlen(p1));
            ccrReturn(ctx, ccr_error_ok1);
        }

        time_obj = calloc(1, sizeof(time_cmd));
        sprintf(time_obj->key, "__time__%s", client->key);
        //TODO time_obj will be free?
        client->key = time_obj->key;
        dkey = Datum_FromCString_(client->key);
        if(SkipDB_exists(client->server->db, dkey)) {
            p1 = "exists\n";
            client_send(EV_A_ client, p1, strlen(p1));
            free(time_obj);
            ccrReturn(ctx, ccr_error_ok1);
        }

        SkipDB_beginTransaction(client->server->db);
        SkipDB_at_put_(client->server->db, dkey, dvalue);
        SkipDB_commitTransaction(client->server->db);

        t1 = time(NULL);
        tnow = localtime(&t1);
        tm2 = *tnow;
        tm2.tm_sec = tm1.tm_sec;
        tm2.tm_min = tm1.tm_min;
        tm2.tm_hour = tm1.tm_hour;
        t2 = mktime(&tm2);
        time_obj->server = client->server;
        if(t2 <= t1) {
            ev_timer_init(&time_obj->watcher, time_cmd_cb, (t1-t2+24*3600), 24*3600);
            ev_timer_start(EV_A_ &time_obj->watcher);
        } else {
            ev_timer_init(&time_obj->watcher, time_cmd_cb, (t2-t1), 24*3600);
            ev_timer_start(EV_A_ &time_obj->watcher);
        }
        p1 = "ok\n";
        client_send(EV_A_ client, p1, strlen(p1));
        ccrReturn(ctx, ccr_error_ok1);

    } else if(!strcmp(client->command, "fire")) {
        p1 = p2+1;
        p2 = strstr(p1, "\n");
        if(NULL == p2) {
            ccrStop(ctx, ccr_error_err2);
        }
        *p2 = '\0';
        //TODO check length hear
        client->key = (char*)malloc(DELAY_KEY_LEN);
        sprintf(client->key, "__event__%s", p1);
        p1 = p2+1;

        CS->n = 0;
        CS->skey = Datum_FromCString_(client->key);
        CS->record = SkipDB_list_first(client->server->db, CS->skey, &CS->cursor);
        while(NULL != CS->record) {
            dkey = SkipDBRecord_keyDatum(CS->record);
            dvalue = SkipDBRecord_valueDatum(CS->record);
            sys_script((char*)dvalue.data);

            CS->record = SkipDB_list_next(client->server->db, CS->skey, CS->cursor);
            CS->n++;
        }

        //send end
        p1 = "ok\n";
        client_send(EV_A_ client, p1, strlen(p1));
        ccrReturn(ctx, ccr_error_ok);

        if(NULL != CS->cursor) {
            SkipDBCursor_release(CS->cursor);
        }
        free(client->key);

        ccrReturn(ctx, ccr_error_ok1);
    } else if((!strcmp(client->command, "inc")) || (!strcmp(client->command, "desc"))) {
        p1 = p2+1;
        p2 = strstr(p1, " ");
        if(NULL == p2) {
            ccrStop(ctx, ccr_error_err2);
        }
        *p2 = '\0';
        client->key = p1;
        p1 = p2+1;

        dkey = Datum_FromCString_(client->key);

        if(S2ISUCCESS != str2int(&tmpi, p1, 10)) {
            p1 = "error\n";
            client_send(EV_A_ client, p1, strlen(p1));
            client->break_level = ccr_break_killed;
            ccrStop(ctx, ccr_error_err2);
        }

        dvalue = SkipDB_at_(client->server->db, dkey);
        if(NULL == dvalue.data) {
            tmp2 = 0;
        } else {
            if(S2ISUCCESS != str2int(&tmp2, (char*)dvalue.data, 10)) {
                p1 = "error\n";
                client_send(EV_A_ client, p1, strlen(p1));
                ccrReturn(ctx, ccr_error_ok1);
            }
        }

        //TODO remote \n from data?
        if(!strcmp(client->command, "inc")) {
            sprintf(static_buffer, "%d", tmp2+tmpi);
        } else {
            sprintf(static_buffer, "%d", tmp2-tmpi);
        }
        dvalue = Datum_FromCString_(static_buffer);
        SkipDB_at_put_(client->server->db, dkey, dvalue);
        client_send(EV_A_ client, (char*)dvalue.data, dvalue.size);;
        ccrReturn(ctx, ccr_error_ok1);
    }

    ccrFinish(ctx, ccr_error_err2);
}

static int client_ccr_process(EV_P_ skipd_client* client)
{
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
        if('\n' == client->origin[client->data_len-1]) {
            client->origin[client->data_len-1] = '\0';
        }

        // Command subroutine
        memset(&client->ccr_runcmd, 0, sizeof(struct ccrContextTag));
        for(;;) {
            CS->rt = client_run_command(EV_A_ client);
            if(CS->rt < 0) {
                p = "run command error\n";
                client_send(EV_A_ client, p, strlen(p));
                client->break_level = ccr_break_killed;
                ccrReturn(ctx, CS->rt);
            }
            if(CS->rt > 0) {
                break;
            }
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
    //int opt = 0;
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
    int client_fd;
    skipd_client* client;

    skipd_server* server = container_of(w, skipd_server, io);

    while (1) {
        client_fd = accept(server->fd, NULL, NULL);
        if( client_fd == -1 ) {
            if( errno != EAGAIN && errno != EWOULDBLOCK ) {
                skipd_log(SKIPD_DEBUG, "accept() failed errno=%i (%s)",  errno, strerror(errno));
                exit(EXIT_FAILURE);
            }
            break;
        }

        skipd_log(SKIPD_DEBUG, "accepted a client\n");
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
    //int opt = 0;

    unlink(sock_path);

    // Setup a unix socket listener.
    fd = socket(AF_UNIX, SOCK_STREAM, 0);
    //fd = socketpair(AF_UNIX, SOCK_STREAM, 0);
    if (-1 == fd) {
        perror("echo server socket");
        exit(EXIT_FAILURE);
    }

    //SOL_TCP
    //setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &opt, sizeof(opt));
    //opt = 1;
    //setsockopt(fd, SOL_SOCKET, SO_NOSIGPIPE, (void *)&opt, sizeof(int));

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
    //syslog(LOG_PERROR, "LOG HEAR:%s\n", server->db_path);
    SkipDB_open(server->db);

    count = SkipDB_count(server->db);
    skipd_log(SKIPD_DEBUG, "Load count=%d\n", count);

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

/* static void not_blocked(EV_P_ ev_periodic *w, int revents) {
    puts(".");
} */

static void server_init_delay(EV_P_ skipd_server *server) {
    int n1;
    char *p1;
    Datum skey, dkey, dvalue;
    SkipDBCursor* cursor = NULL;
    SkipDBRecord* record;
    delay_cmd* delay_obj;

    //Init hear
    skey = Datum_FromCString_("__delay__");
    record = SkipDB_list_first(server->db, skey, &cursor);
    while(NULL != record) {
        dkey = SkipDBRecord_keyDatum(record);
        dvalue = SkipDBRecord_valueDatum(record);

        p1 = strstr((char*)dvalue.data, " ");
        if(NULL != p1) {
            n1 = (p1 - (char*)dvalue.data);
            memcpy(static_buffer, dvalue.data, n1);
            static_buffer[n1] = '\0';
            if(S2ISUCCESS == str2int(&n1, static_buffer, 10)) {
                delay_obj = (delay_cmd*)malloc(sizeof(delay_cmd));
                memcpy(delay_obj->key, dkey.data, dkey.size);
                delay_obj->key[dkey.size] = '\0';
                delay_obj->tick = n1;
                delay_obj->server = server;

                ev_timer_init(&delay_obj->watcher, delay_cmd_cb, delay_obj->tick, delay_obj->tick);
                ev_timer_start(EV_A_ &delay_obj->watcher);
            }
        }

        record = SkipDB_list_next(server->db, skey, cursor);
    }

    if(NULL != cursor) {
        SkipDBCursor_release(cursor);
    }
}

static void server_init_time(EV_P_ skipd_server *server) {
    int n1;
    char *p1;
    Datum skey, dkey, dvalue;
    SkipDBCursor* cursor = NULL;
    SkipDBRecord* record;
    time_cmd* time_obj;
    time_t t1, t2;
    struct tm tm1, tm2, *tnow;

    //Init hear
    skey = Datum_FromCString_("__time__");
    record = SkipDB_list_first(server->db, skey, &cursor);
    while(NULL != record) {
        dkey = SkipDBRecord_keyDatum(record);
        dvalue = SkipDBRecord_valueDatum(record);

        p1 = strstr((char*)dvalue.data, " ");
        if(NULL != p1) {
            n1 = (p1 - (char*)dvalue.data);
            memcpy(static_buffer, dvalue.data, n1);
            static_buffer[n1] = '\0';

            if (strptime(static_buffer, "%H:%M:%S", &tm1) != NULL) {
                t1 = time(NULL);
                tnow = localtime(&t1);
                tm2 = *tnow;
                tm2.tm_sec = tm1.tm_sec;
                tm2.tm_min = tm1.tm_min;
                tm2.tm_hour = tm1.tm_hour;
                t2 = mktime(&tm2);

                if(t2 > t1) {
                    time_obj = (delay_cmd*)malloc(sizeof(time_cmd));
                    memcpy(time_obj->key, dkey.data, dkey.size);
                    time_obj->key[dkey.size] = '\0';
                    time_obj->server = server;

                    ev_timer_init(&time_obj->watcher, time_cmd_cb, (t2-t1), 24*3600);
                    ev_timer_start(EV_A_ &time_obj->watcher);
                } else {
                    ev_timer_init(&time_obj->watcher, time_cmd_cb, (t1-t2+24*3600), 24*3600);
                    ev_timer_start(EV_A_ &time_obj->watcher);
                }
            }
        }

        record = SkipDB_list_next(server->db, skey, cursor);
    }

    if(NULL != cursor) {
        SkipDBCursor_release(cursor);
    }
}

static void server_cmd_init(EV_P_ ev_timer *w, int revents) {
    skipd_server *server = global_server;

    //Stop first
    ev_timer_stop(EV_A_ w);

    server_init_delay(EV_A_ server);
    server_init_time(EV_A_ server);
}

int main(int argc, char **argv)
{
    int n = 0, daemon = 0, max_queue = -1;
    int syslog_options = LOG_PID | LOG_PERROR | LOG_DEBUG;
    skipd_server *server;
    //struct ev_periodic every_few_seconds;
    ev_timer init_watcher = {0};
    EV_P  = ev_default_loop(0);

    global_server = (skipd_server*)malloc(sizeof(skipd_server));
    server = global_server;

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

    setlogmask(LOG_UPTO (LOG_DEBUG));
    openlog("skipd", syslog_options, LOG_DAEMON);

    //kill -SIGUSR2 22459
    signal(SIGPIPE, SIG_IGN);
    signal(SIGABRT, SIG_IGN);
    ev_signal_init (&signal_watcher, sigint_cb, SIGINT);
    ev_signal_start (EV_A_ &signal_watcher);
    ev_signal_init (&signal_watcher2, sigint_cb, SIGUSR2);
    ev_signal_start (EV_A_ &signal_watcher2);

    // Create unix socket in non-blocking fashion
    server_init(server, max_queue);

    // Get notified whenever the socket is ready to read
    ev_io_init(&server->io, server_cb, server->fd, EV_READ);
    ev_io_start(EV_A_ &server->io);

    // To be sure that we aren't actually blocking
    //ev_periodic_init(&every_few_seconds, not_blocked, 0, 5, 0);
    //ev_periodic_start(EV_A_ &every_few_seconds);
    ev_timer_init(EV_A_ &init_watcher, server_cmd_init, 5.0, 0.0);
    ev_timer_start(EV_A_ &init_watcher);


    // Run our loop, ostensibly forever
    ev_loop(EV_A_ 0);

    SkipDB_close(server->db);
    skipd_log(SKIPD_DEBUG, "exit..\n");

    // This point is only ever reached if the loop is manually exited
    close(server->fd);
    return EXIT_SUCCESS;
}

