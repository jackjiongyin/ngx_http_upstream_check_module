/***************************************************************************
 * 
 * Copyright (c) 2017 Baidu.com, Inc. All Rights Reserved
 * $Id$ 
 * 
 **************************************************************************/
 
 /**
 * @file ngx_http_upstream_health_check_module.c
 * @author jiong(jiong@baidu.com)
 * @date 2017/12/07 12:35:29
 * @version $Revision$ 
 * @brief 
 *  
 **/

#include <ngx_config.h>
#include <ngx_core.h>
#include <ngx_http.h>

#define NGX_HTTP_CHECK_STATUS_2XX 0x00000002
#define NGX_HTTP_CHECK_STATUS_3XX 0x00000004
#define NGX_HTTP_CHECK_STATUS_4XX 0x00000008
#define NGX_HTTP_CHECK_STATUS_5XX 0x00000010

typedef struct ngx_http_upstream_health_check_peer_s ngx_http_upstream_health_check_peer_t;

typedef void (*ngx_http_upstream_check_handler_pt)(ngx_http_upstream_health_check_peer_t *peer);

typedef struct {

    ngx_str_t                           type;
    ngx_msec_t                          interval;
    ngx_msec_t                          timeout;
    ngx_uint_t                          fail_count;
    ngx_uint_t                          rise_count;

    ngx_str_t                           http_send;
    ngx_uint_t                          http_alive_status;

} ngx_http_upstream_health_check_conf_t;

struct ngx_http_upstream_health_check_peer_s {

    ngx_uint_t                             index;
    ngx_int_t                              wid;

    ngx_uint_t                             rise;
    ngx_uint_t                             fail;
    ngx_uint_t                             state;

    ngx_http_status_t                      status;
    
    struct sockaddr                       *sockaddr;
    socklen_t                              socklen;
    ngx_str_t                              name;

    ngx_event_t                            check_ev;
    ngx_event_t                            check_timeout_ev;

    ngx_http_upstream_health_check_conf_t *conf;

    ngx_peer_connection_t                  pc;

    ngx_http_upstream_rr_peer_t           *rr_peer;

    ngx_buf_t                             *send_data;
    ngx_buf_t                             *recv_data;

    ngx_http_upstream_check_handler_pt     recv_handler;
    ngx_http_upstream_check_handler_pt     send_handler;



};

typedef struct {
    ngx_str_t   *upstream_name;

    ngx_http_upstream_health_check_conf_t conf;

    ngx_array_t peers;
} ngx_http_upstream_health_check_srv_conf_t;

typedef struct {
    ngx_array_t upstreams; /* ngx_http_upstream_health_check_srv_conf_t */
} ngx_http_upstream_health_check_main_conf_t;

static ngx_str_t ngx_check_types[] = {
    ngx_string("tcp"),
    ngx_string("http"),
    ngx_null_string
};

static ngx_int_t ngx_http_upstream_health_check_init_process(ngx_cycle_t *cycle);

static void *ngx_http_upstream_health_check_create_main_conf(ngx_conf_t *cf);
static char *ngx_http_upstream_health_check_create_init_main_conf(ngx_conf_t *cf, void *conf);

static char *ngx_http_upstream_health_check(ngx_conf_t *cf, ngx_command_t *cmd,
                                            void *conf);

static ngx_int_t ngx_http_get_check_type(ngx_str_t *str);
static ngx_int_t ngx_http_upstream_health_check_add_srv_conf(ngx_conf_t *cf, 
                                                             ngx_http_upstream_health_check_srv_conf_t *ucscf);
static ngx_int_t ngx_http_upstream_health_check_init_peers(ngx_conf_t *cf, ngx_http_upstream_srv_conf_t *uscf);

static ngx_int_t ngx_http_upstream_health_check_init_timers(ngx_cycle_t *cycle);
static ngx_int_t ngx_http_upstream_health_check_add_timers(ngx_http_upstream_health_check_peer_t *peer, 
                                                           ngx_msec_t interval, ngx_log_t *log);

static void ngx_http_upstream_health_check_init_event_handler(ngx_event_t *ev);

static void ngx_http_upstream_health_check_connect_handler(ngx_event_t *ev);
static void ngx_http_upstream_health_check_event_handler(ngx_event_t *ev);
static void ngx_http_upstream_health_check_send_handler(ngx_http_upstream_health_check_peer_t *peer);
static void ngx_http_upstream_health_check_recv_handler(ngx_http_upstream_health_check_peer_t *peer);

static ngx_int_t ngx_http_upstream_health_check_send_request(ngx_connection_t *c);
static ngx_int_t ngx_http_upstream_health_check_process_response(ngx_connection_t *c);

static ngx_int_t ngx_http_upstream_health_check_connect_alive(ngx_connection_t *c);

static void ngx_http_upstream_health_check_update_status(ngx_http_upstream_health_check_peer_t *peer, ngx_int_t rc);

static ngx_int_t ngx_http_upstream_health_check_parse_status_line(ngx_http_upstream_health_check_peer_t *peer, ngx_buf_t *b);

static ngx_http_upstream_rr_peer_t *get_health_check_rr_peer(ngx_http_upstream_health_check_peer_t *peer, 
                                                             ngx_http_upstream_main_conf_t *umcf);

static void ngx_http_upstream_health_check_finalize(ngx_http_upstream_health_check_peer_t *peer);

static ngx_uint_t id = 0;

static ngx_command_t ngx_http_upstream_health_check_module_commands[] = {

    { ngx_string("health_check"),
      NGX_HTTP_UPS_CONF|NGX_CONF_ANY,
      ngx_http_upstream_health_check,
      0,
      0,
      NULL },

      ngx_null_command
};

static ngx_http_module_t ngx_http_upstream_health_check_module_ctx = {
    NULL,                                                 /* preconfiguration */
    NULL,                                                 /* postconfiguration */
    ngx_http_upstream_health_check_create_main_conf,      /* create main configuration */
    ngx_http_upstream_health_check_create_init_main_conf, /* int main configuration */
    NULL,                                                 /* create server configuration */
    NULL,                                                 /* merge server configuration */
    NULL,                                                 /* create location configuration */
    NULL,                                                 /* merge location configuration */
};

ngx_module_t ngx_http_upstream_health_check_module = {
    NGX_MODULE_V1,
    &ngx_http_upstream_health_check_module_ctx,        /* module context */
    ngx_http_upstream_health_check_module_commands,    /* module directives */
    NGX_HTTP_MODULE,                                   /* module type */
    NULL,                                              /* init master */
    NULL,                                              /* init module */
    ngx_http_upstream_health_check_init_process,       /* init process */
    NULL,                                              /* init thread */
    NULL,                                              /* exit thread */
    NULL,                                              /* exit process */
    NULL,                                              /* exit master */
    NGX_MODULE_V1_PADDING
};

static void *
ngx_http_upstream_health_check_create_main_conf(ngx_conf_t *cf)
{
    ngx_http_upstream_health_check_main_conf_t *conf;

    conf = ngx_pcalloc(cf->pool,
                       sizeof(ngx_http_upstream_health_check_main_conf_t));

    if (conf == NULL) {
        return NULL;
    }

#if (NGX_DEBUG)
    if (ngx_array_init(&conf->upstreams, cf->pool, 1, 
                       sizeof(ngx_http_upstream_health_check_srv_conf_t)) != NGX_OK) {
        return NULL;
    }
#else
    if (ngx_array_init(&conf->upstreams, cf->pool, 1024,
                       sizeof(ngx_http_upstream_health_check_srv_conf_t)) != NGX_OK) {
        return NULL;
    }
#endif

    return conf;
}

static char *
ngx_http_upstream_health_check_create_init_main_conf(ngx_conf_t *cf, void *conf)
{
    ngx_http_upstream_main_conf_t              *umcf;
    ngx_http_upstream_srv_conf_t               **uscfp;
    ngx_uint_t                                 i;

    umcf = ngx_http_conf_get_module_main_conf(cf, ngx_http_upstream_module);
    uscfp = umcf->upstreams.elts;

    for(i = 0; i < umcf->upstreams.nelts; i++) {
        if (ngx_http_upstream_health_check_init_peers(cf, uscfp[i]) != NGX_OK) {
            return NGX_CONF_ERROR;
        }
    }

    return NGX_CONF_OK;
}

static ngx_int_t
ngx_http_upstream_health_check_init_peers(ngx_conf_t *cf, ngx_http_upstream_srv_conf_t *uscf)
{    

    ngx_http_upstream_health_check_main_conf_t *ucmcf;
    ngx_http_upstream_health_check_srv_conf_t  *ucscfp, *ucscf;
    ngx_http_upstream_health_check_peer_t      *peer;
    ngx_http_upstream_server_t                 *server;
    ngx_uint_t                                  i, j;
    
    ucmcf = ngx_http_conf_get_module_main_conf(cf, ngx_http_upstream_health_check_module);

    if (ucmcf == NULL) {
        return NGX_ERROR;
    }

    ucscfp = ucmcf->upstreams.elts;
    ucscf = NULL;

    for (i = 0; i < ucmcf->upstreams.nelts; i++) {
        if (ucscfp[i].upstream_name == &uscf->host) {
            ucscf = &ucscfp[i];
            break;
        }
    }

    if (ucscf != NULL && uscf->servers) {

        server = uscf->servers->elts;

        for (i = 0; i < uscf->servers->nelts; i++) {
            if (server[i].backup) {
                continue;
            }

            for (j = 0; j < server[i].naddrs; j++) {
                peer = ngx_array_push(&ucscf->peers);

                peer->rise = 0;
                peer->fail = 0;
                peer->index = id;
                peer->state = 0;
                peer->wid = NGX_CONF_UNSET;

                peer->name = server[i].addrs[j].name;
                peer->sockaddr = server[i].addrs[j].sockaddr;
                peer->socklen = server[i].addrs[j].socklen;

                peer->conf = &ucscf->conf;
                peer->rr_peer = NGX_CONF_UNSET_PTR;

                ngx_memzero(&peer->status, sizeof(ngx_http_status_t));

                if (peer->conf->http_send.len > 0 && peer->conf->http_send.data != NULL) {
                    peer->send_data = ngx_create_temp_buf(cf->pool, peer->conf->http_send.len);
                    if (peer->send_data == NULL) {
                        return NGX_ERROR;
                    }

                    peer->send_data->last = peer->send_data->pos + peer->conf->http_send.len;
                    ngx_memcpy(peer->send_data->start, peer->conf->http_send.data, peer->conf->http_send.len);
                }

                peer->recv_data = ngx_create_temp_buf(cf->pool, ngx_pagesize);
                if (peer->send_data == NULL) {
                    return NGX_ERROR;
                }

                id++;
            }

        }
    }

    return NGX_OK;
}

static char *
ngx_http_upstream_health_check(ngx_conf_t *cf, ngx_command_t *cmd, void *conf)
{
    ngx_http_upstream_health_check_srv_conf_t  *ucscf;
    ngx_http_upstream_health_check_main_conf_t *ucmcf;
    ngx_int_t                                  rv;

    ucmcf = ngx_http_conf_get_module_main_conf(cf, ngx_http_upstream_health_check_module);

    ucscf = ngx_array_push(&ucmcf->upstreams);
    rv = ngx_http_upstream_health_check_add_srv_conf(cf, ucscf);

    if (rv == NGX_ERROR) {
        return NGX_CONF_ERROR;
    }
    
    return NGX_CONF_OK;
}

static ngx_int_t
ngx_http_upstream_health_check_add_srv_conf(ngx_conf_t *cf, ngx_http_upstream_health_check_srv_conf_t *ucscf) 
{
    ngx_str_t                             *value, str;
    ngx_uint_t                             i;
    ngx_msec_t                             interval, timeout;
    ngx_uint_t                             rise, fail;
    ngx_http_upstream_srv_conf_t          *uscf;
    ngx_http_upstream_health_check_conf_t *conf;

    if (ucscf == NULL) {
        return NGX_ERROR;
    }

    uscf = ngx_http_conf_get_module_srv_conf(cf, ngx_http_upstream_module);

    conf = &ucscf->conf;

    ucscf->upstream_name = &uscf->host;

    conf->interval = NGX_CONF_UNSET_MSEC;
    conf->timeout = NGX_CONF_UNSET_MSEC;
    conf->fail_count = NGX_CONF_UNSET_UINT;
    conf->rise_count = NGX_CONF_UNSET_UINT;
    conf->http_alive_status = NGX_CONF_UNSET_UINT;
    ngx_str_null(&conf->http_send);
    ngx_str_null(&conf->type);
 
    value = cf->args->elts;
    
    for (i = 1; i < cf->args->nelts; i++) {
        if (ngx_strncmp(value[i].data, "type=", 5) == 0) {
            str.len = value[i].len - 5;
            str.data = value[i].data + 5;

            if (ngx_http_get_check_type(&str)) { 
                conf->type.len = str.len;
                conf->type.data = (u_char *)str.data;
            }

            continue;
        }

        if (ngx_strncmp(value[i].data, "interval=", 9) == 0) {
            str.len = value[i].len - 9;
            str.data = value[i].data + 9;

            interval = ngx_atoi(str.data, str.len);

            if (interval != (ngx_msec_t)NGX_ERROR && interval != 0) {
                conf->interval = interval;
            }

            continue;
        }

        if (ngx_strncmp(value[i].data, "timeout=", 8) == 0) {
            str.len = value[i].len - 8;
            str.data = value[i].data + 8;

            timeout = ngx_atoi(str.data, str.len);

            if (timeout != (ngx_msec_t)NGX_ERROR && timeout != 0) {
                conf->timeout = timeout;
            }

            continue;
        }

        if (ngx_strncmp(value[i].data, "rise=", 5) == 0) {
            str.len = value[i].len - 5;
            str.data = value[i].data + 5;

            rise = ngx_atoi(str.data, str.len);
            if (rise != (ngx_uint_t)NGX_ERROR && rise != 0) {
                conf->rise_count = rise;
            }

            continue;
        }

        if (ngx_strncmp(value[i].data, "fail=", 5) == 0) {
            str.len = value[i].len - 5;
            str.data = value[i].data + 5;

            fail = ngx_atoi(str.data, str.len);
            if (fail != (ngx_uint_t)NGX_ERROR && fail != 0) {
                conf->fail_count = fail;
            }
            continue;
        }
    }

    if (conf->type.len == 0 && conf->type.data == NULL) {
        ngx_str_set(&conf->type, "tcp");
    }

    if (conf->http_send.len == 0 && conf->http_send.data == NULL) {
        ngx_str_set(&conf->http_send, "HEAD / HTTP/1.0\r\n\r\n");
    }

    if (conf->interval == NGX_CONF_UNSET_MSEC) {
        conf->interval = 5000;
    }

    if (conf->timeout == NGX_CONF_UNSET_MSEC) {
        conf->timeout = 1000;
    }

    if (conf->rise_count == NGX_CONF_UNSET_MSEC) {
        conf->rise_count = 1;
    }
    
    if (conf->fail_count == NGX_CONF_UNSET_MSEC) {
        conf->fail_count = 2;
    }

    if (conf->http_alive_status == NGX_CONF_UNSET_MSEC) {
        conf->http_alive_status = NGX_HTTP_CHECK_STATUS_2XX | NGX_HTTP_CHECK_STATUS_3XX;
    }

#if (NGX_DEBUG)

    if (ngx_array_init(&ucscf->peers, cf->pool, 1, 
                        sizeof(ngx_http_upstream_health_check_peer_t)) != NGX_OK) {
        return NGX_ERROR;
    }
    
#else

    if (ngx_array_init(&ucscf->peers, cf->pool, 1024, 
                        sizeof(ngx_http_upstream_health_check_peer_t)) != NGX_OK) {
        return NGX_ERROR;
    }

#endif

    return NGX_OK;
}

static ngx_int_t
ngx_http_upstream_health_check_init_process(ngx_cycle_t *cycle)
{
    ngx_http_upstream_health_check_main_conf_t *ucmcf;

    ucmcf = ngx_http_cycle_get_module_main_conf(cycle, ngx_http_upstream_health_check_module);
    
    if (ucmcf == NULL) {
        return NGX_OK;
    }

    return ngx_http_upstream_health_check_init_timers(cycle);
}

static ngx_int_t
ngx_http_upstream_health_check_init_timers(ngx_cycle_t *cycle)
{
    ngx_http_upstream_health_check_main_conf_t *ucmcf;
    ngx_http_upstream_health_check_srv_conf_t  *ucscf;
    ngx_http_upstream_health_check_peer_t      *peers;
    ngx_http_upstream_main_conf_t              *umcf;
    ngx_core_conf_t                            *ccf;
    ngx_uint_t                                  i, j;
    
    ucmcf = ngx_http_cycle_get_module_main_conf(cycle, ngx_http_upstream_health_check_module);
    umcf = ngx_http_cycle_get_module_main_conf(cycle, ngx_http_upstream_module);
    ccf = (ngx_core_conf_t *) ngx_get_conf(cycle->conf_ctx, ngx_core_module);

    ucscf = ucmcf->upstreams.elts;

    for (i = 0; i < ucmcf->upstreams.nelts; i++) {
        peers = (ucscf[i].peers).elts;
        for (j = 0; j < (ucscf[i].peers).nelts; j++) {
            if (peers[j].wid == NGX_CONF_UNSET && peers[j].rr_peer == NGX_CONF_UNSET_PTR) {
                peers[j].wid = peers[j].index % ccf->worker_processes;
                peers[j].rr_peer = get_health_check_rr_peer(&peers[j], umcf);
                ngx_http_upstream_health_check_add_timers(&peers[j], ucscf[i].conf.interval, cycle->log);
            }
        }
    }

    return NGX_OK;
}

static ngx_http_upstream_rr_peer_t *
get_health_check_rr_peer(ngx_http_upstream_health_check_peer_t *peer, ngx_http_upstream_main_conf_t *umcf)
{
    ngx_http_upstream_srv_conf_t **uscfp;
    ngx_http_upstream_rr_peers_t *rr_peers;
    ngx_http_upstream_rr_peer_t  *rr_peer;
    ngx_uint_t                    i;

    uscfp = umcf->upstreams.elts;

    rr_peer = NULL;

    for (i = 0; i< umcf->upstreams.nelts; i++) {
        rr_peers = uscfp[i]->peer.data;
        for (rr_peer = rr_peers->peer; rr_peer; rr_peer = rr_peer->next) {
            if (ngx_strncmp(rr_peer->name.data, peer->name.data, peer->name.len) == 0) {
                return rr_peer;
            }
        }
    }

    return rr_peer;
}

static ngx_int_t
ngx_http_upstream_health_check_add_timers(ngx_http_upstream_health_check_peer_t *peer, 
                                          ngx_msec_t interval, ngx_log_t *log)
{
    if (peer->wid != ngx_process_slot) {
        return NGX_OK;
    }

    peer->check_ev.handler = ngx_http_upstream_health_check_init_event_handler;
    peer->check_ev.data = peer;
    peer->check_ev.log = log;
    peer->check_ev.timer_set = 0;

    peer->send_handler = ngx_http_upstream_health_check_send_handler;
    peer->recv_handler = ngx_http_upstream_health_check_recv_handler;

    ngx_add_timer(&peer->check_ev, interval);

    return NGX_OK;
}

static void
ngx_http_upstream_health_check_init_event_handler(ngx_event_t *ev)
{
    ngx_http_upstream_health_check_peer_t *peer;
    
    peer = ev->data;

    ngx_add_timer(&peer->check_ev, peer->conf->interval);

    ngx_http_upstream_health_check_connect_handler(ev);
}

static void
ngx_http_upstream_health_check_connect_handler(ngx_event_t *ev)
{
    ngx_http_upstream_health_check_peer_t *peer;
    ngx_connection_t                      *c;
    ngx_peer_connection_t                 *pc;
    ngx_log_t                             *log;
    ngx_int_t                              rc;

    peer = ev->data;
    log = ev->log;

    pc = &peer->pc;

    pc->log = log;
    pc->log_error =  NGX_ERROR_ERR;
    pc->start_time = ngx_current_msec;
    pc->get = ngx_event_get_peer;
    pc->sockaddr = peer->sockaddr;
    pc->socklen = peer->socklen;
    pc->name =  &peer->name;

    if (pc->connection != NULL) {
        if (ngx_http_upstream_health_check_connect_alive(pc->connection) != NGX_OK){
            ngx_log_debug0(NGX_LOG_DEBUG_HTTP, ev->log, 0, "peer close connection");
            ngx_http_upstream_health_check_finalize(peer);
        } else {
            ngx_log_debug0(NGX_LOG_DEBUG_HTTP, ev->log, 0, "peer  connection ok");
            goto connect_done;
        }
    }

    rc = ngx_event_connect_peer(pc);

    if (rc == NGX_ERROR || rc == NGX_DECLINED) {
        ngx_http_upstream_health_check_update_status(peer, 0);
        return ;
    }

    c = pc->connection;

    c->data = peer;

    c->log = peer->pc.log;
    c->write->log = c->log;
    c->read->log = c->log;

    c->write->handler = ngx_http_upstream_health_check_event_handler;
    c->read->handler = ngx_http_upstream_health_check_event_handler;

    if (rc == NGX_AGAIN) {
        ngx_add_timer(c->write, peer->conf->timeout);
        return ;
    }

connect_done:

    ngx_http_upstream_health_check_send_handler(peer);

}

static ngx_int_t
ngx_http_upstream_health_check_connect_alive(ngx_connection_t *c) {
    char      buf[1];
    ngx_err_t err;
    ngx_int_t n;

    n = recv(c->fd, buf, 1, MSG_PEEK);

    err = ngx_socket_errno;

    ngx_log_debug2(NGX_LOG_DEBUG_HTTP, c->log, 0, "conection check client: %z, %z", n, err);

    if (n == 1 || (n == -1 && err == NGX_EAGAIN)) {
        ngx_log_debug0(NGX_LOG_DEBUG_HTTP, c->log, 0, "conection check client: alive");
        return NGX_OK;
    } else {
        ngx_log_debug0(NGX_LOG_DEBUG_HTTP, c->log, 0, "conection check client: death");
        return NGX_ERROR;
    }

}



static void
ngx_http_upstream_health_check_event_handler(ngx_event_t *ev)
{
    ngx_connection_t *c;
    ngx_http_upstream_health_check_peer_t *peer;

    c = ev->data;
    peer = c->data;

    if (ev->write) {
        peer->send_handler(peer);
    } else {
        peer->recv_handler(peer);
    }

}

static void
ngx_http_upstream_health_check_send_handler(ngx_http_upstream_health_check_peer_t *peer)
{
    ngx_connection_t *c;
    ngx_int_t         rc;
    
    c = peer->pc.connection;

    if (c->write->timer_set) {
        ngx_del_timer(c->write);
    }

    if (c->write->timedout) {
        ngx_log_debug0(NGX_LOG_DEBUG_HTTP, c->log, 0, "connection write timeout");
        ngx_http_upstream_health_check_update_status(peer, 0);
        ngx_http_upstream_health_check_finalize(peer);
        return ;
    }

    if (ngx_http_upstream_health_check_connect_alive(c) != NGX_OK) {
        ngx_http_upstream_health_check_update_status(peer, 0);
        ngx_http_upstream_health_check_finalize(peer);
        return;
    }

    rc = ngx_http_upstream_health_check_send_request(c);

    ngx_log_debug1(NGX_LOG_DEBUG_HTTP, c->log, 0, "connection write rc : %z", rc);

    if (rc == NGX_ERROR) {
        ngx_http_upstream_health_check_update_status(peer, 0);
        ngx_http_upstream_health_check_finalize(peer);
        return ;
    }

    if (rc == NGX_AGAIN) {
    }

    /* rc == NGX_OK */

    ngx_add_timer(c->read, peer->conf->timeout);

    if (c->read->ready) {
        ngx_http_upstream_health_check_recv_handler(peer);
        return;
    }

}

static ngx_int_t
ngx_http_upstream_health_check_send_request(ngx_connection_t *c)
{
    ngx_http_upstream_health_check_peer_t *peer;
    ngx_buf_t                             *b;
    ssize_t                                size;

    peer = c->data;
    b = peer->send_data;

    if (b == NULL) {
        return NGX_ERROR;
    }

    b->pos = b->start;

    while (b->pos < b->last) {
        size = c->send(c, b->pos, b->last - b->pos);

        if (size > 0) {
            b->pos += size;
        } else {
            return NGX_ERROR;
        }
    }

    if (b->pos != b->last) {
        return NGX_ERROR;
    }

    return NGX_OK;
}

static void
ngx_http_upstream_health_check_recv_handler(ngx_http_upstream_health_check_peer_t *peer)
{
    ngx_connection_t *c;
    ngx_int_t         rc;

    c = peer->pc.connection;

    if (c->read->timer_set) {
        ngx_del_timer(c->read);
    }

    if (c->read->timedout) {
        ngx_http_upstream_health_check_update_status(peer, 0);
        ngx_http_upstream_health_check_finalize(peer);
        return;
    }

    if (ngx_http_upstream_health_check_connect_alive(c) != NGX_OK) {
        ngx_http_upstream_health_check_update_status(peer, 0);
        ngx_http_upstream_health_check_finalize(peer);
        return;
    }

    rc = ngx_http_upstream_health_check_process_response(c);

    if (rc == NGX_OK || rc == NGX_ERROR) {
       
        if(rc == NGX_OK && peer->status.code == 200) {
            ngx_http_upstream_health_check_update_status(peer, 1);
        }

        if (rc == NGX_ERROR || peer->status.code != 200) {
            ngx_http_upstream_health_check_update_status(peer, 0);
        }

        ngx_log_debug5(NGX_LOG_DEBUG_HTTP, c->log, 0, 
                "parse status line : rc=%z, version=%z, status=%z, rise=%z, fail=%z"
                , rc, peer->status.http_version, peer->status.code, peer->rise, peer->fail);
 
        ngx_http_upstream_health_check_finalize(peer);

    }
    
}

static void ngx_http_upstream_health_check_update_status(ngx_http_upstream_health_check_peer_t *peer, ngx_int_t rc) {

    if (rc == 1) {
         peer->rise += 1;
    }  else {
         peer->fail += 1;
    } 

    ngx_log_debug3(NGX_LOG_DEBUG_HTTP, peer->pc.log, 0, 
            "health check peer status : name=%V, rise=%z, fail=%z"
            , &peer->name, peer->rise, peer->fail);
      
    if (peer->rise == peer->conf->rise_count) {

        peer->fail = 0;

        if (peer->rr_peer->down == 1) {
            peer->rr_peer->down = 0;

            ngx_log_debug1(NGX_LOG_DEBUG_HTTP, peer->pc.log, 0, 
                    "health check peer down !!!!! : name=%V", &peer->name);
        }

    }  else if (peer->fail == peer->conf->fail_count) {
        
        peer->rise = 0;

        if (peer->rr_peer->down == 0) {
            peer->rr_peer->down = 1;

            ngx_log_debug1(NGX_LOG_DEBUG_HTTP, peer->pc.log, 0, 
                    "health check peer up !!!!! : name=%V", &peer->name);
        }

    }
}

static ngx_int_t ngx_http_upstream_health_check_process_response(ngx_connection_t *c)
{
    ngx_http_upstream_health_check_peer_t *peer;
    ssize_t                                size;
    ngx_buf_t                             *b;
    ngx_int_t                              rc;

    peer = c->data;
    b = peer->recv_data;

    while(1) {
        size = c->recv(c, b->last, b->end - b->last);

        if (size == NGX_AGAIN) {

            if (ngx_handle_read_event(c->read, 0) != NGX_OK) {
                return NGX_ERROR;
            }

            return NGX_AGAIN;
        }

        if (size == NGX_ERROR || size <= 0) {
            return NGX_ERROR;
        }

        b->last += size;

        rc = ngx_http_upstream_health_check_parse_status_line(peer, b);

        if (rc == NGX_AGAIN) {
            continue;
        }

        break;
    }

    return rc;
}

static void
ngx_http_upstream_health_check_finalize(ngx_http_upstream_health_check_peer_t *peer) {
    ngx_connection_t *c;
    ngx_buf_t        *send_buf, *recv_buf;

    c = peer->pc.connection;

    recv_buf = peer->recv_data;
    send_buf = peer->send_data;
 
    if (c != NULL) {
        if (c->write->timer_set) {
            ngx_del_timer(c->write);
        }

        if (c->read->timer_set) {
            ngx_del_timer(c->read);
        }

        ngx_close_connection(c);
        peer->pc.connection = NULL;
    }

    if (&peer->status != NULL) {
        ngx_memzero(&peer->status, sizeof(ngx_http_status_t));
    }

    if (recv_buf != NULL) {
        recv_buf->pos = recv_buf->last = recv_buf->start;
    }

    if (send_buf != NULL) {
        send_buf->pos = send_buf->start;
        send_buf->last = send_buf->end;
    }
}

static ngx_int_t
ngx_http_upstream_health_check_parse_status_line(ngx_http_upstream_health_check_peer_t *peer, ngx_buf_t *b)
{
    u_char ch;
    u_char *p;

    enum {
        sw_start = 0,
        sw_H,
        sw_HT,
        sw_HTT,
        sw_HTTP,
        sw_first_major_digit,
        sw_major_digit,
        sw_first_minor_digit,
        sw_minor_digit,
        sw_status,
        sw_space_after_status,
        sw_status_text,
        sw_almost_done
    } state;

    state = peer->state;

    for (p = b->pos; p < b->last; p++) {
        ch = *p;

        switch(state) {
            case sw_start:
                switch(ch) {
                    case 'H':
                        state = sw_H;
                        break;
                    default:
                        return NGX_ERROR;
                }
                break;
            case sw_H:
                switch(ch) {
                    case 'T':
                        state = sw_HT;
                        break;
                    default:
                        return NGX_ERROR;
                }
                break;
            case sw_HT:
                switch(ch) {
                    case 'T':
                        state = sw_HTT;
                        break;
                    default:
                        return NGX_ERROR;
                }
                break;
            case sw_HTT:
                switch(ch) {
                    case 'P':
                        state = sw_HTTP;
                        break;
                    default:
                        return NGX_ERROR;
                }
                break;
            case sw_HTTP:
                switch(ch) {
                    case '/':
                        state = sw_first_major_digit;
                        break;
                    default:
                        return NGX_ERROR;
                }
                break;
            case sw_first_major_digit:
                if (ch < '1' || ch > '9') {
                    return NGX_ERROR;
                }
                peer->status.http_version = (ch - '0') * 1000;
                state = sw_major_digit;
                break;
            case sw_major_digit:
                if (ch != '.') {
                    return NGX_ERROR;
                }
                state = sw_first_minor_digit;
                break;
            case sw_first_minor_digit:
                if (ch < '1' || ch > '9') {
                    return NGX_ERROR;
                }
                peer->status.http_version += ch - '0';
                state = sw_minor_digit;
                break;
            case sw_minor_digit:
                if (ch != ' ') {
                    return NGX_ERROR;
                }
                state = sw_status;
                break;
            case sw_status:
                if (ch == ' ') {
                    break;
                }
                if (ch < '0' || ch > '9') {
                    return NGX_ERROR;
                }
                peer->status.code = peer->status.code * 10 + ch - '0';
                if (++peer->status.count == 3) {
                    state = sw_space_after_status;
                    peer->status.start = p - 2;
                }
                break;
            case sw_space_after_status:
                if (ch != ' ') {
                    return NGX_ERROR;
                }
                state = sw_status_text;
                break;
            case sw_status_text:
                switch(ch) {
                    case CR:
                        state = sw_almost_done;
                        break;
                    case LF:
                        goto done;
                }
                break;
            case sw_almost_done:
                switch(ch) {
                    case LF:
                        goto done;
                    default:
                        return NGX_ERROR;
                }
                break;
        }
    }
    b->pos = p;
    peer->state = state;

    return NGX_AGAIN;

done:
    b->pos = p + 1;
    if (peer->status.end == NULL) {
        peer->status.end = p;
    }

    peer->state = sw_start;

    return NGX_OK;
}

static ngx_int_t
ngx_http_get_check_type(ngx_str_t *str)
{
    ngx_uint_t i;

    for (i = 0; ; i++) {
        if (ngx_strncmp(str->data, ngx_check_types[i].data, ngx_check_types[i].len) == 0) {
            return NGX_OK;
        }
    }

    return NGX_ERROR;
}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
