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

#define NGX_CHECK_HTTP_STATUS_2XX 0x00000002
#define NGX_CHECK_HTTP_STATUS_3XX 0x00000004
#define NGX_CHECK_HTTP_STATUS_4XX 0x00000008
#define NGX_CHECK_HTTP_STATUS_5XX 0x00000010

#define NGX_CHECK                 0          
#define NGX_CHECK_CONNECT_DONE    1
#define NGX_CHECK_SEND_DONE       2
#define NGX_CHECK_RECV_DONE       3

//shared memory size, at lease 1M?
#define SHM_NAME "check_shm"
#define SHM_SIZE 1 * 1024 * 1024

#define ngx_http_check_peer_lock(peer)        \
                                              \
    if (peer->shpool) {                       \
        ngx_shmtx_lock(&peer->shpool->mutex); \
    }                                         

#define ngx_http_check_peer_unlock(peer)        \
                                                \
    if (peer->shpool) {                         \
        ngx_shmtx_unlock(&peer->shpool->mutex); \
    }                                         

typedef struct ngx_http_upstream_health_check_peer_s ngx_http_upstream_health_check_peer_t;

typedef struct {
    ngx_str_t            name;
    ngx_str_t            http_send;
    ngx_event_handler_pt send_handler;
    ngx_event_handler_pt recv_handler;
} ngx_peer_check_type;

typedef struct {

    ngx_str_t                             *upstream_name;

    ngx_peer_check_type                 type;

    ngx_msec_t                          interval;
    ngx_msec_t                          timeout;

    ngx_uint_t                          fail_threshold;
    ngx_uint_t                          rise_threshold;

    ngx_uint_t                          alive_http_status;

} ngx_http_upstream_health_check_conf_t;

typedef struct ngx_http_upstream_check_peer_shm_s ngx_http_upstream_check_peer_shm_t;

struct ngx_http_upstream_check_peer_shm_s {
    ngx_uint_t                          index;

    ngx_int_t                           wid;

    ngx_uint_t                          rise;
    ngx_uint_t                          fail;

    struct sockaddr                    *sockaddr;
    socklen_t                           socklen;
    ngx_str_t                          *name;

    ngx_http_upstream_rr_peer_t        *rr_peer;

    ngx_http_upstream_check_peer_shm_t *next;
};

typedef struct {
    ngx_uint_t                          peer_sum;
    ngx_http_upstream_check_peer_shm_t *head;
} ngx_http_upstream_check_peers_shm_t;

struct ngx_http_upstream_health_check_peer_s {

    ngx_uint_t                             index;

    ngx_int_t                              check_state;
    ngx_uint_t                             state;
    ngx_http_status_t                      status;
    
    struct sockaddr                       *sockaddr;
    socklen_t                              socklen;
    ngx_str_t                              name;

    ngx_event_t                            check_ev;
    ngx_event_t                            check_timeout_ev;

    ngx_http_upstream_health_check_conf_t *conf;

    ngx_peer_connection_t                  pc;

    ngx_pool_t                            *pool;

    ngx_buf_t                             *send_data;
    ngx_buf_t                             *recv_data;

    ngx_http_upstream_check_peer_shm_t    *shm;
    ngx_slab_pool_t                       *shpool;

    ngx_log_t                             *log;
};

typedef struct {

    ngx_http_upstream_health_check_conf_t *conf;

    ngx_array_t peers;
} ngx_http_upstream_health_check_srv_conf_t;

typedef struct {
    ngx_array_t                          upstreams; /* ngx_http_upstream_health_check_srv_conf_t */

    ngx_array_t                          confs; /* ngx_http_upstream_health_check_conf_t */

    ngx_http_upstream_check_peers_shm_t *peers_shm; // next step ? delete ?

} ngx_http_upstream_health_check_main_conf_t;

static ngx_int_t ngx_http_upstream_health_check_init_process(ngx_cycle_t *cycle);

static void *ngx_http_upstream_health_check_create_main_conf(ngx_conf_t *cf);
static char *ngx_http_upstream_health_check_create_init_main_conf(ngx_conf_t *cf, void *conf);

static char *ngx_http_upstream_health_check(ngx_conf_t *cf, ngx_command_t *cmd, void *conf);
static char *ngx_http_check_set_http_send(ngx_conf_t *cf, ngx_command_t *cmd, void *conf);
static char *ngx_http_check_set_alive_http_status(ngx_conf_t *cf, ngx_command_t *cmd, void *conf);

static ngx_peer_check_type *ngx_http_get_peer_check_type(ngx_str_t *str);

static ngx_int_t ngx_http_upstream_health_check_add_srv_conf(ngx_conf_t *cf, 
                                                             ngx_http_upstream_health_check_srv_conf_t *ucscf);
static ngx_int_t ngx_http_upstream_health_check_init_peers(ngx_conf_t *cf, ngx_http_upstream_srv_conf_t *uscf);

static ngx_int_t ngx_http_upstream_health_check_init_timers(ngx_cycle_t *cycle);
static ngx_int_t ngx_http_upstream_health_check_add_timers(ngx_http_upstream_health_check_peer_t *peer, 
                                                           ngx_msec_t interval, ngx_log_t *log);

static void ngx_http_upstream_health_check_init_event_handler(ngx_event_t *ev);
static void ngx_http_upstream_health_check_timeout_handler(ngx_event_t *ev);

static void ngx_http_upstream_health_check_connect_handler(ngx_event_t *ev);
static void ngx_http_upstream_health_check_send_handler(ngx_event_t *ev);
static void ngx_http_upstream_health_check_recv_handler(ngx_event_t *ev);
static void ngx_http_upstream_health_check_peek_handler(ngx_event_t *ev);

static ngx_int_t ngx_http_upstream_health_check_send_request(ngx_connection_t *c);
static ngx_int_t ngx_http_upstream_health_check_process_response(ngx_connection_t *c);

static ngx_int_t ngx_http_upstream_health_check_connect_alive(ngx_connection_t *c);

static void ngx_http_upstream_health_check_update_status(ngx_http_upstream_health_check_peer_t *peer, ngx_int_t rc);

static ngx_int_t ngx_http_upstream_health_check_parse_status_line(ngx_http_upstream_health_check_peer_t *peer, ngx_buf_t *b);

static ngx_http_upstream_rr_peer_t *get_health_check_rr_peer(ngx_http_upstream_health_check_peer_t *peer, 
                                                             ngx_http_upstream_main_conf_t *umcf);

static void ngx_http_upstream_health_check_finish_handler(ngx_http_upstream_health_check_peer_t *peer, ngx_int_t need_reinit_ev);

static ngx_int_t ngx_http_upstream_health_check_need_exit(ngx_event_t *ev);

static ngx_int_t ngx_http_upstream_health_check_init_shm(ngx_shm_zone_t *shm_zone, void *data);

static ngx_int_t ngx_http_upstream_health_check_init_send_data_buf(ngx_http_upstream_health_check_peer_t *peer);
static ngx_int_t ngx_http_upstream_health_check_init_recv_data_buf(ngx_http_upstream_health_check_peer_t *peer);
static void ngx_http_upstream_health_check_clear(ngx_http_upstream_health_check_peer_t *peer);

static ngx_http_upstream_health_check_conf_t *ngx_http_check_get_conf(ngx_conf_t *cf, ngx_array_t *confs);

static ngx_uint_t id = 0;

static ngx_peer_check_type ngx_peer_check_types[] = {

    {
        ngx_string("http"),
        ngx_string("HEAD / HTTP/1.0\r\n\r\n"),
        ngx_http_upstream_health_check_send_handler,
        ngx_http_upstream_health_check_recv_handler,
    },

    {
        ngx_string("tcp"),
        ngx_null_string,
        ngx_http_upstream_health_check_peek_handler,
        ngx_http_upstream_health_check_peek_handler,
    }

};

static ngx_conf_bitmask_t ngx_peer_check_http_statuses[] = {
    { ngx_string("2xx"), NGX_CHECK_HTTP_STATUS_2XX },
    { ngx_string("3xx"), NGX_CHECK_HTTP_STATUS_3XX },
    { ngx_string("4xx"), NGX_CHECK_HTTP_STATUS_4XX },
    { ngx_string("5xx"), NGX_CHECK_HTTP_STATUS_5XX },
    { ngx_null_string, 0 }
};

static ngx_command_t ngx_http_upstream_health_check_module_commands[] = {

    { ngx_string("health_check"),
      NGX_HTTP_UPS_CONF|NGX_CONF_ANY,
      ngx_http_upstream_health_check,
      0,
      0,
      NULL },

    { ngx_string("http_send"),
      NGX_HTTP_UPS_CONF|NGX_CONF_TAKE1,
      ngx_http_check_set_http_send,
      0,
      0,
      NULL },

    { ngx_string("alive_http_status"),
      NGX_HTTP_UPS_CONF|NGX_CONF_TAKE1234,
      ngx_http_check_set_alive_http_status,
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
    ngx_shm_zone_t                             *shm_zone;
    ngx_str_t                                  shm_name = ngx_string(SHM_NAME);

    conf = ngx_pcalloc(cf->pool,
                       sizeof(ngx_http_upstream_health_check_main_conf_t));

    if (conf == NULL) {
        return NULL;
    }

    conf->peers_shm = NGX_CONF_UNSET_PTR;

    shm_zone = ngx_shared_memory_add(cf, &shm_name, SHM_SIZE, &ngx_http_upstream_health_check_module);
    if (shm_zone == NULL) {
        ngx_conf_log_error(NGX_LOG_ERR, cf, 0,
                "[check] create main conf: create shm_zone fail");
        return NGX_CONF_ERROR;
    }
    shm_zone->init = ngx_http_upstream_health_check_init_shm;
    shm_zone->data = conf;
    shm_zone->noreuse = 1;

#if (NGX_DEBUG)
    if (ngx_array_init(&conf->upstreams, cf->pool, 1, 
                       sizeof(ngx_http_upstream_health_check_srv_conf_t)) != NGX_OK) {
        ngx_conf_log_error(NGX_LOG_ERR, cf, 0,
                "[check] create main conf: init srv_conf fail");
        return NULL;
    }
    if (ngx_array_init(&conf->confs, cf->pool, 1, 
                       sizeof(ngx_http_upstream_health_check_conf_t)) != NGX_OK) {
        ngx_conf_log_error(NGX_LOG_ERR, cf, 0,
                "[check] create main conf: init check_conf fail");
        return NULL;
    }
#else
    if (ngx_array_init(&conf->upstreams, cf->pool, 1024,
                       sizeof(ngx_http_upstream_health_check_srv_conf_t)) != NGX_OK) {
        ngx_conf_log_error(NGX_LOG_ERR, cf, 0,
                "[check] create main conf: init srv_conf fail");
        return NULL;
    }
    if (ngx_array_init(&conf->confs, cf->pool, 1024,
                       sizeof(ngx_http_upstream_health_check_conf_t)) != NGX_OK) {
        ngx_conf_log_error(NGX_LOG_ERR, cf, 0,
                "[check] create main conf: init check_conf fail");
        return NULL;
    }
#endif

    ngx_log_debug0(NGX_LOG_DEBUG_HTTP, cf->log, 0,
                "[check] create main conf: success");

    return conf;
}

static char *
ngx_http_upstream_health_check_create_init_main_conf(ngx_conf_t *cf, void *conf)
{
    ngx_http_upstream_main_conf_t              *umcf;
    ngx_http_upstream_srv_conf_t               **uscfp;
    ngx_uint_t                                 i;

    umcf = ngx_http_conf_get_module_main_conf(cf, ngx_http_upstream_module);
    if (umcf == NULL) {
        ngx_conf_log_error(NGX_LOG_ERR, cf, 0,
                "[check] init main conf: main_conf is null");
        return NGX_CONF_ERROR;
    }
    uscfp = umcf->upstreams.elts;
    id = 0;

    for(i = 0; i < umcf->upstreams.nelts; i++) {
        if (ngx_http_upstream_health_check_init_peers(cf, uscfp[i]) != NGX_OK) {
            return NGX_CONF_ERROR;
        }
    }

    ngx_log_debug0(NGX_LOG_ERR, cf->log, 0,
            "[check] init main conf: init success");

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
        if ((ucscfp[i].conf)->upstream_name == &uscf->host) {
            ucscf = &ucscfp[i];
            break;
        }
    }

    if (ucscf != NULL && uscf->servers) {

        server = uscf->servers->elts;

        for (i = 0; i < uscf->servers->nelts; i++) {

            if (server[i].backup) {
                //todo add backup server
                continue;
            }

            for (j = 0; j < server[i].naddrs; j++) {
                peer = ngx_array_push(&ucscf->peers);

                if (peer == NULL) {
                    ngx_conf_log_error(NGX_LOG_ERR, cf, 0,
                            "[check] init peer: alloc new peer fail, %V", &(server[i].addrs[j].name));
                    return NGX_ERROR;
                }

                peer->index = id;
                peer->state = 0;
                peer->check_state = NGX_CHECK;

                peer->name = server[i].addrs[j].name;
                peer->sockaddr = server[i].addrs[j].sockaddr;
                peer->socklen = server[i].addrs[j].socklen;

                peer->conf = ucscf->conf;

                ngx_memzero(&peer->status, sizeof(ngx_http_status_t));

                peer->pool = ngx_create_pool(ngx_pagesize, cf->log);
                if (peer->pool == NULL) {
                    ngx_conf_log_error(NGX_LOG_ERR, cf, 0,
                            "[check] init peer: alloc peer pool fail, %V", &peer->name);
                    return NGX_ERROR;
                }

                ngx_log_debug3(NGX_LOG_DEBUG_HTTP, cf->log, 0,
                        "[check] init peer: peer succ upstream_name=%V, name=%V, id=%i", 
                        peer->conf->upstream_name, &peer->name, id);

                id++;
            }

        }

        ngx_log_debug1(NGX_LOG_DEBUG_HTTP, cf->log, 0,
                        "[check] init peer: srv succ upstream_name=%V", &uscf->host);
        return NGX_OK;

    } else {
        ngx_conf_log_error(NGX_LOG_ERR, cf, 0,
                        "[check] init peer: srv fail upstream_name=%V", &uscf->host);
        return NGX_ERROR;
    }
}

static ngx_int_t
ngx_http_upstream_health_check_init_shm(ngx_shm_zone_t *shm_zone, void *data)
{
    size_t                                      len;
    u_char                                     *file;
    ngx_array_t                                *ucscfp, *peersp;
    ngx_http_upstream_health_check_peer_t      *peers, *peer;
    ngx_http_upstream_health_check_main_conf_t *ucmcf;
    ngx_http_upstream_health_check_srv_conf_t  *ucscf;
    ngx_http_upstream_check_peer_shm_t         *peer_shm, *prev_shm;
    ngx_http_upstream_check_peers_shm_t        *peers_shm;
    ngx_slab_pool_t                            *shpool;
    ngx_uint_t                                  i, j, same;

    shpool = (ngx_slab_pool_t *)shm_zone->shm.addr;

    ucmcf = shm_zone->data;
    ucscfp = &ucmcf->upstreams;
    ucscf = ucscfp->elts;

    if (!ucscfp->nelts) {
        return NGX_OK;
    }

    peers_shm = ngx_slab_alloc(shpool, sizeof(ngx_http_upstream_check_peers_shm_t));
    if (peers_shm == NULL) {
        return NGX_ERROR;
    }

    peers_shm->head = ngx_slab_alloc(shpool, sizeof(ngx_http_upstream_check_peer_shm_t));
    if (peers_shm->head == NULL) {
        return NGX_ERROR;
    }
    peers_shm->head->next = NULL;
    peers_shm->peer_sum = 0;
    prev_shm = peers_shm->head;

    for (i = 0; i < ucscfp->nelts; i++) {

        peersp = &ucscf[i].peers;
        if (peersp->nelts == 0) {
            continue;
        }
        peers = peersp->elts;

        for (j = 0; j < peersp->nelts; j++) {
            peer = &peers[j];
            peer_shm = peers_shm->head->next;
            // check exists peer
            same = 0;
            while(peer_shm) {
                if (ngx_strncmp(peer->name.data, peer_shm->name->data, peer_shm->name->len) == 0) {
                    same = 1;
                    break;
                }
                peer_shm = peer_shm->next;
            }
            if (!same) {
                peer_shm = ngx_slab_alloc(shpool, sizeof(ngx_http_upstream_check_peer_shm_t));
                if (peer_shm == NULL) {
                    return NGX_ERROR;
                }

                peer_shm->index = peer->index;
                peer_shm->wid = -1;
                peer_shm->rr_peer = NULL;

                peer_shm->socklen = peer->socklen;
                peer_shm->sockaddr = ngx_slab_alloc(shpool, peer_shm->socklen);
                if (peer_shm->sockaddr == NULL) {
                    return NGX_ERROR;
                }
                ngx_memcpy(peer_shm->sockaddr, peer->sockaddr,
                        peer_shm->socklen);

                peer_shm->name = ngx_slab_alloc(shpool, sizeof(ngx_str_t));
                if (peer_shm->name == NULL) {
                    return NGX_ERROR;
                }
                peer_shm->name->len = peer->name.len;
                peer_shm->name->data = ngx_slab_alloc(shpool, peer_shm->name->len);
                if (peer_shm->name->data == NULL) {
                    return NGX_ERROR;
                }
                ngx_memcpy(peer_shm->name->data, peer->name.data, peer_shm->name->len);

                peer_shm->fail = 0;
                peer_shm->rise = 0;

                peer_shm->next = NULL;
                prev_shm->next = peer_shm;
                prev_shm = peer_shm;

                peers_shm->peer_sum += 1;
            }
            peer->shm = peer_shm;
            peer->shpool = shpool;
        }
    }
    
    ucmcf->peers_shm = peers_shm;
    shpool->data = peers_shm;
    shm_zone->data = peers_shm;

    len = sizeof("[check] in peer shm \"\"") + shm_zone->shm.name.len;
    shpool->log_ctx = ngx_slab_alloc(shpool, len);
    if (shpool->log_ctx == NULL) {
        return NGX_ERROR;
    }
    ngx_sprintf(shpool->log_ctx, "[check] in peer shm \"%V\"%Z", &shm_zone->shm.name);

#if (NGX_HAVE_ATOMIC_OPS)
    file = NULL;
#else
    file = ngx_pnalloc(ngx_cycle->pool, ngx_cycle->lock_file.len + shm_zone->shm.name.len);
    if (file == NULL) {
        return NGX_ERROR;
    }

    (void) ngx_sprintf(file, "%V%V%Z", &ngx_cycle->lock_file, &shm_zone->shm.name);
#endif

    if (ngx_shmtx_create(&shpool->mutex, &shpool->lock, file) != NGX_OK) {
        return NGX_ERROR;
    }

    return NGX_OK;
}

static char *
ngx_http_upstream_health_check(ngx_conf_t *cf, ngx_command_t *cmd, void *conf)
{
    ngx_http_upstream_health_check_srv_conf_t  *ucscf;
    ngx_http_upstream_health_check_main_conf_t *ucmcf;
    ngx_http_upstream_health_check_conf_t      *check_conf;
    ngx_int_t                                   rv;
    ngx_array_t                                *confs, *upstreams; 

    ucmcf = ngx_http_conf_get_module_main_conf(cf, ngx_http_upstream_health_check_module);
    if (ucmcf == NULL) {
        ngx_conf_log_error(NGX_LOG_ERR, cf, 0,
                "[check] health check conf: main conf is null");
        return NGX_CONF_ERROR;
    }

    confs = &ucmcf->confs;
    upstreams = &ucmcf->upstreams;

    ucscf = ngx_array_push(upstreams);
    if (ucscf == NULL) {
        ngx_conf_log_error(NGX_LOG_ERR, cf, 0,
                "[check] health check conf: alloc srv conf fail");
        return NGX_CONF_ERROR;
    }

    check_conf = ngx_http_check_get_conf(cf, confs);
    if (check_conf == NULL) {
        return NGX_CONF_ERROR;
    }
    ucscf->conf = check_conf;

    rv = ngx_http_upstream_health_check_add_srv_conf(cf, ucscf);

    if (rv == NGX_ERROR) {
        return NGX_CONF_ERROR;
    }
    
    ngx_log_debug0(NGX_LOG_DEBUG_HTTP, cf->log, 0,
            "[check] health check conf: succ");

    return NGX_CONF_OK;
}

static ngx_http_upstream_health_check_conf_t *
ngx_http_check_get_conf(ngx_conf_t *cf, ngx_array_t *confs)
{
    ngx_uint_t                             i;
    ngx_str_t                             *upstream_name;
    ngx_http_upstream_health_check_conf_t *confp, *conf;
    ngx_http_upstream_srv_conf_t          *uscf;

    confp = confs->elts;
    conf = NULL;

    uscf = ngx_http_conf_get_module_srv_conf(cf, ngx_http_upstream_module);
    upstream_name = &uscf->host;

    for (i = 0; i < confs->nelts; i++) {
        conf = &confp[i];
        if (upstream_name == conf->upstream_name) {
            break;
        }
    }

    ngx_log_debug1(NGX_LOG_DEBUG_HTTP, cf->log, 0,
            "[check] get conf: upstream=%V", upstream_name);

    if (conf == NULL) {
        conf = ngx_array_push(confs);
        if (conf == NULL) {
            ngx_conf_log_error(NGX_LOG_ERR, cf, 0,
                    "[check] get conf: alloc conf fail");
            return NULL;
        }
        conf->upstream_name = upstream_name;
        conf->interval = NGX_CONF_UNSET_MSEC;
        conf->timeout = NGX_CONF_UNSET_MSEC;
        conf->fail_threshold = NGX_CONF_UNSET_UINT;
        conf->rise_threshold = NGX_CONF_UNSET_UINT;
        conf->alive_http_status = NGX_CONF_UNSET_UINT;
        ngx_memzero(&conf->type, sizeof(ngx_peer_check_type));

        ngx_log_debug1(NGX_LOG_DEBUG_HTTP, cf->log, 0,
                "[check] get conf: init conf succ, %V", upstream_name);
    }

    return conf;
}

static char *
ngx_http_check_set_http_send(ngx_conf_t *cf, ngx_command_t *cmd, void *conf)
{
    ngx_http_upstream_health_check_main_conf_t *ucmcf;
    ngx_array_t                                *confs;
    ngx_http_upstream_health_check_conf_t      *check_conf;
    ngx_str_t                                  *value;

    ucmcf = ngx_http_conf_get_module_main_conf(cf, ngx_http_upstream_health_check_module);
    if (ucmcf == NULL) {
        ngx_conf_log_error(NGX_LOG_ERR, cf, 0, 
                "[check] set http_send conf: main conf is null");
 
        return NGX_CONF_ERROR;
    }
    confs = &ucmcf->confs;

    check_conf = ngx_http_check_get_conf(cf, confs);

    if (check_conf == NULL) {
        return NGX_CONF_ERROR;
    }

    value = cf->args->elts;

    ngx_log_debug1(NGX_LOG_DEBUG_HTTP, cf->log, 0,
            "[check] set http_send conf: http_send args num %i", cf->args->nelts);
 
    if (value[1].data == NULL && value[1].len == 0) {
        return NGX_CONF_ERROR;
    }

    check_conf->type.http_send = value[1];

    ngx_log_debug0(NGX_LOG_DEBUG_HTTP, cf->log, 0, "[check] set http_send conf: succ");

    return NGX_CONF_OK;
}

static char *
ngx_http_check_set_alive_http_status(ngx_conf_t *cf, ngx_command_t *cmd, void *conf)
{
    ngx_http_upstream_health_check_main_conf_t *ucmcf;
    ngx_array_t                                *confs;
    ngx_http_upstream_health_check_conf_t      *check_conf;
    ngx_str_t                                  *value;
    ngx_uint_t                                  i, j, status;
    ngx_conf_bitmask_t                          *mask;

    mask = ngx_peer_check_http_statuses;
    status = 0;

    ucmcf = ngx_http_conf_get_module_main_conf(cf, ngx_http_upstream_health_check_module);
    if (ucmcf == NULL) {
        ngx_conf_log_error(NGX_LOG_ERR, cf, 0, 
                "[check] set alive_http_status conf: main conf is null");
        return NGX_CONF_ERROR;
    }
 
    confs = &ucmcf->confs;
    check_conf = ngx_http_check_get_conf(cf, confs);
    if (check_conf == NULL) {
        return NGX_CONF_ERROR;
    }

    value = cf->args->elts;

    ngx_log_debug1(NGX_LOG_DEBUG_HTTP, cf->log, 0,
            "[check] set alive_http_status conf: http_send args num %i", cf->args->nelts);

    for (i = 1; i < cf->args->nelts; i++) {
        for (j = 0; mask[j].name.len != 0; j++) {

            if (mask[j].name.len != value[i].len
                    || ngx_strcasecmp(mask[j].name.data, value[i].data) != 0 ) {
                continue;
            }

            if (mask[j].mask & status) {
                ngx_conf_log_error(NGX_LOG_ERR, cf, 0, 
                        "[check] set alive_http_status conf: duplicate value \"%s\"", value[i].data);
                return NGX_CONF_ERROR;
            } else {
                status |= mask[j].mask;
            }
        }
    }

    if (!status) {
        ngx_conf_log_error(NGX_LOG_ERR, cf, 0, 
                "[check] set alive_http_status conf: invalid status %i", status);
        return NGX_CONF_ERROR;
    }

    check_conf->alive_http_status = status;

    ngx_log_debug0(NGX_LOG_DEBUG_HTTP, cf->log, 0, "[check] set alive_http_status conf: succ");

    return NGX_CONF_OK;
}

static ngx_int_t
ngx_http_upstream_health_check_add_srv_conf(ngx_conf_t *cf, ngx_http_upstream_health_check_srv_conf_t *ucscf) 
{
    ngx_str_t                             *value, str;
    ngx_uint_t                             i;
    ngx_msec_t                             interval, timeout;
    ngx_uint_t                             rise, fail;
    ngx_http_upstream_health_check_conf_t *conf;
    ngx_peer_check_type                   *type;

    if (ucscf == NULL) {
        return NGX_ERROR;
    }

    conf = ucscf->conf;
    value = cf->args->elts;

    ngx_log_debug1(NGX_LOG_DEBUG_HTTP, cf->log, 0,
            "[check] add srv conf: health check args num %i", cf->args->nelts);
    
    for (i = 1; i < cf->args->nelts; i++) {

        if (ngx_strncmp(value[i].data, "type=", 5) == 0) {
            str.len = value[i].len - 5;
            str.data = value[i].data + 5;

            if ((type = ngx_http_get_peer_check_type(&str)) != NULL) { 
                ngx_memcpy(&conf->type, type, sizeof(ngx_peer_check_type));
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
                conf->rise_threshold = rise;
            }

            continue;
        }

        if (ngx_strncmp(value[i].data, "fail=", 5) == 0) {
            str.len = value[i].len - 5;
            str.data = value[i].data + 5;

            fail = ngx_atoi(str.data, str.len);
            if (fail != (ngx_uint_t)NGX_ERROR && fail != 0) {
                conf->fail_threshold = fail;
            }
            continue;
        }
    }

    if (&conf->type == NULL) {
        ngx_memcpy(&conf->type, ngx_peer_check_types, sizeof(ngx_peer_check_type));
    }

    conf->interval = (conf->interval != NGX_CONF_UNSET_MSEC) 
                        ? conf->interval : 5000;
    conf->timeout = (conf->timeout != NGX_CONF_UNSET_MSEC) 
                        ? conf->timeout : 1000;
    conf->rise_threshold = (conf->rise_threshold != NGX_CONF_UNSET_UINT) 
                        ? conf->rise_threshold : 1;
    conf->fail_threshold = (conf->fail_threshold != NGX_CONF_UNSET_UINT) 
                        ? conf->fail_threshold : 2;
    conf->alive_http_status = (conf->alive_http_status != NGX_CONF_UNSET_UINT) 
                        ? conf->alive_http_status : (NGX_CHECK_HTTP_STATUS_2XX|NGX_CHECK_HTTP_STATUS_3XX);

    ngx_log_debug6(NGX_LOG_DEBUG_HTTP, cf->log, 0,
            "[check] add srv conf: type=%V, interval=%M, timeout=%M, rise=%i, fail=%i, status=%i",
            &conf->type.name, conf->interval, conf->timeout, 
            conf->rise_threshold, conf->fail_threshold, conf->alive_http_status);

#if (NGX_DEBUG)

    if (ngx_array_init(&ucscf->peers, cf->pool, 1, 
                        sizeof(ngx_http_upstream_health_check_peer_t)) != NGX_OK) {
        ngx_conf_log_error(NGX_LOG_ERR, cf, 0,
                "[check] add srv conf: init peer arr fail");
        return NGX_ERROR;
    }
    
#else

    if (ngx_array_init(&ucscf->peers, cf->pool, 1024, 
                        sizeof(ngx_http_upstream_health_check_peer_t)) != NGX_OK) {
        ngx_conf_log_error(NGX_LOG_ERR, cf, 0,
                "[check] add srv conf: init peer arr fail");
        return NGX_ERROR;
    }

#endif
    ngx_log_debug0(NGX_LOG_DEBUG_HTTP, cf->log, 0,
            "[check] add srv conf: add srv conf succ");
 
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

    ngx_log_debug1(NGX_LOG_DEBUG_HTTP, cycle->log, 0,
        "[check] init process: begin, %P", ngx_pid);

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
    ngx_int_t                                   worker_processes;
    
    ucmcf = ngx_http_cycle_get_module_main_conf(cycle, ngx_http_upstream_health_check_module);
    umcf = ngx_http_cycle_get_module_main_conf(cycle, ngx_http_upstream_module);
    ccf = (ngx_core_conf_t *) ngx_get_conf(cycle->conf_ctx, ngx_core_module);

    worker_processes = ccf->worker_processes;
    ucscf = ucmcf->upstreams.elts;

    ngx_log_debug3(NGX_LOG_DEBUG_HTTP, cycle->log, 0,
            "[check] init timers: wokers=%i, srvs=%i, pid=%P", 
            worker_processes, ucmcf->upstreams.nelts, ngx_pid);

    for (i = 0; i < ucmcf->upstreams.nelts; i++) {
        peers = (ucscf[i].peers).elts;

        for (j = 0; j < (ucscf[i].peers).nelts; j++) {

            ngx_http_check_peer_lock((&peers[j]));
            
            if (peers[j].shm->wid == -1) {
                peers[j].shm->wid = peers[j].index % worker_processes;
            }
            if (peers[j].shm->rr_peer == NULL) {
                peers[j].shm->rr_peer = get_health_check_rr_peer(&peers[j], umcf);
            }

            if (peers[j].shm->rr_peer == NULL) {
                continue;
            }

            ngx_log_debug6(NGX_LOG_DEBUG_HTTP, cycle->log, 0, 
                    "[check] init timers: peer=%V, rr_peer=%V, pid=%P, slot=%i, wid=%i, wp=%i", 
                    &peers[j].name, &peers[j].shm->rr_peer->name, ngx_pid, 
                    ngx_process_slot, peers[j].shm->wid, worker_processes);

            if (peers[j].shm->wid != (ngx_process_slot % worker_processes)) {
                ngx_http_check_peer_unlock((&peers[j]));
                continue;
            }

            ngx_http_check_peer_unlock((&peers[j]));

            ngx_http_upstream_health_check_add_timers(&peers[j], ucscf[i].conf->interval, cycle->log);
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
    peer->log = log;

    peer->check_ev.handler = ngx_http_upstream_health_check_init_event_handler;
    peer->check_ev.data = peer;
    peer->check_ev.log = log;
    peer->check_ev.timer_set = 0;

    peer->check_timeout_ev.handler = ngx_http_upstream_health_check_timeout_handler;
    peer->check_timeout_ev.data = peer;
    peer->check_timeout_ev.log = log;
    peer->check_timeout_ev.timer_set = 0;

    ngx_add_timer(&peer->check_ev, interval);

    ngx_log_debug3(NGX_LOG_DEBUG_HTTP, log, 0,
            "[check] add timer: peer=%V, interval=%M, current_time=%M",
            &peer->name, interval, ngx_current_msec);

    return NGX_OK;
}

static void
ngx_http_upstream_health_check_init_event_handler(ngx_event_t *ev)
{
    if (ngx_http_upstream_health_check_need_exit(ev)) {
        return ;
    }

    ngx_http_upstream_health_check_connect_handler(ev);
}

static void
ngx_http_upstream_health_check_timeout_handler(ngx_event_t *ev)
{
    ngx_log_error(NGX_LOG_ERR, ev->log, 0,"[check] timeout: happen");

    if (ngx_http_upstream_health_check_need_exit(ev)) {
        return;
    }

    ngx_http_upstream_health_check_update_status(ev->data, 0);
    ngx_http_upstream_health_check_finish_handler(ev->data, 1);
}

static void
ngx_http_upstream_health_check_connect_handler(ngx_event_t *ev)
{
    ngx_http_upstream_health_check_peer_t *peer;
    ngx_connection_t                      *c;
    ngx_peer_connection_t                 *pc;
    ngx_log_t                             *log;
    ngx_int_t                              rc;

    if (ngx_http_upstream_health_check_need_exit(ev)) {
        return;
    }

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
            ngx_http_upstream_health_check_finish_handler(peer, 0);
        } else {
            goto connect_done;
        }
    }

    rc = ngx_event_connect_peer(pc);

    if (rc == NGX_ERROR || rc == NGX_DECLINED) {
        ngx_http_upstream_health_check_update_status(peer, 0);
        ngx_http_upstream_health_check_finish_handler(peer, 1);
        return ;
    }

    /* rc == NGX_OK || rc == NGX_AGAIN */
    c = pc->connection;

    c->data = peer;

    c->log = peer->pc.log;
    c->write->log = c->log;
    c->read->log = c->log;

    c->write->handler = peer->conf->type.send_handler;
    c->write->data = peer;
    c->read->handler = peer->conf->type.recv_handler;
    c->read->data = peer;

connect_done:
    peer->check_state = NGX_CHECK_CONNECT_DONE;
    ngx_add_timer(&peer->check_timeout_ev, peer->conf->timeout);
}

static ngx_int_t
ngx_http_upstream_health_check_connect_alive(ngx_connection_t *c) {
    char      buf[1];
    ngx_err_t err;
    ngx_int_t n;

    n = recv(c->fd, buf, 1, MSG_PEEK);

    err = ngx_socket_errno;

    ngx_log_debug3(NGX_LOG_DEBUG_HTTP, c->log, 0, "[check] check conection: %P, %z, %z", ngx_pid, n, err);

    if (n == 1 || (n == -1 && err == NGX_EAGAIN)) {
        ngx_log_debug1(NGX_LOG_DEBUG_HTTP, c->log, 0, "[check] check conection: %P alive", ngx_pid);
        return NGX_OK;
    } else {
        ngx_log_debug1(NGX_LOG_DEBUG_HTTP, c->log, 0, "[check] check conection: %P death", ngx_pid);
        return NGX_ERROR;
    }

}

static void
ngx_http_upstream_health_check_peek_handler(ngx_event_t *ev)
{
    ngx_http_upstream_health_check_peer_t *peer;
    ngx_connection_t                      *c;

    peer = ev->data;
    c = peer->pc.connection;

    ngx_log_debug2(NGX_LOG_DEBUG_HTTP, ev->log, 0, 
            "[check] peek handler: name=%V, pid=%P", &peer->name, ngx_pid);

    if (ngx_http_upstream_health_check_need_exit(ev)) {
        return;
    }

    if (ngx_http_upstream_health_check_connect_alive(c) == NGX_OK) {
        ngx_http_upstream_health_check_update_status(peer, 1);
    } else {
        ngx_http_upstream_health_check_update_status(peer, 0);
    }

    ngx_http_upstream_health_check_finish_handler(peer, 1);
}

static void
ngx_http_upstream_health_check_send_handler(ngx_event_t *ev)
{
    ngx_http_upstream_health_check_peer_t *peer;
    ngx_connection_t *c;
    ngx_int_t         rc;
    
    if (ngx_http_upstream_health_check_need_exit(ev)) {
        return;
    }
    
    peer = ev->data;
    c = peer->pc.connection;

    if (c->write->timedout) {
        ngx_log_error(NGX_LOG_ERR, c->log, 0, "[check] http send: write timeout");
        goto send_fail;
    }

    ngx_log_debug0(NGX_LOG_DEBUG_HTTP, c->log, 0, "[check] http send: begin");

    if (ngx_http_upstream_health_check_connect_alive(c) != NGX_OK 
            && peer->check_state != NGX_CHECK_CONNECT_DONE) {
        goto send_fail;
    }

    rc = ngx_http_upstream_health_check_send_request(c);

    ngx_log_debug1(NGX_LOG_DEBUG_HTTP, c->log, 0, "[check] http send: write rc %z", rc);

    if (rc == NGX_ERROR) {
        goto send_fail;
    }

    if (rc == NGX_AGAIN) {
        if (ngx_handle_write_event(c->write, 0) != NGX_OK) {
            goto send_fail;
        }
    }

    /* rc == NGX_OK */
    peer->check_state = NGX_CHECK_SEND_DONE;
    if (c->read->ready) {
        ngx_log_debug0(NGX_LOG_DEBUG_HTTP, c->log, 0, "[check] http send: send done, begin read");
        c->read->handler(ev);
    }

    ngx_log_debug0(NGX_LOG_DEBUG_HTTP, c->log, 0, "[check] http send: done");

    return;

send_fail:
    ngx_log_error(NGX_LOG_ERR, peer->log, 0, "[check] http send: error");

    ngx_http_upstream_health_check_update_status(peer, 0);
    ngx_http_upstream_health_check_finish_handler(peer, 1);

    return ;
}

static ngx_int_t
ngx_http_upstream_health_check_send_request(ngx_connection_t *c)
{
    ngx_http_upstream_health_check_peer_t *peer;
    ngx_buf_t                             *b;
    ssize_t                                size;
    ngx_int_t                              rc;

    peer = c->data;

    if (peer->send_data == NULL) {
        rc = ngx_http_upstream_health_check_init_send_data_buf(peer);
        if (rc == NGX_ERROR) {
            return NGX_ERROR;
        }
    }

    b = peer->send_data;

    ngx_log_debug2(NGX_LOG_DEBUG_HTTP, peer->log, 0, 
            "[check] send req: begin send \"%*s\"", b->last - b->pos, b->pos);

    while (b->pos < b->last) {
        size = c->send(c, b->pos, b->last - b->pos);
        
        ngx_log_debug2(NGX_LOG_DEBUG_HTTP, peer->log, 0,
                "[check] send req: size=%z, err=%d", size, ngx_socket_errno);

        if (size > 0) {
            if (size == (b->last - b->pos)) {
                return NGX_OK;
            }
            b->pos += size;
            continue;
        }

        if (size == 0 || size == NGX_AGAIN) {
            return NGX_AGAIN;
        }

        if (size == NGX_ERROR) {
            ngx_log_error(NGX_LOG_ERR, peer->log, 0,
                "[check] send req: send fail err=%d", ngx_socket_errno);
            return NGX_ERROR;
        }
    }

    return NGX_OK;
}

static ngx_int_t
ngx_http_upstream_health_check_init_send_data_buf(ngx_http_upstream_health_check_peer_t *peer)
{
    ngx_str_t *http_send;

    if (peer->pool == NULL) {
        return NGX_ERROR;
    }
    
    http_send = &peer->conf->type.http_send;

    if (http_send->len > 0 && http_send->data != NULL) {

        peer->send_data = ngx_create_temp_buf(peer->pool, http_send->len);
        if (peer->send_data == NULL) {
            ngx_log_error(NGX_LOG_ERR, peer->log, 0,
                    "[check] init send data: alloc fail");
            return NGX_ERROR;
        }

        peer->send_data->last = peer->send_data->pos + http_send->len;
        ngx_memcpy(peer->send_data->start, http_send->data, http_send->len);

        ngx_log_debug0(NGX_LOG_DEBUG_HTTP, peer->log, 0, "[check] init send data: succ");
    }

    return NGX_OK;
}

static void
ngx_http_upstream_health_check_recv_handler(ngx_event_t *ev)
{
    ngx_http_upstream_health_check_peer_t *peer;
    ngx_connection_t *c;
    ngx_int_t         rc;
    ngx_uint_t        code, status;

    peer = ev->data;
    c = peer->pc.connection;

    if (c->read->timedout) {
        goto recv_fail;
    }

    if (ngx_http_upstream_health_check_connect_alive(c) != NGX_OK
            && peer->check_state != NGX_CHECK_SEND_DONE) {
        goto recv_fail;
    }

    rc = ngx_http_upstream_health_check_process_response(c);
    
    if (rc == NGX_AGAIN) {
        ngx_log_debug0(NGX_LOG_DEBUG_HTTP, peer->log, 0, "[check] recv res: need recv again");

        if (ngx_handle_read_event(c->read, 0) != NGX_OK) {
            goto recv_fail;
        }
        return;
    }

    if (rc == NGX_ERROR) {
        goto recv_fail;
    }

    if (rc == NGX_OK) {

        ngx_log_debug0(NGX_LOG_DEBUG_HTTP, peer->log, 0, "[check] recv res: done");

        code = peer->status.code;

        if (code >= 200 && code <= 299) {
            status = NGX_CHECK_HTTP_STATUS_2XX;
        } else if (code >= 300 && code <= 399) {
            status = NGX_CHECK_HTTP_STATUS_3XX;
        } else if (code >= 400 && code <= 499) {
            status = NGX_CHECK_HTTP_STATUS_4XX;
        } else if (code >= 500 && code <= 599) {
            status = NGX_CHECK_HTTP_STATUS_5XX;
        } else {
            status = 0;
        }

        if (status) {
            if (status & peer->conf->alive_http_status) {
                ngx_http_upstream_health_check_update_status(peer, 1);
            } else {
                ngx_http_upstream_health_check_update_status(peer, 0);
            }
        } else {
            ngx_http_upstream_health_check_update_status(peer, 0);
        }
       
        ngx_log_debug6(NGX_LOG_DEBUG_HTTP, c->log, 0, 
                "[check] http recv: parse status line rc=%z, version=%z, status=%z, rise=%z, fail=%z, pid=%P"
                , rc, peer->status.http_version, peer->status.code, peer->shm->rise, peer->shm->fail, ngx_pid);
 
        peer->check_state = NGX_CHECK_RECV_DONE;

        ngx_http_upstream_health_check_finish_handler(peer, 1);
    }

    return;

recv_fail:
    ngx_log_error(NGX_LOG_ERR, peer->log, 0, "[check] http recv: fail");

    ngx_http_upstream_health_check_update_status(peer, 0);
    ngx_http_upstream_health_check_finish_handler(peer, 1);

    return;
}

static void ngx_http_upstream_health_check_update_status(ngx_http_upstream_health_check_peer_t *peer, ngx_int_t rc) {

    ngx_http_upstream_rr_peer_t           *rr_peer;
    ngx_http_upstream_health_check_conf_t *conf;

    conf = peer->conf;

    ngx_http_check_peer_lock(peer);

    if (rc == 1) {
         peer->shm->rise += 1;
    }  else {
         peer->shm->fail += 1;
    } 

    ngx_log_debug4(NGX_LOG_DEBUG_HTTP, peer->log, 0, 
            "[check] update status : name=%V, rise=%z, fail=%z, pid=%P"
            , &peer->name, peer->shm->rise, peer->shm->fail, ngx_pid);
      
    rr_peer = peer->shm->rr_peer;

    if (peer->shm->rise == conf->rise_threshold) {

        peer->shm->fail = 0;

        if (rr_peer->down == 1) {
            rr_peer->down = 0;
            ngx_log_error(NGX_LOG_NOTICE, peer->log, 0, 
                    "[check] update status: peer up name=%V, pid=%P", &peer->name, ngx_pid);
        }

    }  else if (peer->shm->fail == conf->fail_threshold) {
        
        peer->shm->rise = 0;

        if (rr_peer->down == 0) {
            rr_peer->down = 1;
            ngx_log_error(NGX_LOG_WARN, peer->log, 0, 
                    "[check] update status: peer down name=%V, pid=%P", &peer->name, ngx_pid);
        }

    }

    ngx_http_check_peer_unlock(peer);
}

static ngx_int_t ngx_http_upstream_health_check_process_response(ngx_connection_t *c)
{
    ngx_http_upstream_health_check_peer_t *peer;
    ssize_t                                size;
    ngx_buf_t                             *b;
    ngx_int_t                              rc;

    peer = c->data;

    if (peer->recv_data == NULL) {
        rc = ngx_http_upstream_health_check_init_recv_data_buf(peer);
        if (rc == NGX_ERROR) {
            return NGX_ERROR;
        }
    }

    b = peer->recv_data;

    while(1) {
        size = c->recv(c, b->last, b->end - b->last);

        ngx_log_debug2(NGX_LOG_DEBUG_HTTP, peer->log, 0,
                "[check] recv res: size=%z, err=%d", size, ngx_socket_errno);

        if (size > 0) {
            b->last += size;
            rc = ngx_http_upstream_health_check_parse_status_line(peer, b);
            if (rc == NGX_AGAIN) {
                continue;
            }
            if (rc == NGX_ERROR || rc == NGX_OK) {
                return rc;
            }
        }

        if (size == NGX_AGAIN || size == 0) {
            return NGX_AGAIN;
        }

        if (size == NGX_ERROR) {
            return NGX_ERROR;
        }

    }
}

static ngx_int_t
ngx_http_upstream_health_check_init_recv_data_buf(ngx_http_upstream_health_check_peer_t *peer)
{
    if (peer->pool == NULL) {
        return NGX_ERROR;
    }

    peer->recv_data = ngx_create_temp_buf(peer->pool, ngx_pagesize);

    if (peer->recv_data == NULL) {
        ngx_log_error(NGX_LOG_ERR, peer->log, 0, "[check] init recv data: fail");
        return NGX_ERROR;
    }

    return NGX_OK;
}

static void
ngx_http_upstream_health_check_finish_handler(ngx_http_upstream_health_check_peer_t *peer, 
        ngx_int_t need_reinit_ev) 
{
    ngx_connection_t *c;
    ngx_buf_t        *send_buf, *recv_buf;

    ngx_log_debug2(NGX_LOG_DEBUG_HTTP, peer->log, 0,
            "[check] finish handler: name=%V, pid=%P", &peer->name, ngx_pid);

    c = peer->pc.connection;
    recv_buf = peer->recv_data;
    send_buf = peer->send_data;

    if (peer->check_timeout_ev.timer_set) {
        ngx_del_timer(&peer->check_timeout_ev);
    }

    if (peer->check_ev.timer_set) {
        ngx_del_timer(&peer->check_ev);
    }
 
    if (c != NULL) {
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

    peer->check_state = NGX_CHECK;

    if (need_reinit_ev) {
        ngx_log_debug2(NGX_LOG_DEBUG_HTTP, peer->log, 0,
            "[check] finish handler: reinit check_ev, name=%V, pid=%P", &peer->name, ngx_pid);
        ngx_add_timer(&peer->check_ev, peer->conf->interval);
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
    
    ngx_log_debug3(NGX_LOG_DEBUG_HTTP, peer->log, 0,
            "[check] parse status line: begin parse, state=%i, str=\"%*s\"", 
            state, b->last - b->pos, b->pos);

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

    ngx_log_debug1(NGX_LOG_DEBUG_HTTP, peer->log, 0,
            "[check] parse status line: again parse, state=%i", state);

    return NGX_AGAIN;

done:
    b->pos = p + 1;
    if (peer->status.end == NULL) {
        peer->status.end = p;
    }

    peer->state = sw_start;

    ngx_log_debug1(NGX_LOG_DEBUG_HTTP, peer->log, 0,
            "[check] parse status line: ok parse, state=%i", state);

    return NGX_OK;
}

static ngx_int_t
ngx_http_upstream_health_check_need_exit(ngx_event_t *ev)
{
    ngx_http_upstream_health_check_peer_t *peer;

    peer = ev->data;
    if (ngx_terminate || ngx_quit || ngx_exiting) {
        ngx_log_error(NGX_LOG_ERR, ev->log, 0,
                "[check] need exit: worker exist, pid=%P", ngx_pid);
        ngx_http_upstream_health_check_clear(peer);
        return NGX_ERROR;
    }

    return NGX_OK;
}

static void
ngx_http_upstream_health_check_clear(ngx_http_upstream_health_check_peer_t *peer)
{
    ngx_log_debug2(NGX_LOG_DEBUG_HTTP, peer->log, 0, 
            "[check] peer clear: name=%V, pid=%P", &peer->name, ngx_pid);

    ngx_connection_t *c;
    
    c = peer->pc.connection;

    if (peer->check_timeout_ev.timer_set) {
        ngx_del_timer(&peer->check_timeout_ev);
    }

    if (peer->check_ev.timer_set) {
        ngx_del_timer(&peer->check_ev);
    }
 
    if (c != NULL) {
        ngx_close_connection(c);
        peer->pc.connection = NULL;
    }

    if (peer->pool != NULL) {
        ngx_destroy_pool(peer->pool);
        peer->pool = NULL;
    }

    ngx_memzero(peer, sizeof(ngx_http_upstream_health_check_peer_t));
}

static ngx_peer_check_type *
ngx_http_get_peer_check_type(ngx_str_t *str)
{
    ngx_uint_t i;
    ngx_str_t  *name;

    for (i = 0; ngx_peer_check_types[i].name.data; i++) {
        name = &ngx_peer_check_types[i].name;
        if (ngx_strncasecmp(str->data, name->data, name->len) == 0) {
            return &ngx_peer_check_types[i];
        }
    }

    return NULL;
}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
