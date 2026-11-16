/* loadc.c: the bench/load.js user model in C, so a few hundred thousand keep-alive connections can
 * run it from one machine. One VU = one keep-alive connection, started on a linear ramp:
 *   iter 0 sleeps U(0,5) s; GET /feed -> think U(3,7) -> GET /posts/:id (random post of the feed)
 *   -> think U(3,8) -> like it (15 %) -> write a post (2 %) -> idle U(5,15) -> repeat.
 * Latency is k6's http_req_duration: from the request's send to its last byte, connect excluded.
 * A status >= 400, a transport error or a 60 s timeout is a failed request (k6's http_req_failed).
 * Usage: loadc host port VUS ramp_s hold_s threads [report_s]   (TOKENS=path to tokens.json)
 * Prints a line every report_s and a k6-style summary at the end (whole run and hold-only). */
#define _GNU_SOURCE
#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

#define HBUCKETS 1000000 /* 0.1 ms buckets up to 100 s */
#define BUFSZ 8192
#define TIMEOUT_NS 60000000000ULL

enum { K_FEED, K_POST, K_LIKE, K_CREATE, K_N };
static const char *kname[K_N] = {"GET /feed", "GET /posts/:id", "POST like", "POST /posts"};

struct buf { int got, need, hdr_end, status; char d[BUFSZ]; };

struct vu {
  int fd;
  uint8_t st;   /* 0 idle/scheduled, 1 connecting, 2 awaiting reply */
  uint8_t step; /* what the next action is: K_FEED, K_POST, K_LIKE, K_CREATE */
  uint8_t kind; /* request in flight */
  uint8_t do_create;
  int32_t hpos; /* position in the thread's heap, -1 if not scheduled */
  uint32_t uid, iter;
  uint8_t ring; /* from the X-Ring response header (diagnostic builds), 255 unknown */
  int64_t post_id;
  uint64_t t0, due, close_at;
  struct buf *b;
};

struct hist { uint32_t *b; uint64_t n, over, sum; uint64_t max; };

struct th {
  int id, ep;
  struct vu *vus;
  int nvu;
  int *heap, hn;
  struct buf **pool; int npool, pool_cap;
  uint64_t rng;
  int down; /* the ramp-down has begun: VUs close at their close_at */
  struct hist total, hold, rdown, win[2], wfirst[2], wlate[2], conn; /* wfirst: first-iteration requests; wlate: the last tenth of the VUs */
  uint64_t kind_n[K_N], kind_fail[K_N], fails, timeouts, conn_fail, conn_ok, reconnects, inflight, open;
  uint64_t fail_status[6];
  pthread_t pt;
};

static int N, T, RAMP, HOLD, DOWN, PORT;
static const char *HOST;
static uint64_t START, HOLD_T0, DOWN_T0, STOP_T;
static _Atomic int win_idx;
static _Atomic int stop_flag;
static char **tokens, **usernames;
static int ntokens;
static struct th *TH;
static int conns_per_ip = 25000;
/* CLOSE_ON_DOWN=1: a ramped-down user closes its connection. Default 0, as k6 does: a ramped-down VU only stops
 * running (internal/js/runner.go never closes its idle keep-alive connections; its transport has no idle timeout). */
static int close_on_down = 0;
static unsigned long cerr[140]; /* connect failures by errno (EPOLLHUP without SO_ERROR counted at 0) */
#define CERR(e) __atomic_fetch_add(&cerr[(e) > 0 && (e) < 140 ? (e) : 0], 1, __ATOMIC_RELAXED)
static uint32_t src_base = 0x7f000002u; /* SRC_BASE: first source address (127.0.0.2 on loopback) */

static uint64_t now_ns(void) {
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
}
static uint64_t xr(uint64_t *s) { uint64_t x = *s; x ^= x << 13; x ^= x >> 7; x ^= x << 17; return *s = x; }
static double ur(uint64_t *s, double lo, double hi) { return lo + (double)(xr(s) >> 11) / 9007199254740992.0 * (hi - lo); }

static void hist_init(struct hist *h) { h->b = calloc(HBUCKETS, sizeof *h->b); h->n = h->over = h->sum = h->max = 0; }
static void hist_add(struct hist *h, uint64_t ns) {
  uint64_t k = ns / 100000;
  if (k >= HBUCKETS) { h->over++; k = HBUCKETS - 1; }
  h->b[k]++; h->n++; h->sum += ns; if (ns > h->max) h->max = ns;
}
static void hist_merge(struct hist *d, struct hist *s) {
  for (uint64_t i = 0; i < HBUCKETS; i++) d->b[i] += s->b[i];
  d->n += s->n; d->over += s->over; d->sum += s->sum; if (s->max > d->max) d->max = s->max;
}
static double hist_pct(struct hist *h, double p) {
  if (!h->n) return 0;
  uint64_t want = (uint64_t)((double)h->n * p), acc = 0;
  if (want >= h->n) want = h->n - 1;
  for (uint64_t i = 0; i < HBUCKETS; i++) { acc += h->b[i]; if (acc > want) return (double)i * 0.1 + 0.05; }
  return (double)h->max / 1e6;
}

/* heap of VUs by due time */
static void hswap(struct th *t, int a, int b) { int x = t->heap[a]; t->heap[a] = t->heap[b]; t->heap[b] = x; t->vus[t->heap[a]].hpos = a; t->vus[t->heap[b]].hpos = b; }
static void hup(struct th *t, int i) { while (i > 0) { int p = (i - 1) / 2; if (t->vus[t->heap[p]].due <= t->vus[t->heap[i]].due) break; hswap(t, p, i); i = p; } }
static void hdown(struct th *t, int i) {
  for (;;) { int l = 2 * i + 1, r = l + 1, m = i;
    if (l < t->hn && t->vus[t->heap[l]].due < t->vus[t->heap[m]].due) m = l;
    if (r < t->hn && t->vus[t->heap[r]].due < t->vus[t->heap[m]].due) m = r;
    if (m == i) break;
    hswap(t, i, m); i = m; }
}
static void schedule(struct th *t, int vi, uint64_t due) {
  struct vu *v = &t->vus[vi];
  if (t->down && due > v->close_at) due = v->close_at; /* wake up to close on time */
  v->due = due; v->st = 0;
  if (v->hpos >= 0) { hup(t, v->hpos); hdown(t, v->hpos); return; }
  t->heap[t->hn] = vi; v->hpos = t->hn; t->hn++; hup(t, v->hpos);
}
static int heap_pop(struct th *t) { int vi = t->heap[0]; t->hn--; if (t->hn > 0) { t->heap[0] = t->heap[t->hn]; t->vus[t->heap[0]].hpos = 0; hdown(t, 0); } t->vus[vi].hpos = -1; return vi; }

static struct buf *buf_get(struct th *t) {
  struct buf *b = t->npool ? t->pool[--t->npool] : malloc(sizeof *b);
  b->got = 0; b->need = -1; b->hdr_end = 0; b->status = 0;
  return b;
}
static void buf_put(struct th *t, struct buf *b) {
  if (t->npool == t->pool_cap) { t->pool_cap = t->pool_cap ? t->pool_cap * 2 : 1024; t->pool = realloc(t->pool, (size_t)t->pool_cap * sizeof *t->pool); }
  t->pool[t->npool++] = b;
}

static void vu_close(struct th *t, struct vu *v) {
  if (v->fd >= 0) { epoll_ctl(t->ep, EPOLL_CTL_DEL, v->fd, NULL); close(v->fd); v->fd = -1; t->open--; }
  if (v->b) { buf_put(t, v->b); v->b = NULL; }
  if (v->st == 2) t->inflight--;
  v->st = 0;
}

/* the k6 loop, continued after the reply to `kind` with `status` */
static void next_action(struct th *t, int vi, int kind, int status) {
  struct vu *v = &t->vus[vi];
  uint64_t now = now_ns();
  if (t->down && now >= v->close_at) { if (close_on_down) vu_close(t, v); v->st = 4; return; } /* this user has left */
  switch (kind) {
  case K_FEED:
    if (status != 200) { v->iter++; v->step = K_FEED; schedule(t, vi, now + (uint64_t)(ur(&t->rng, 3, 7) * 1e9)); return; }
    v->step = K_POST; schedule(t, vi, now + (uint64_t)(ur(&t->rng, 3, 7) * 1e9)); return;
  case K_POST: {
    int like = ur(&t->rng, 0, 1) < 0.15, create = ur(&t->rng, 0, 1) < 0.02;
    v->do_create = (uint8_t)create;
    uint64_t due = now + (uint64_t)(ur(&t->rng, 3, 8) * 1e9);
    if (like) { v->step = K_LIKE; schedule(t, vi, due); return; }
    if (create) { v->step = K_CREATE; schedule(t, vi, due); return; }
    v->iter++; v->step = K_FEED; schedule(t, vi, due + (uint64_t)(ur(&t->rng, 5, 15) * 1e9)); return;
  }
  case K_LIKE:
    if (v->do_create) { v->step = K_CREATE; schedule(t, vi, now); return; }
    /* fallthrough */
  default:
    v->iter++; v->step = K_FEED; schedule(t, vi, now + (uint64_t)(ur(&t->rng, 5, 15) * 1e9)); return;
  }
}

static void send_request(struct th *t, int vi) {
  struct vu *v = &t->vus[vi];
  char req[1024];
  int n;
  const char *tok = tokens[v->uid];
  switch (v->step) {
  case K_FEED: n = snprintf(req, sizeof req, "GET /feed HTTP/1.1\r\nHost: %s\r\nUser-Agent: k6/1.0\r\n\r\n", HOST); break;
  case K_POST: n = snprintf(req, sizeof req, "GET /posts/%lld HTTP/1.1\r\nHost: %s\r\nUser-Agent: k6/1.0\r\n\r\n", (long long)v->post_id, HOST); break;
  case K_LIKE:
    n = snprintf(req, sizeof req, "POST /posts/%lld/like HTTP/1.1\r\nHost: %s\r\nUser-Agent: k6/1.0\r\nAuthorization: Bearer %s\r\nContent-Type: application/json\r\nContent-Length: 0\r\n\r\n", (long long)v->post_id, HOST, tok);
    break;
  default: {
    char body[256];
    int bl = snprintf(body, sizeof body, "{\"body\":\"%s says hi at 2026-11-12T10:00:00.000Z (VU %u, iter %u)\"}", usernames[v->uid], vi * T + t->id + 1, v->iter);
    n = snprintf(req, sizeof req, "POST /posts HTTP/1.1\r\nHost: %s\r\nUser-Agent: k6/1.0\r\nAuthorization: Bearer %s\r\nContent-Type: application/json\r\nContent-Length: %d\r\n\r\n%s", HOST, tok, bl, body);
  }
  }
  v->kind = v->step;
  v->t0 = now_ns();
  ssize_t k = send(v->fd, req, (size_t)n, MSG_NOSIGNAL);
  if (k != n) { t->fails++; t->kind_fail[v->kind]++; t->kind_n[v->kind]++; vu_close(t, v); next_action(t, vi, v->kind, 599); return; }
  v->b = buf_get(t);
  v->st = 2; t->inflight++;
  struct epoll_event ev = {.events = EPOLLIN, .data.u32 = (uint32_t)vi};
  epoll_ctl(t->ep, EPOLL_CTL_MOD, v->fd, &ev);
}

static void vu_connect(struct th *t, int vi) {
  struct vu *v = &t->vus[vi];
  int gi = vi * T + t->id; /* global index: picks the source address */
  v->fd = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0);
  if (v->fd < 0) { CERR(errno); t->conn_fail++; t->fails++; t->kind_n[v->step]++; t->kind_fail[v->step]++; next_action(t, vi, v->step, 599); return; }
  int one = 1;
  setsockopt(v->fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof one);
  struct sockaddr_in src = {.sin_family = AF_INET, .sin_addr.s_addr = htonl(src_base + (uint32_t)(gi / conns_per_ip))};
  setsockopt(v->fd, IPPROTO_IP, IP_BIND_ADDRESS_NO_PORT, &one, sizeof one); /* pick the port at connect: a 4-tuple hash, not a scan */
  bind(v->fd, (struct sockaddr *)&src, sizeof src);
  struct sockaddr_in dst = {.sin_family = AF_INET, .sin_port = htons((uint16_t)PORT)};
  inet_pton(AF_INET, HOST, &dst.sin_addr);
  t->open++;
  if (connect(v->fd, (struct sockaddr *)&dst, sizeof dst) < 0 && errno != EINPROGRESS) {
    CERR(errno); t->conn_fail++; t->fails++; t->kind_n[v->step]++; t->kind_fail[v->step]++; vu_close(t, v); next_action(t, vi, v->step, 599); return;
  }
  v->st = 1; v->t0 = now_ns();
  struct epoll_event ev = {.events = EPOLLOUT | EPOLLIN, .data.u32 = (uint32_t)vi};
  epoll_ctl(t->ep, EPOLL_CTL_ADD, v->fd, &ev);
}

static void act(struct th *t, int vi) {
  struct vu *v = &t->vus[vi];
  if (t->down && now_ns() >= v->close_at) { if (close_on_down) vu_close(t, v); v->st = 4; return; }
  if (v->fd < 0) { if (v->iter || v->st) t->reconnects += v->iter > 0; vu_connect(t, vi); return; }
  send_request(t, vi);
}

/* feed body: ids of the post objects (depth 2, "id": right after a brace or comma) */
static int64_t pick_post(struct buf *b, uint64_t *rng) {
  int64_t ids[64]; int nid = 0, depth = 0, instr = 0;
  const char *p = b->d + b->hdr_end, *e = b->d + (b->got < BUFSZ ? b->got : BUFSZ);
  for (; p < e; p++) {
    char c = *p;
    if (instr) { if (c == '\\') p++; else if (c == '"') instr = 0; continue; }
    if (c == '"') {
      if (depth == 2 && (p[-1] == '{' || p[-1] == ',') && e - p > 6 && memcmp(p, "\"id\":", 5) == 0) { ids[nid] = atoll(p + 5); if (nid < 63) nid++; p += 4; continue; }
      instr = 1;
    } else if (c == '{') depth++;
    else if (c == '}') depth--;
  }
  return nid ? ids[xr(rng) % (uint64_t)nid] : 1;
}

static void on_reply_done(struct th *t, int vi) {
  struct vu *v = &t->vus[vi];
  struct buf *b = v->b;
  uint64_t now = now_ns(), lat = now - v->t0;
  int kind = v->kind, status = b->status;
  t->kind_n[kind]++;
  int ok = kind == K_CREATE ? status == 201 : status == 200 || status == 201;
  if (!ok) { t->fails++; t->kind_fail[kind]++; int s = status / 100; t->fail_status[s < 6 ? s : 5]++; }
  hist_add(&t->total, lat);
  int wi = atomic_load(&win_idx);
  hist_add(&t->win[wi], lat);
  if (v->iter == 0) hist_add(&t->wfirst[wi], lat);
  if (vi >= t->nvu - t->nvu / 10) hist_add(&t->wlate[wi], lat);
  if (now >= DOWN_T0) hist_add(&t->rdown, lat);
  else if (now >= HOLD_T0) hist_add(&t->hold, lat);
  if (kind == K_FEED && status == 200) v->post_id = pick_post(b, &t->rng);
  buf_put(t, b); v->b = NULL;
  v->st = 0; t->inflight--;
  next_action(t, vi, kind, status);
}

static void on_readable(struct th *t, int vi) {
  struct vu *v = &t->vus[vi];
  struct buf *b = v->b;
  char scratch[16384];
  for (;;) {
    int room = BUFSZ - 1 - b->got;
    ssize_t k = room > 0 ? recv(v->fd, b->d + b->got, (size_t)room, 0) : recv(v->fd, scratch, sizeof scratch, 0);
    if (k < 0) { if (errno == EAGAIN) return; goto fail; }
    if (k == 0) goto fail;
    b->got += (int)k;
    if (room > 0) b->d[b->got] = 0;
    if (b->need < 0) {
      char *he = strstr(b->d, "\r\n\r\n");
      if (!he) { if (b->got > BUFSZ - 2) goto fail; continue; }
      b->status = atoi(b->d + 9);
      char *cl = strcasestr(b->d, "content-length:");
      int body = cl && cl < he ? atoi(cl + 15) : 0;
      b->hdr_end = (int)(he + 4 - b->d);
      b->need = b->hdr_end + body;
      { char *xr = strstr(b->d, "X-Ring: "); if (xr && xr < he) v->ring = (uint8_t)(xr[8] - '0'); }
    }
    if (b->got >= b->need) { on_reply_done(t, vi); return; }
  }
fail:
  t->fails++; t->kind_fail[v->kind]++; t->kind_n[v->kind]++; t->fail_status[5]++;
  int kind = v->kind;
  vu_close(t, v);
  next_action(t, vi, kind, 599);
}

static void *thread_main(void *arg) {
  struct th *t = arg;
  struct epoll_event evs[2048];
  int n_in_flight_scan = 0;
  while (!atomic_load(&stop_flag)) {
    uint64_t now = now_ns();
    if (DOWN && !t->down && now >= DOWN_T0) { /* the ramp-down: every user leaves at a random moment of it */
      t->down = 1;
      for (int j = 0; j < t->nvu; j++) {
        struct vu *v = &t->vus[j];
        v->close_at = DOWN_T0 + (uint64_t)(ur(&t->rng, 0, DOWN) * 1e9);
        if (v->st == 0) schedule(t, j, v->due);
      }
    }
    int fired = 0;
    while (t->hn > 0 && t->vus[t->heap[0]].due <= now && fired < 2000) { int vi = heap_pop(t); act(t, vi); fired++; }
    int tmo = 10;
    if (t->hn > 0) { uint64_t d = t->vus[t->heap[0]].due; tmo = d <= now ? 0 : (int)((d - now) / 1000000ULL); if (tmo > 10) tmo = 10; }
    int n = epoll_wait(t->ep, evs, 2048, tmo);
    for (int i = 0; i < n; i++) {
      int vi = (int)evs[i].data.u32;
      struct vu *v = &t->vus[vi];
      if (v->st == 1) {
        int err = 0; socklen_t el = sizeof err;
        getsockopt(v->fd, SOL_SOCKET, SO_ERROR, &err, &el);
        if (err || (evs[i].events & (EPOLLERR | EPOLLHUP))) { CERR(err); t->conn_fail++; t->fails++; t->kind_n[v->step]++; t->kind_fail[v->step]++; int k = v->step; vu_close(t, v); next_action(t, vi, k, 599); continue; }
        t->conn_ok++;
        hist_add(&t->conn, now_ns() - v->t0);
        send_request(t, vi);
      } else if (v->st == 2 && (evs[i].events & (EPOLLIN | EPOLLERR | EPOLLHUP))) {
        on_readable(t, vi);
      }
    }
    /* timeouts: scan a slice of the VUs each loop (whole table every ~2 s) */
    now = now_ns();
    for (int k = 0; k < t->nvu / 200 + 1; k++) {
      int si = n_in_flight_scan;
      struct vu *v = &t->vus[si];
      if ((v->st == 2 || v->st == 1) && now > v->t0 && now - v->t0 > TIMEOUT_NS) {
        int kind = v->st == 2 ? v->kind : v->step;
        t->timeouts++; t->fails++; t->kind_fail[kind]++; t->kind_n[kind]++; t->fail_status[5]++;
        vu_close(t, v); next_action(t, si, kind, 599);
      }
      if (++n_in_flight_scan >= t->nvu) n_in_flight_scan = 0;
    }
  }
  return NULL;
}

static void load_tokens(const char *path) {
  FILE *f = fopen(path, "rb");
  if (!f) { perror(path); exit(2); }
  fseek(f, 0, SEEK_END); long len = ftell(f); fseek(f, 0, SEEK_SET);
  char *d = malloc((size_t)len + 1); if (fread(d, 1, (size_t)len, f) != (size_t)len) exit(2); d[len] = 0; fclose(f);
  int cap = 1024; tokens = malloc(cap * sizeof *tokens); usernames = malloc(cap * sizeof *usernames);
  char *p = d;
  for (;;) {
    char *u = strstr(p, "\"username\":\""); if (!u) break; u += 12; char *ue = strchr(u, '"');
    char *tk = strstr(ue + 1, "\"token\":\""); if (!tk) break; tk += 9; char *te = strchr(tk, '"');
    if (ntokens == cap) { cap *= 2; tokens = realloc(tokens, cap * sizeof *tokens); usernames = realloc(usernames, cap * sizeof *usernames); }
    *ue = 0; *te = 0; usernames[ntokens] = u; tokens[ntokens] = tk; ntokens++;
    p = te + 1;
  }
}

static void report_line(double at, struct hist *w, uint64_t reqs_prev, double dt) {
  uint64_t fails = 0, inflight = 0, open = 0, tmo = 0, cfail = 0, reqs = 0;
  for (int i = 0; i < T; i++) { fails += TH[i].fails; inflight += TH[i].inflight; open += TH[i].open; tmo += TH[i].timeouts; cfail += TH[i].conn_fail; reqs += TH[i].total.n; }
  printf("t=%4.0fs conns=%llu inflight=%llu reqs=%llu req/s=%.0f win: p50=%.1f p95=%.1f p99=%.1f max=%.0f ms | fails=%llu timeouts=%llu conn_fail=%llu\n", at,
         (unsigned long long)open, (unsigned long long)inflight, (unsigned long long)reqs, (double)(reqs - reqs_prev) / dt, hist_pct(w, 0.5), hist_pct(w, 0.95), hist_pct(w, 0.99), (double)w->max / 1e6,
         (unsigned long long)fails, (unsigned long long)tmo, (unsigned long long)cfail);
  fflush(stdout);
}

static void summary(const char *what, struct hist *h, uint64_t fails, double secs) {
  double p95 = hist_pct(h, 0.95), p99 = hist_pct(h, 0.99);
  double fr = h->n + fails ? (double)fails / (double)(h->n + fails) * 100 : 0;
  int pass = p95 < 500 && p99 < 1000 && fr < 1.0;
  printf("%s: %llu requests (%.0f req/s), p50 %.1f ms, p95 %.1f ms, p99 %.1f ms, max %.0f ms, failed %.3f %% -> %s\n", what, (unsigned long long)h->n,
         (double)h->n / secs, hist_pct(h, 0.5), p95, p99, (double)h->max / 1e6, fr, pass ? "PASS" : "FAIL");
}

int main(int argc, char **argv) {
  if (argc < 7) { fprintf(stderr, "usage: loadc host port vus ramp_s hold_s threads [down_s] [report_s]\n"); return 2; }
  HOST = argv[1]; PORT = atoi(argv[2]); N = atoi(argv[3]); RAMP = atoi(argv[4]); HOLD = atoi(argv[5]); T = atoi(argv[6]);
  DOWN = argc > 7 ? atoi(argv[7]) : 0;
  int report = argc > 8 ? atoi(argv[8]) : 10;
  if (getenv("SRC_BASE")) { struct in_addr ia; if (inet_pton(AF_INET, getenv("SRC_BASE"), &ia) == 1) src_base = ntohl(ia.s_addr); }
  if (getenv("CONNS_PER_IP")) conns_per_ip = atoi(getenv("CONNS_PER_IP"));
  if (getenv("CLOSE_ON_DOWN")) close_on_down = atoi(getenv("CLOSE_ON_DOWN"));
  load_tokens(getenv("TOKENS") ? getenv("TOKENS") : "seed/tokens.json");
  if (!ntokens) { fprintf(stderr, "no tokens\n"); return 2; }
  TH = calloc((size_t)T, sizeof *TH);
  START = now_ns(); HOLD_T0 = START + (uint64_t)RAMP * 1000000000ULL; DOWN_T0 = HOLD_T0 + (uint64_t)HOLD * 1000000000ULL;
  STOP_T = DOWN_T0 + (uint64_t)DOWN * 1000000000ULL;
  for (int i = 0; i < T; i++) {
    struct th *t = &TH[i];
    t->id = i; t->ep = epoll_create1(0); t->nvu = N / T + (i < N % T);
    t->vus = calloc((size_t)t->nvu, sizeof *t->vus); t->heap = malloc((size_t)t->nvu * sizeof *t->heap);
    t->rng = 0x9E3779B97F4A7C15ULL * (uint64_t)(i + 1) ^ START;
    hist_init(&t->total); hist_init(&t->hold); hist_init(&t->rdown); hist_init(&t->win[0]); hist_init(&t->win[1]); hist_init(&t->wfirst[0]); hist_init(&t->wfirst[1]); hist_init(&t->wlate[0]); hist_init(&t->wlate[1]); hist_init(&t->conn);
    for (int j = 0; j < t->nvu; j++) {
      struct vu *v = &t->vus[j];
      int gi = j * T + i;
      v->fd = -1; v->hpos = -1; v->uid = (uint32_t)(gi % ntokens); v->step = K_FEED; v->post_id = 1;
      /* VU gi starts at its point of the ramp, then iteration 0 sleeps U(0,5) s */
      uint64_t due = START + (uint64_t)((double)gi / (double)N * (double)RAMP * 1e9) + (uint64_t)(ur(&t->rng, 0, 5) * 1e9);
      schedule(t, j, due);
    }
  }
  for (int i = 0; i < T; i++) pthread_create(&TH[i].pt, NULL, thread_main, &TH[i]);
  struct hist agg, aggf, aggl; hist_init(&agg); hist_init(&aggf); hist_init(&aggl);
  uint64_t last = START, reqs_prev = 0;
  for (;;) {
    usleep(200000);
    uint64_t now = now_ns();
    if (now >= STOP_T) break;
    if (now - last >= (uint64_t)report * 1000000000ULL) {
      int old = atomic_load(&win_idx); atomic_store(&win_idx, !old); usleep(2000);
      memset(agg.b, 0, HBUCKETS * sizeof *agg.b); agg.n = agg.over = agg.sum = agg.max = 0;
      for (int i = 0; i < T; i++) {
        hist_merge(&agg, &TH[i].win[old]); memset(TH[i].win[old].b, 0, HBUCKETS * sizeof *agg.b); TH[i].win[old].n = TH[i].win[old].max = TH[i].win[old].sum = 0;
        hist_merge(&aggf, &TH[i].wfirst[old]); memset(TH[i].wfirst[old].b, 0, HBUCKETS * sizeof *agg.b); TH[i].wfirst[old].n = TH[i].wfirst[old].max = TH[i].wfirst[old].sum = 0;
        hist_merge(&aggl, &TH[i].wlate[old]); memset(TH[i].wlate[old].b, 0, HBUCKETS * sizeof *agg.b); TH[i].wlate[old].n = TH[i].wlate[old].max = TH[i].wlate[old].sum = 0;
      }
      printf("   first-iteration: n=%llu p50=%.1f p99=%.1f | last-tenth VUs: n=%llu p50=%.1f p99=%.1f\n", (unsigned long long)aggf.n, hist_pct(&aggf, 0.5), hist_pct(&aggf, 0.99), (unsigned long long)aggl.n, hist_pct(&aggl, 0.5), hist_pct(&aggl, 0.99));
      { uint64_t stuck[17] = {0}, all[17] = {0}, nw = now_ns();
        for (int i = 0; i < T; i++) for (int j = 0; j < TH[i].nvu; j++) { struct vu *v = &TH[i].vus[j]; int r = v->ring < 16 ? v->ring : 16; all[r]++; if (v->st == 2 && nw - v->t0 > 300000000ULL) stuck[r]++; }
        printf("   stuck>300ms by ring:"); for (int r = 0; r < 17; r++) if (all[r]) printf(" r%d:%llu/%llu", r == 16 ? -1 : r, (unsigned long long)stuck[r], (unsigned long long)all[r]); printf("\n"); }
      memset(aggf.b, 0, HBUCKETS * sizeof *aggf.b); aggf.n = aggf.max = aggf.sum = 0; memset(aggl.b, 0, HBUCKETS * sizeof *aggl.b); aggl.n = aggl.max = aggl.sum = 0;
      report_line((double)(now - START) / 1e9, &agg, reqs_prev, (double)(now - last) / 1e9);
      reqs_prev = 0; for (int i = 0; i < T; i++) reqs_prev += TH[i].total.n;
      last = now;
    }
  }
  atomic_store(&stop_flag, 1);
  for (int i = 0; i < T; i++) pthread_join(TH[i].pt, NULL);
  struct hist tot, hold, rdown; hist_init(&tot); hist_init(&hold); hist_init(&rdown);
  uint64_t fails = 0, kn[K_N] = {0}, kf[K_N] = {0}, fs[6] = {0}, tmo = 0, cf = 0, rc = 0;
  for (int i = 0; i < T; i++) {
    hist_merge(&tot, &TH[i].total); hist_merge(&hold, &TH[i].hold); hist_merge(&rdown, &TH[i].rdown); fails += TH[i].fails; tmo += TH[i].timeouts; cf += TH[i].conn_fail; rc += TH[i].reconnects;
    for (int k = 0; k < K_N; k++) { kn[k] += TH[i].kind_n[k]; kf[k] += TH[i].kind_fail[k]; }
    for (int k = 0; k < 6; k++) fs[k] += TH[i].fail_status[k];
  }
  printf("== loadc %d VUs, ramp %d s, hold %d s, ramp-down %d s\n", N, RAMP, HOLD, DOWN);
  summary("whole run", &tot, fails, (double)(RAMP + HOLD + DOWN));
  summary("hold only", &hold, 0, (double)HOLD);
  if (DOWN) summary("ramp-down", &rdown, 0, (double)DOWN);
  for (int k = 0; k < K_N; k++) printf("  %-16s %10llu requests, %llu failed\n", kname[k], (unsigned long long)kn[k], (unsigned long long)kf[k]);
  printf("  failures: 4xx=%llu 5xx+transport=%llu timeouts=%llu connect_failures=%llu reconnects=%llu\n",
         (unsigned long long)fs[4], (unsigned long long)fs[5], (unsigned long long)tmo, (unsigned long long)cf, (unsigned long long)rc);
  { struct hist ch; hist_init(&ch); for (int i = 0; i < T; i++) hist_merge(&ch, &TH[i].conn);
    uint64_t over1s = 0; for (uint64_t i = 9000; i < HBUCKETS; i++) over1s += ch.b[i];
    printf("  connect time: n=%llu p50=%.1f p99=%.1f max=%.0f ms, %llu took over 0.9 s (a dropped SYN)\n", (unsigned long long)ch.n,
           hist_pct(&ch, 0.5), hist_pct(&ch, 0.99), (double)ch.max / 1e6, (unsigned long long)over1s); }
  { int any = 0; for (int e = 0; e < 140; e++) if (cerr[e]) { if (!any) printf("  connect failures by errno:"); any = 1; printf(" %s=%lu", e ? strerror(e) : "hup-no-error", cerr[e]); } if (any) printf("\n"); }
  return 0;
}
