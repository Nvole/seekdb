#include "file_sql_core.h"

#include <ctype.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>

#if !defined(_WIN32)
#include <dirent.h>
#include <fnmatch.h>
#include <unistd.h>
#endif

enum { kMaxCols = 4096, kMaxName = 256, kErrCap = 512, kFieldCap = 16 * 1024 * 1024 };

struct Buf {
  char *p;
  size_t n;
  size_t cap;
};

int buf_reserve(Buf *b, size_t need) {
  if (need <= b->cap) return 0;
  size_t cap = b->cap ? b->cap : 256;
  while (cap < need) {
    if (cap > (size_t)kFieldCap) return -1;
    cap *= 2;
  }
  char *np = (char *)realloc(b->p, cap);
  if (!np) return -1;
  b->p = np;
  b->cap = cap;
  return 0;
}

int buf_add(Buf *b, const char *s, size_t n) {
  if (buf_reserve(b, b->n + n + 1)) return -1;
  memcpy(b->p + b->n, s, n);
  b->n += n;
  b->p[b->n] = 0;
  return 0;
}

int buf_putc(Buf *b, char c) {
  return buf_add(b, &c, 1);
}

void buf_reset(Buf *b) { b->n = 0; if (b->p) b->p[0] = 0; }
void buf_free(Buf *b) { free(b->p); b->p = NULL; b->n = b->cap = 0; }

void set_err(char *err, int errcap, const char *msg) {
  if (!err || errcap <= 0) return;
  snprintf(err, (size_t)errcap, "%s", msg ? msg : "file sql error");
}

int is_remote_path(const char *path) {
  if (!path) return 1;
  if (strstr(path, "://")) return 1;
  if (strncmp(path, "http:", 5) == 0 || strncmp(path, "https:", 6) == 0 ||
      strncmp(path, "s3:", 3) == 0 || strncmp(path, "hdfs:", 5) == 0 ||
      strncmp(path, "oss:", 4) == 0) return 1;
  return 0;
}

int eq_ci(const char *s, int n, const char *lit) {
  int i = 0;
  for (; lit[i]; ++i) {
    if (i >= n) return 0;
    if (tolower((unsigned char)s[i]) != tolower((unsigned char)lit[i])) return 0;
  }
  return i == n;
}

int is_leap(int y) { return (y % 4 == 0 && y % 100 != 0) || (y % 400 == 0); }

int valid_ymd(int y, int m, int d) {
  static const int md[12] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
  if (y < 1 || y > 9999 || m < 1 || m > 12 || d < 1) return 0;
  int dim = md[m - 1];
  if (m == 2 && is_leap(y)) dim = 29;
  return d <= dim;
}

int parse_i64(const char *s, int n, int64_t *out) {
  if (n <= 0) return 0;
  int i = 0;
  int neg = 0;
  if (s[0] == '+' || s[0] == '-') {
    neg = s[0] == '-';
    i = 1;
    if (i >= n) return 0;
  }
  if (s[i] < '0' || s[i] > '9') return 0;
  uint64_t v = 0;
  for (; i < n; ++i) {
    if (s[i] < '0' || s[i] > '9') return 0;
    uint64_t d = (uint64_t)(s[i] - '0');
    if (v > (uint64_t)INT64_MAX / 10) return 0;
    v = v * 10 + d;
  }
  if (!neg) {
    if (v > (uint64_t)INT64_MAX) return 0;
    *out = (int64_t)v;
  } else {
    if (v > (uint64_t)INT64_MAX + 1) return 0;
    *out = v == (uint64_t)INT64_MAX + 1 ? INT64_MIN : -(int64_t)v;
  }
  return 1;
}

int parse_f64(const char *s, int n, double *out) {
  if (n <= 0 || n > 128) return 0;
  char tmp[160];
  memcpy(tmp, s, (size_t)n);
  tmp[n] = 0;
  char *end = NULL;
  errno = 0;
  double v = strtod(tmp, &end);
  if (end != tmp + n || errno == ERANGE) return 0;
  int saw_dot = 0, saw_exp = 0, saw_digit = 0;
  for (int i = 0; i < n; ++i) {
    char c = tmp[i];
    if (c == '.' ) saw_dot = 1;
    else if (c == 'e' || c == 'E') saw_exp = 1;
    else if (c >= '0' && c <= '9') saw_digit = 1;
  }
  if (!saw_digit || (!saw_dot && !saw_exp)) return 0;
  *out = v;
  return 1;
}

int parse_date(const char *s, int n, int *y, int *m, int *d) {
  if (n != 10 || s[4] != '-' || s[7] != '-') return 0;
  int64_t yy, mm, dd;
  if (!parse_i64(s, 4, &yy) || !parse_i64(s + 5, 2, &mm) || !parse_i64(s + 8, 2, &dd)) return 0;
  if (!valid_ymd((int)yy, (int)mm, (int)dd)) return 0;
  *y = (int)yy; *m = (int)mm; *d = (int)dd;
  return 1;
}

int parse_datetime(const char *s, int n) {
  if (n < 19) return 0;
  char sep = s[10];
  if (sep != ' ' && sep != 'T' && sep != 't') return 0;
  int y, m, d;
  if (!parse_date(s, 10, &y, &m, &d)) return 0;
  if (s[13] != ':' || s[16] != ':') return 0;
  int64_t hh, mi, ss;
  if (!parse_i64(s + 11, 2, &hh) || !parse_i64(s + 14, 2, &mi) || !parse_i64(s + 17, 2, &ss)) return 0;
  if (hh < 0 || hh > 23 || mi < 0 || mi > 59 || ss < 0 || ss > 59) return 0;
  if (n == 19) return 1;
  if (s[19] != '.') return 0;
  if (n == 20) return 0;
  for (int i = 20; i < n; ++i) if (s[i] < '0' || s[i] > '9') return 0;
  return n - 20 <= 6;
}

int classify_text(const char *s, int n) {
  if (n <= 0) return FS_NULL;
  if (eq_ci(s, n, "true") || eq_ci(s, n, "false")) return FS_BOOL;
  int64_t iv;
  if (parse_i64(s, n, &iv)) return FS_BIGINT;
  double dv;
  if (parse_f64(s, n, &dv)) return FS_DOUBLE;
  if (parse_datetime(s, n)) return FS_DATETIME;
  int y, m, d;
  if (parse_date(s, n, &y, &m, &d)) return FS_DATE;
  return FS_VARCHAR;
}

int is_num_type(int t) {
  return t == FS_BOOL || t == FS_BIGINT || t == FS_DOUBLE;
}

int num_rank(int t) {
  if (t == FS_BOOL) return 1;
  if (t == FS_BIGINT) return 2;
  if (t == FS_DOUBLE) return 3;
  return 0;
}

int widen_type(int cur, int nxt) {
  if (cur == FS_NULL || cur == 0) return nxt;
  if (nxt == FS_NULL) return cur;
  if (cur == nxt) return cur;
  if (is_num_type(cur) && is_num_type(nxt)) return num_rank(cur) >= num_rank(nxt) ? cur : nxt;
  if ((cur == FS_DATE && nxt == FS_DATETIME) || (cur == FS_DATETIME && nxt == FS_DATE)) return FS_DATETIME;
  return FS_VARCHAR;
}

int text_fits(int type, const char *s, int n) {
  if (n <= 0) return 1;
  int got = classify_text(s, n);
  if (got == FS_NULL) return 1;
  if (type == FS_VARCHAR) return 1;
  if (type == FS_NULL) return 1;
  if (got == type) return 1;
  if (is_num_type(type) && is_num_type(got) && num_rank(got) <= num_rank(type)) return 1;
  if (type == FS_DATETIME && (got == FS_DATE || got == FS_DATETIME)) return 1;
  if (type == FS_DOUBLE && got == FS_BIGINT) return 1;
  if (type == FS_BIGINT && got == FS_BOOL) return 1;
  return 0;
}

struct ColState {
  FsCol col;
  int seen;
};

int name_exists(ColState *cols, int n, const char *name, int nlen) {
  for (int i = 0; i < n; ++i) {
    if (cols[i].col.name_len == nlen && memcmp(cols[i].col.name, name, (size_t)nlen) == 0) return 1;
  }
  return 0;
}

int add_unique_name(ColState *cols, int *n, int cap, const char *raw, int raw_len) {
  if (*n >= cap) return -1;
  char tmp[kMaxName];
  int len = raw_len;
  if (len <= 0) {
    memcpy(tmp, "column", 6);
    len = 6;
  } else {
    if (len >= kMaxName) len = kMaxName - 1;
    memcpy(tmp, raw, (size_t)len);
  }
  tmp[len] = 0;
  if (name_exists(cols, *n, tmp, len)) {
    int k = 2;
    for (;;) {
      int m = snprintf(tmp, sizeof(tmp), "%.*s__%d", raw_len > 200 ? 200 : raw_len, raw, k);
      if (m < 0) return -1;
      if (!name_exists(cols, *n, tmp, m)) { len = m; break; }
      ++k;
      if (k > 100000) return -1;
    }
  }
  ColState *c = &cols[*n];
  memset(c, 0, sizeof(*c));
  memcpy(c->col.name, tmp, (size_t)len);
  c->col.name[len] = 0;
  c->col.name_len = len;
  c->col.type = FS_NULL;
  c->col.nullable = 0;
  c->col.ordinal = *n;
  *n += 1;
  return *n - 1;
}

void observe(ColState *c, int ty, int is_null) {
  c->seen = 1;
  if (is_null || ty == FS_NULL) {
    c->col.nullable = 1;
    return;
  }
  c->col.type = widen_type(c->col.type, ty);
}

void finish_nullability(ColState *cols, int n, int saw_value_nonnull[]) {
  for (int i = 0; i < n; ++i) {
    cols[i].col.nullable = saw_value_nonnull[i] ? cols[i].col.nullable : 1;
  }
}

struct Sig {
  int64_t size;
  int64_t mtime_sec;
  int64_t mtime_nsec;
  uint64_t ino;
  int ok;
};

int stat_path(const char *path, Sig *s) {
  struct stat st;
  if (stat(path, &st) != 0) return -1;
  s->size = (int64_t)st.st_size;
  s->mtime_sec = (int64_t)st.st_mtime;
#if defined(st_mtime)
  s->mtime_nsec = 0;
#ifdef __linux__
  s->mtime_nsec = (int64_t)st.st_mtim.tv_nsec;
  s->mtime_sec = (int64_t)st.st_mtim.tv_sec;
#endif
#endif
  s->ino = (uint64_t)st.st_ino;
  s->ok = 1;
  return 0;
}

#define reader_err(r, msg) snprintf((r)->err, sizeof((r)->err), "%s", (msg) ? (msg) : "file sql error")

/* ---------------- CSV ---------------- */

struct CsvIn {
  FILE *fp;
  char buf[8192];
  size_t n, i;
  int eof;
  int line;
  int col;
};

int csv_getc(CsvIn *in, int *line) {
  if (in->i >= in->n) {
    if (in->eof) return EOF;
    in->n = fread(in->buf, 1, sizeof(in->buf), in->fp);
    in->i = 0;
    if (in->n == 0) { in->eof = 1; return EOF; }
  }
  unsigned char c = (unsigned char)in->buf[in->i++];
  if (c == '\n') { in->line++; in->col = 1; }
  else in->col++;
  if (line) *line = in->line;
  return c;
}

int csv_read_row(CsvIn *in, FsOptions opt, Buf *fields, int max_fields, int *nfields, int *start_line, char *err, int errcap) {
  *nfields = 0;
  int c = csv_getc(in, NULL);
  if (c == EOF) return FS_END;
  *start_line = in->line;
  /* skip blank? keep empty row as one empty field if not EOF */
  Buf *cur = NULL;
  int in_q = 0;
  int field_i = 0;
  int any = 0;
  auto start_field = [&]() -> int {
    if (field_i >= max_fields) {
      set_err(err, errcap, "too many columns");
      return FS_ERR;
    }
    buf_reset(&fields[field_i]);
    cur = &fields[field_i];
    return FS_OK;
  };
  if (start_field()) return FS_ERR;
  for (;;) {
    if (c == EOF) break;
    any = 1;
    if (in_q) {
      if (c == opt.quote) {
        int n1 = csv_getc(in, NULL);
        if (n1 == opt.quote && opt.escape == opt.quote) {
          if (buf_putc(cur, (char)c)) { set_err(err, errcap, "field too large"); return FS_ERR; }
        } else if (opt.escape != opt.quote && c == opt.escape) {
          int n2 = csv_getc(in, NULL);
          if (n2 == EOF) { set_err(err, errcap, "unterminated escape"); return FS_BAD_ROW; }
          if (buf_putc(cur, (char)n2)) return FS_ERR;
        } else {
          in_q = 0;
          c = n1;
          continue;
        }
      } else if (opt.escape != opt.quote && c == opt.escape) {
        int n2 = csv_getc(in, NULL);
        if (n2 == EOF) { set_err(err, errcap, "unterminated escape"); return FS_BAD_ROW; }
        if (buf_putc(cur, (char)n2)) return FS_ERR;
      } else {
        if (buf_putc(cur, (char)c)) { set_err(err, errcap, "field too large"); return FS_ERR; }
      }
    } else {
      if (c == opt.quote && cur->n == 0) {
        in_q = 1;
      } else if (c == opt.delim) {
        field_i++;
        if (start_field()) return FS_ERR;
      } else if (c == '\n') {
        break;
      } else if (c == '\r') {
        int n1 = csv_getc(in, NULL);
        if (n1 != '\n' && n1 != EOF) {
          /* put back is hard; treat CR as data if not CRLF. Unread by sliding. */
          if (in->i == 0) {
            if (buf_putc(cur, '\r')) return FS_ERR;
          } else {
            in->i--;
            if (in->buf[in->i] == '\n') { /* shouldn't */ }
            else {
              /* we consumed n1 which is stored; step back */
            }
          }
          /* simpler: if not LF, keep CR and reprocess n1 */
          if (n1 != '\n' && n1 != EOF) {
            if (buf_putc(cur, '\r')) return FS_ERR;
            c = n1;
            continue;
          }
        }
        break;
      } else {
        if (buf_putc(cur, (char)c)) { set_err(err, errcap, "field too large"); return FS_ERR; }
      }
    }
    c = csv_getc(in, NULL);
  }
  if (in_q) {
    snprintf(err, errcap > 0 ? (size_t)errcap : 0, "unterminated quote at line %d", *start_line);
    return FS_BAD_ROW;
  }
  if (!any && c == EOF && field_i == 0 && fields[0].n == 0) return FS_END;
  *nfields = field_i + 1;
  return FS_OK;
}

int field_is_null(const FsOptions *opt, const char *s, int n) {
  if (opt->nullstr && opt->nullstr_len > 0 && n == opt->nullstr_len &&
      memcmp(s, opt->nullstr, (size_t)n) == 0) return 1;
  if (n == 0) return 1;
  return 0;
}

int csv_infer(const char *path, const FsOptions *opt, FsCol *cols, int cap, int *ncol, char *err, int errcap) {
  if (opt->sample_size != -1) {
    set_err(err, errcap, "sample_size only supports -1");
    return FS_UNSUPPORTED;
  }
  FILE *fp = fopen(path, "rb");
  if (!fp) { set_err(err, errcap, "cannot open file"); return FS_NOT_FOUND; }
  CsvIn in;
  memset(&in, 0, sizeof(in));
  in.fp = fp;
  in.line = 1;
  Buf *fields = (Buf *)calloc((size_t)kMaxCols, sizeof(Buf));
  ColState *st = (ColState *)calloc((size_t)kMaxCols, sizeof(ColState));
  int *nonnull = (int *)calloc((size_t)kMaxCols, sizeof(int));
  if (!fields || !st || !nonnull) { fclose(fp); free(fields); free(st); free(nonnull); return FS_ERR; }
  int n = 0;
  int header = opt->header;
  int rows = 0;
  int rc = FS_OK;
  for (;;) {
    int nf = 0, line = 0;
    rc = csv_read_row(&in, *opt, fields, kMaxCols, &nf, &line, err, errcap);
    if (rc == FS_END) { rc = FS_OK; break; }
    if (rc != FS_OK) break;
    if (header) {
      for (int i = 0; i < nf; ++i) {
        if (add_unique_name(st, &n, cap, fields[i].p ? fields[i].p : "", (int)fields[i].n) < 0) {
          set_err(err, errcap, "too many columns");
          rc = FS_ERR;
          break;
        }
      }
      header = 0;
      continue;
    }
    if (n == 0) {
      for (int i = 0; i < nf; ++i) {
        char nm[32];
        snprintf(nm, sizeof(nm), "column%d", i + 1);
        if (add_unique_name(st, &n, cap, nm, (int)strlen(nm)) < 0) { rc = FS_ERR; break; }
      }
    }
    if (nf != n) {
      snprintf(err, errcap > 0 ? (size_t)errcap : 0,
               "csv column count mismatch at line %d: got %d expected %d", line, nf, n);
      rc = FS_BAD_ROW;
      break;
    }
    for (int i = 0; i < nf; ++i) {
      const char *s = fields[i].p ? fields[i].p : "";
      int len = (int)fields[i].n;
      if (field_is_null(opt, s, len)) {
        st[i].col.nullable = 1;
      } else {
        int ty = classify_text(s, len);
        observe(&st[i], ty, 0);
        nonnull[i] = 1;
      }
    }
    rows++;
  }
  if (rc == FS_OK) {
    for (int i = 0; i < n; ++i) {
      if (!nonnull[i]) {
        st[i].col.type = FS_NULL;
        st[i].col.nullable = 1;
      }
      cols[i] = st[i].col;
    }
    *ncol = n;
    if (n == 0) {
      set_err(err, errcap, "csv has no columns");
      rc = FS_ERR;
    }
  }
  for (int i = 0; i < kMaxCols; ++i) buf_free(&fields[i]);
  free(fields); free(st); free(nonnull);
  fclose(fp);
  (void)rows;
  return rc;
}

/* ---------------- JSONL ---------------- */

void skip_ws(const char *s, int n, int *i) {
  while (*i < n && (s[*i] == ' ' || s[*i] == '\t' || s[*i] == '\r')) (*i)++;
}

int parse_json_string(const char *s, int n, int *i, Buf *out) {
  if (*i >= n || s[*i] != '"') return -1;
  (*i)++;
  buf_reset(out);
  while (*i < n) {
    char c = s[(*i)++];
    if (c == '"') return 0;
    if (c == '\\') {
      if (*i >= n) return -1;
      char e = s[(*i)++];
      char o = e;
      if (e == 'n') o = '\n';
      else if (e == 't') o = '\t';
      else if (e == 'r') o = '\r';
      else if (e == '"' || e == '\\' || e == '/') o = e;
      else if (e == 'u') {
        if (*i + 4 > n) return -1;
        int cp = 0;
        for (int k = 0; k < 4; ++k) {
          char h = s[(*i)++];
          cp <<= 4;
          if (h >= '0' && h <= '9') cp += h - '0';
          else if (h >= 'a' && h <= 'f') cp += h - 'a' + 10;
          else if (h >= 'A' && h <= 'F') cp += h - 'A' + 10;
          else return -1;
        }
        if (cp < 128) {
          if (buf_putc(out, (char)cp)) return -1;
        } else {
          /* store as UTF-8 */
          char tmp[4];
          int m = 0;
          if (cp < 0x800) {
            tmp[0] = (char)(0xC0 | (cp >> 6));
            tmp[1] = (char)(0x80 | (cp & 0x3F));
            m = 2;
          } else {
            tmp[0] = (char)(0xE0 | (cp >> 12));
            tmp[1] = (char)(0x80 | ((cp >> 6) & 0x3F));
            tmp[2] = (char)(0x80 | (cp & 0x3F));
            m = 3;
          }
          if (buf_add(out, tmp, (size_t)m)) return -1;
        }
        continue;
      } else return -1;
      if (buf_putc(out, o)) return -1;
    } else {
      if (buf_putc(out, c)) return -1;
    }
  }
  return -1;
}

enum { JV_NULL, JV_BOOL, JV_INT, JV_FLOAT, JV_STR, JV_NEST };

struct JVal {
  int kind;
  int b;
  int64_t i64;
  double f64;
  Buf str;
};

void jval_free(JVal *v) { buf_free(&v->str); }

int parse_json_value(const char *s, int n, int *i, JVal *v) {
  skip_ws(s, n, i);
  if (*i >= n) return -1;
  char c = s[*i];
  if (c == '{') return JV_NEST;
  if (c == '[') return JV_NEST;
  if (c == '"') {
    v->kind = JV_STR;
    if (parse_json_string(s, n, i, &v->str)) return -1;
    return 0;
  }
  if (c == 't' && *i + 4 <= n && memcmp(s + *i, "true", 4) == 0) {
    *i += 4; v->kind = JV_BOOL; v->b = 1; return 0;
  }
  if (c == 'f' && *i + 5 <= n && memcmp(s + *i, "false", 5) == 0) {
    *i += 5; v->kind = JV_BOOL; v->b = 0; return 0;
  }
  if (c == 'n' && *i + 4 <= n && memcmp(s + *i, "null", 4) == 0) {
    *i += 4; v->kind = JV_NULL; return 0;
  }
  int start = *i;
  if (c == '-' ) (*i)++;
  if (*i >= n || s[*i] < '0' || s[*i] > '9') return -1;
  while (*i < n && s[*i] >= '0' && s[*i] <= '9') (*i)++;
  int is_float = 0;
  if (*i < n && s[*i] == '.') {
    is_float = 1; (*i)++;
    if (*i >= n || s[*i] < '0' || s[*i] > '9') return -1;
    while (*i < n && s[*i] >= '0' && s[*i] <= '9') (*i)++;
  }
  if (*i < n && (s[*i] == 'e' || s[*i] == 'E')) {
    is_float = 1; (*i)++;
    if (*i < n && (s[*i] == '+' || s[*i] == '-')) (*i)++;
    if (*i >= n || s[*i] < '0' || s[*i] > '9') return -1;
    while (*i < n && s[*i] >= '0' && s[*i] <= '9') (*i)++;
  }
  int len = *i - start;
  if (!is_float && parse_i64(s + start, len, &v->i64)) { v->kind = JV_INT; return 0; }
  if (parse_f64(s + start, len, &v->f64) || is_float) {
    char tmp[160];
    if (len >= (int)sizeof(tmp)) return -1;
    memcpy(tmp, s + start, (size_t)len);
    tmp[len] = 0;
    char *end = NULL;
    v->f64 = strtod(tmp, &end);
    v->kind = JV_FLOAT;
    return 0;
  }
  return -1;
}

int find_col(ColState *st, int n, const char *name, int nlen) {
  for (int i = 0; i < n; ++i) {
    if (st[i].col.name_len == nlen && memcmp(st[i].col.name, name, (size_t)nlen) == 0) return i;
  }
  return -1;
}

int json_type_of(const JVal *v) {
  if (v->kind == JV_NULL) return FS_NULL;
  if (v->kind == JV_BOOL) return FS_BOOL;
  if (v->kind == JV_INT) return FS_BIGINT;
  if (v->kind == JV_FLOAT) return FS_DOUBLE;
  if (v->kind == JV_STR) {
    int ty = classify_text(v->str.p ? v->str.p : "", (int)v->str.n);
    if (ty == FS_BIGINT || ty == FS_DOUBLE || ty == FS_BOOL) return FS_VARCHAR;
    return ty;
  }
  return FS_VARCHAR;
}

int jsonl_infer(const char *path, const FsOptions *opt, FsCol *cols, int cap, int *ncol, char *err, int errcap) {
  if (opt->sample_size != -1) {
    set_err(err, errcap, "sample_size only supports -1");
    return FS_UNSUPPORTED;
  }
  if (opt->json_format && opt->json_format_len > 0) {
    if (!eq_ci(opt->json_format, opt->json_format_len, "newline_delimited") &&
        !eq_ci(opt->json_format, opt->json_format_len, "ndjson") &&
        !eq_ci(opt->json_format, opt->json_format_len, "jsonl") &&
        !eq_ci(opt->json_format, opt->json_format_len, "auto")) {
      set_err(err, errcap, "unsupported json format");
      return FS_UNSUPPORTED;
    }
  }
  FILE *fp = fopen(path, "rb");
  if (!fp) { set_err(err, errcap, "cannot open file"); return FS_NOT_FOUND; }
  ColState *st = (ColState *)calloc((size_t)cap, sizeof(ColState));
  int *present = (int *)calloc((size_t)cap, sizeof(int));
  int *nonnull = (int *)calloc((size_t)cap, sizeof(int));
  int total_rows = 0;
  char *line = NULL;
  size_t lcap = 0;
  int n = 0, lineno = 0, rc = FS_OK;
  if (!st || !present || !nonnull) { rc = FS_ERR; goto done; }
  while (1) {
    ssize_t m = getline(&line, &lcap, fp);
    if (m < 0) break;
    lineno++;
    while (m > 0 && (line[m - 1] == '\n' || line[m - 1] == '\r')) line[--m] = 0;
    int i = 0;
    while (i < m && (line[i] == ' ' || line[i] == '\t')) i++;
    if (i >= m) continue;
    if (line[i] != '{') {
      snprintf(err, errcap > 0 ? (size_t)errcap : 0, "jsonl line %d is not a flat object", lineno);
      rc = FS_BAD_ROW;
      break;
    }
    i++;
    int saw[kMaxCols];
    memset(saw, 0, sizeof(saw));
    int guard = 0;
    while (1) {
      skip_ws(line, (int)m, &i);
      if (i < m && line[i] == '}') { i++; break; }
      if (guard++ && i < m && line[i] == ',') { i++; skip_ws(line, (int)m, &i); }
      Buf key;
      memset(&key, 0, sizeof(key));
      if (parse_json_string(line, (int)m, &i, &key)) {
        snprintf(err, errcap > 0 ? (size_t)errcap : 0, "jsonl line %d bad key", lineno);
        rc = FS_BAD_ROW;
        buf_free(&key);
        break;
      }
      skip_ws(line, (int)m, &i);
      if (i >= m || line[i] != ':') { rc = FS_BAD_ROW; buf_free(&key); break; }
      i++;
      JVal val;
      memset(&val, 0, sizeof(val));
      int pr = parse_json_value(line, (int)m, &i, &val);
      if (pr == JV_NEST || val.kind == JV_NEST || (pr < 0 && pr != 0)) {
        if (pr == JV_NEST) {
          snprintf(err, errcap > 0 ? (size_t)errcap : 0, "nested json at line %d", lineno);
          rc = FS_NESTED;
        } else if (pr < 0) {
          snprintf(err, errcap > 0 ? (size_t)errcap : 0, "jsonl line %d bad value", lineno);
          rc = FS_BAD_ROW;
        }
        buf_free(&key); jval_free(&val);
        break;
      }
      int idx = find_col(st, n, key.p ? key.p : "", (int)key.n);
      if (idx < 0) {
        idx = add_unique_name(st, &n, cap, key.p ? key.p : "", (int)key.n);
        if (idx < 0) { rc = FS_ERR; buf_free(&key); jval_free(&val); break; }
      }
      if (idx < kMaxCols) saw[idx] = 1;
      int ty = json_type_of(&val);
      if (ty == FS_NULL) st[idx].col.nullable = 1;
      else {
        if (st[idx].col.type != FS_NULL && st[idx].col.type != FS_VARCHAR && ty == FS_VARCHAR) {
          if (!text_fits(st[idx].col.type, val.str.p ? val.str.p : "", (int)val.str.n)) {
            st[idx].col.type = FS_VARCHAR;
          }
        } else if (ty == FS_VARCHAR && st[idx].col.type != FS_NULL && st[idx].col.type != FS_VARCHAR) {
          if (text_fits(st[idx].col.type, val.str.p ? val.str.p : "", (int)val.str.n)) {
            /* keep narrower type */
          } else st[idx].col.type = widen_type(st[idx].col.type, ty);
        } else {
          st[idx].col.type = widen_type(st[idx].col.type, ty);
        }
        nonnull[idx] = 1;
      }
      buf_free(&key);
      jval_free(&val);
    }
    if (rc != FS_OK) break;
    total_rows++;
    for (int c = 0; c < n; ++c) {
      if (saw[c]) present[c]++;
      else st[c].col.nullable = 1;
    }
  }
  if (rc == FS_OK) {
    for (int i = 0; i < n; ++i) {
      if (!nonnull[i]) { st[i].col.type = FS_NULL; st[i].col.nullable = 1; }
      if (present[i] < total_rows) st[i].col.nullable = 1;
      cols[i] = st[i].col;
    }
    *ncol = n;
    if (n == 0) { set_err(err, errcap, "jsonl has no columns"); rc = FS_ERR; }
  }
done:
  free(line); free(st); free(present); free(nonnull);
  fclose(fp);
  return rc;
}

/* ---------------- scan readers ---------------- */

int fill_from_text(const FsCol *col, const char *s, int n, int is_null, FsValue *out, Buf *store) {
  out->is_null = 0;
  out->type = col->type;
  out->str = NULL;
  out->str_len = 0;
  out->i64 = 0;
  out->f64 = 0;
  if (is_null || col->type == FS_NULL) { out->is_null = 1; out->type = col->type == FS_NULL ? FS_NULL : col->type; return FS_OK; }
  if (col->type == FS_VARCHAR || col->type == FS_DATE || col->type == FS_DATETIME) {
    buf_reset(store);
    if (buf_add(store, s, (size_t)n)) return FS_ERR;
    out->str = store->p;
    out->str_len = n;
    out->type = col->type;
    return FS_OK;
  }
  if (col->type == FS_BOOL) {
    if (eq_ci(s, n, "true") || (n == 1 && s[0] == '1')) { out->i64 = 1; return FS_OK; }
    if (eq_ci(s, n, "false") || (n == 1 && s[0] == '0')) { out->i64 = 0; return FS_OK; }
    return FS_BAD_ROW;
  }
  if (col->type == FS_BIGINT) {
    if (eq_ci(s, n, "true")) { out->i64 = 1; return FS_OK; }
    if (eq_ci(s, n, "false")) { out->i64 = 0; return FS_OK; }
    if (!parse_i64(s, n, &out->i64)) return FS_BAD_ROW;
    return FS_OK;
  }
  if (col->type == FS_DOUBLE) {
    if (eq_ci(s, n, "true")) { out->f64 = 1; return FS_OK; }
    if (eq_ci(s, n, "false")) { out->f64 = 0; return FS_OK; }
    int64_t iv;
    if (parse_i64(s, n, &iv)) { out->f64 = (double)iv; return FS_OK; }
    if (!parse_f64(s, n, &out->f64)) return FS_BAD_ROW;
    return FS_OK;
  }
  out->is_null = 1;
  return FS_OK;
}

struct CsvCursor {
  FILE *fp;
  char buf[8192];
  size_t n, i;
  int eof;
  int line;
};

static int cur_getc(CsvCursor *in) {
  if (in->i >= in->n) {
    if (in->eof) return EOF;
    in->n = fread(in->buf, 1, sizeof(in->buf), in->fp);
    in->i = 0;
    if (in->n == 0) { in->eof = 1; return EOF; }
  }
  unsigned char c = (unsigned char)in->buf[in->i++];
  if (c == '\n') in->line++;
  return c;
}

static int cur_row(CsvCursor *in, FsOptions opt, Buf *fields, int max_fields, int *nfields, int *start_line, char *err, int errcap) {
  int c = cur_getc(in);
  if (c == EOF) return FS_END;
  *start_line = in->line == 0 ? 1 : in->line;
  int field_i = 0;
  int in_q = 0;
  buf_reset(&fields[0]);
  for (;;) {
    if (c == EOF) break;
    if (in_q) {
      if (c == opt.quote) {
        int n1 = cur_getc(in);
        if (n1 == opt.quote) {
          if (buf_putc(&fields[field_i], (char)c)) return FS_ERR;
        } else {
          in_q = 0;
          c = n1;
          continue;
        }
      } else if (buf_putc(&fields[field_i], (char)c)) return FS_ERR;
    } else {
      if (c == opt.quote && fields[field_i].n == 0) in_q = 1;
      else if (c == opt.delim) {
        field_i++;
        if (field_i >= max_fields) { set_err(err, errcap, "too many columns"); return FS_ERR; }
        buf_reset(&fields[field_i]);
      } else if (c == '\n') break;
      else if (c == '\r') {
        int n1 = cur_getc(in);
        if (n1 != '\n' && n1 != EOF) {
          if (buf_putc(&fields[field_i], '\r')) return FS_ERR;
          c = n1;
          continue;
        }
        break;
      } else if (buf_putc(&fields[field_i], (char)c)) return FS_ERR;
    }
    c = cur_getc(in);
  }
  if (in_q) {
    snprintf(err, errcap > 0 ? (size_t)errcap : 0, "unterminated quote at line %d", *start_line);
    return FS_BAD_ROW;
  }
  *nfields = field_i + 1;
  return FS_OK;
}

struct FsReaderImpl {
  int kind;
  FILE *fp;
  FsOptions opt;
  FsCol cols[kMaxCols];
  int ncol;
  Sig sig;
  int line;
  int header_done;
  char err[kErrCap];
  char path[4096];
  Buf fields[512];
  int fields_ready;
  CsvCursor csv;
  int csv_init;
  /* glob */
  char **gpaths;
  int ng, gi;
  /* parquet state kept as opaque pointer */
  void *pq;
};

static FsReaderImpl *as_impl(FsReader *r) { return (FsReaderImpl *)r; }

static int ensure_fields(FsReaderImpl *r, int n) {
  if (r->fields_ready) return 0;
  int m = n < 512 ? 512 : n;
  if (m > kMaxCols) return -1;
  memset(r->fields, 0, sizeof(r->fields));
  r->fields_ready = 1;
  (void)m;
  return 0;
}

void fs_options_init(FsOptions *opt) {
  memset(opt, 0, sizeof(*opt));
  opt->header = 1;
  opt->delim = ',';
  opt->quote = '"';
  opt->escape = '"';
  opt->sample_size = -1;
  opt->expect_size = -1;
  opt->expect_mtime_sec = -1;
  opt->expect_mtime_nsec = -1;
}

int fs_check_local_path(const char *path, char *err, int errcap) {
  if (!path || !path[0]) { set_err(err, errcap, "empty path"); return FS_ERR; }
  if (is_remote_path(path)) { set_err(err, errcap, "remote file is not supported"); return FS_UNSUPPORTED; }
  return FS_OK;
}

int fs_stat_file(const char *path, int64_t *size, int64_t *mtime_sec, int64_t *mtime_nsec,
                 uint64_t *ino, char *err, int errcap) {
  if (fs_check_local_path(path, err, errcap)) return FS_UNSUPPORTED;
  Sig s;
  if (stat_path(path, &s)) { set_err(err, errcap, "cannot stat file"); return FS_NOT_FOUND; }
  if (size) *size = s.size;
  if (mtime_sec) *mtime_sec = s.mtime_sec;
  if (mtime_nsec) *mtime_nsec = s.mtime_nsec;
  if (ino) *ino = s.ino;
  return FS_OK;
}

const char *fs_type_name(int type) {
  switch (type) {
    case FS_NULL: return "NULL";
    case FS_BIGINT: return "BIGINT";
    case FS_DOUBLE: return "DOUBLE";
    case FS_BOOL: return "BOOLEAN";
    case FS_VARCHAR: return "VARCHAR";
    case FS_DATE: return "DATE";
    case FS_DATETIME: return "DATETIME";
    default: return "VARCHAR";
  }
}

int fs_infer(int kind, const char *path, const FsOptions *opt, FsCol *cols, int cap, int *ncol,
             char *err, int errcap) {
  if (!opt || !cols || !ncol) return FS_ERR;
  int ck = fs_check_local_path(path, err, errcap);
  if (ck) return ck;
  if (kind == FS_KIND_CSV) return csv_infer(path, opt, cols, cap, ncol, err, errcap);
  if (kind == FS_KIND_JSONL) return jsonl_infer(path, opt, cols, cap, ncol, err, errcap);
  if (kind == FS_KIND_GLOB) {
    if (cap < 1) return FS_ERR;
    memset(&cols[0], 0, sizeof(cols[0]));
    memcpy(cols[0].name, "file", 4);
    cols[0].name_len = 4;
    cols[0].type = FS_VARCHAR;
    cols[0].nullable = 0;
    *ncol = 1;
    return FS_OK;
  }
  if (kind == FS_KIND_PARQUET) {
    set_err(err, errcap, "parquet infer is performed by fs_open schema");
    return FS_UNSUPPORTED;
  }
  set_err(err, errcap, "unsupported format");
  return FS_UNSUPPORTED;
}

const char *fs_error(const FsReader *reader) {
  if (!reader) return "";
  return ((const FsReaderImpl *)reader)->err;
}
int fs_lineno(const FsReader *reader) {
  if (!reader) return 0;
  return ((const FsReaderImpl *)reader)->line;
}

static int open_common(FsReaderImpl *r, const char *path, const FsOptions *opt) {
  memset(r, 0, sizeof(*r));
  r->opt = *opt;
  snprintf(r->path, sizeof(r->path), "%s", path);
  if (stat_path(path, &r->sig)) {
    reader_err(r, "cannot stat file");
    return FS_NOT_FOUND;
  }
  if (opt->expect_size >= 0 && opt->expect_size != r->sig.size) {
    reader_err(r, "file changed before scan");
    return FS_CHANGED;
  }
  r->fp = fopen(path, "rb");
  if (!r->fp) { reader_err(r, "cannot open file"); return FS_NOT_FOUND; }
  return FS_OK;
}

static int csv_next_impl(FsReaderImpl *r, FsValue *vals, int ncol) {
  if (!r->csv_init) {
    memset(&r->csv, 0, sizeof(r->csv));
    r->csv.fp = r->fp;
    r->csv.line = 1;
    r->csv_init = 1;
    if (ensure_fields(r, r->ncol)) return FS_ERR;
  }
  int chs = FS_OK;
  {
    struct stat st;
    if (fstat(fileno(r->fp), &st) == 0) {
      if ((int64_t)st.st_size != r->sig.size || (uint64_t)st.st_ino != r->sig.ino) {
        reader_err(r, "file changed during scan");
        return FS_CHANGED;
      }
    }
  }
  while (1) {
    int nf = 0, line = 0;
    int rc = cur_row(&r->csv, r->opt, r->fields, kMaxCols, &nf, &line, r->err, kErrCap);
    if (rc == FS_END) return FS_END;
    if (rc != FS_OK) return rc;
    r->line = line;
    if (!r->header_done && r->opt.header) { r->header_done = 1; continue; }
    r->header_done = 1;
    if (nf < r->ncol) {
      snprintf(r->err, sizeof(r->err), "csv column count mismatch at line %d", line);
      return FS_BAD_ROW;
    }
    for (int i = 0; i < ncol && i < r->ncol; ++i) {
      int ord = r->cols[i].ordinal;
      if (ord < 0 || ord >= nf) { vals[i].is_null = 1; continue; }
      const char *s = r->fields[ord].p ? r->fields[ord].p : "";
      int len = (int)r->fields[ord].n;
      int is_null = field_is_null(&r->opt, s, len);
      Buf hold;
      memset(&hold, 0, sizeof(hold));
      if (len > 0 && buf_add(&hold, s, (size_t)len)) { buf_free(&hold); return FS_ERR; }
      int fr = fill_from_text(&r->cols[i], hold.p ? hold.p : "", len, is_null, &vals[i], &r->fields[ord]);
      buf_free(&hold);
      if (fr != FS_OK) {
        snprintf(r->err, sizeof(r->err), "csv value does not match type at line %d column %d", line, ord + 1);
        return FS_BAD_ROW;
      }
      if (!is_null && (r->cols[i].type == FS_VARCHAR || r->cols[i].type == FS_DATE || r->cols[i].type == FS_DATETIME)) {
        /* fill_from_text stored into the same field buffer which is the source. OK pointer. */
      }
    }
    return FS_OK;
  }
  (void)chs;
}

/* JSONL scan */
static int jsonl_next_impl(FsReaderImpl *r, FsValue *vals, int ncol) {
  if (!r->fields_ready) {
    memset(r->fields, 0, sizeof(r->fields));
    r->fields_ready = 1;
  }
  char *line = NULL;
  while (1) {
    errno = 0;
    /* use fgets growing */
    Buf lb; memset(&lb, 0, sizeof(lb));
    int c;
    int any = 0;
    while ((c = fgetc(r->fp)) != EOF) {
      any = 1;
      if (c == '\n') break;
      if (c == '\r') continue;
      if (buf_putc(&lb, (char)c)) { buf_free(&lb); return FS_ERR; }
    }
    if (!any && c == EOF) { buf_free(&lb); free(line); return FS_END; }
    r->line++;
    int m = (int)lb.n;
    int i = 0;
    const char *s = lb.p ? lb.p : "";
    while (i < m && (s[i] == ' ' || s[i] == '\t')) i++;
    if (i >= m) { buf_free(&lb); if (c == EOF) return FS_END; continue; }
    if (s[i] != '{') {
      snprintf(r->err, sizeof(r->err), "jsonl line %d is not a flat object", r->line);
      buf_free(&lb); free(line); return FS_BAD_ROW;
    }
    i++;
    for (int k = 0; k < ncol; ++k) { vals[k].is_null = 1; vals[k].type = r->cols[k].type; vals[k].str = NULL; vals[k].str_len = 0; }
    int rc = FS_OK;
    int guard = 0;
    while (rc == FS_OK) {
      skip_ws(s, m, &i);
      if (i < m && s[i] == '}') break;
      if (guard++ && i < m && s[i] == ',') { i++; skip_ws(s, m, &i); }
      Buf key; memset(&key, 0, sizeof(key));
      if (parse_json_string(s, m, &i, &key)) { rc = FS_BAD_ROW; buf_free(&key); break; }
      skip_ws(s, m, &i);
      if (i >= m || s[i] != ':') { rc = FS_BAD_ROW; buf_free(&key); break; }
      i++;
      JVal val; memset(&val, 0, sizeof(val));
      int pr = parse_json_value(s, m, &i, &val);
      if (pr == JV_NEST) {
        snprintf(r->err, sizeof(r->err), "nested json at line %d", r->line);
        rc = FS_NESTED; buf_free(&key); jval_free(&val); break;
      }
      if (pr < 0) { rc = FS_BAD_ROW; buf_free(&key); jval_free(&val); break; }
      for (int k = 0; k < ncol; ++k) {
        if (r->cols[k].name_len == (int)key.n && key.p && memcmp(r->cols[k].name, key.p, key.n) == 0) {
          if (val.kind == JV_NULL) vals[k].is_null = 1;
          else if (val.kind == JV_BOOL) {
            vals[k].is_null = 0;
            if (r->cols[k].type == FS_BOOL || r->cols[k].type == FS_BIGINT) vals[k].i64 = val.b ? 1 : 0;
            else if (r->cols[k].type == FS_DOUBLE) vals[k].f64 = val.b ? 1 : 0;
            else {
              buf_reset(&r->fields[k]);
              buf_add(&r->fields[k], val.b ? "true" : "false", val.b ? 4 : 5);
              vals[k].str = r->fields[k].p; vals[k].str_len = (int)r->fields[k].n;
            }
          } else if (val.kind == JV_INT) {
            vals[k].is_null = 0;
            if (r->cols[k].type == FS_DOUBLE) vals[k].f64 = (double)val.i64;
            else if (r->cols[k].type == FS_BOOL) vals[k].i64 = val.i64 ? 1 : 0;
            else if (r->cols[k].type == FS_BIGINT) vals[k].i64 = val.i64;
            else {
              char tmp[64];
              int ln = snprintf(tmp, sizeof(tmp), "%lld", (long long)val.i64);
              buf_reset(&r->fields[k]); buf_add(&r->fields[k], tmp, (size_t)ln);
              vals[k].str = r->fields[k].p; vals[k].str_len = ln;
            }
          } else if (val.kind == JV_FLOAT) {
            vals[k].is_null = 0;
            vals[k].f64 = val.f64;
            if (r->cols[k].type != FS_DOUBLE) {
              char tmp[64];
              int ln = snprintf(tmp, sizeof(tmp), "%.16g", val.f64);
              buf_reset(&r->fields[k]); buf_add(&r->fields[k], tmp, (size_t)ln);
              vals[k].str = r->fields[k].p; vals[k].str_len = ln;
            }
          } else if (val.kind == JV_STR) {
            const char *ps = val.str.p ? val.str.p : "";
            int pn = (int)val.str.n;
            int fr = fill_from_text(&r->cols[k], ps, pn, 0, &vals[k], &r->fields[k]);
            if (fr != FS_OK) rc = fr;
          }
        }
      }
      buf_free(&key);
      jval_free(&val);
    }
    buf_free(&lb);
    free(line);
    return rc;
  }
}

/* -------- parquet (minimal) -------- */

struct PCursor {
  const unsigned char *b;
  size_t n, i;
};

static uint64_t pq_varint(PCursor *c) {
  uint64_t n = 0; int shift = 0;
  while (c->i < c->n) {
    unsigned char x = c->b[c->i++];
    n |= (uint64_t)(x & 0x7f) << shift;
    if (x < 128) return n;
    shift += 7;
  }
  return n;
}
static int64_t pq_zig(PCursor *c) {
  uint64_t n = pq_varint(c);
  return (int64_t)((n >> 1) ^ (~(n & 1) + 1));
}

static void pq_skip(PCursor *c, int t);

static void pq_skip_struct(PCursor *c) {
  int fid = 0;
  while (c->i < c->n) {
    unsigned char b = c->b[c->i++];
    if (b == 0) return;
    int ty = b & 0x0f;
    int d = b >> 4;
    if (d == 0) fid = (int)pq_varint(c);
    else fid += d;
    (void)fid;
    pq_skip(c, ty);
  }
}

static void pq_skip(PCursor *c, int t) {
  if (t == 1 || t == 2) return;
  if (t == 3) { if (c->i < c->n) c->i++; return; }
  if (t == 4 || t == 5 || t == 6) { pq_zig(c); return; }
  if (t == 7) { c->i += 8; return; }
  if (t == 8) { uint64_t n = pq_varint(c); c->i += n; return; }
  if (t == 9 || t == 10) {
    unsigned char h = c->b[c->i++];
    int et = h & 0x0f; int sz = h >> 4;
    if (sz == 15) sz = (int)pq_varint(c);
    for (int i = 0; i < sz; ++i) pq_skip(c, et);
    return;
  }
  if (t == 11) {
    unsigned char h = c->b[c->i++];
    int kt = h & 0x0f; int vt = h >> 4;
    int sz = (int)pq_varint(c);
    for (int i = 0; i < sz; ++i) { pq_skip(c, kt); pq_skip(c, vt); }
    return;
  }
  if (t == 12) { pq_skip_struct(c); return; }
}

struct PqLeaf {
  char name[kMaxName];
  int type; /* parquet physical */
  int rep;
  int converted;
  int ordinal;
  int fs_type;
  int nullable;
};

struct PqChunk {
  int64_t data_off;
  int64_t idx_off;
  int64_t comp_size;
  int64_t unc_size;
  int codec;
  int64_t nvalues;
  int ptype;
};

struct PqState {
  unsigned char *data;
  size_t len;
  FILE *fp;
  unsigned char *page_window;
  PqLeaf leaves[kMaxCols];
  int nleaves;
  PqChunk chunks[8][kMaxCols];
  int nrg;
  int64_t rg_rows[8];
  int rg_i;
  int64_t row_in_rg;
  unsigned char *decoded;
  size_t decoded_cap;
  size_t decoded_n;
  size_t value_i;
  int page_ready;
  int def_max;
};

static int snappy_decompress(const unsigned char *src, size_t src_n, unsigned char *dst, size_t dst_cap, size_t *out_n) {
  size_t i = 0;
  uint64_t ulen = 0; int shift = 0;
  while (i < src_n) {
    unsigned char c = src[i++];
    ulen |= (uint64_t)(c & 0x7f) << shift;
    if (c < 128) break;
    shift += 7;
  }
  if (ulen > dst_cap) return -1;
  size_t o = 0;
  while (o < ulen && i < src_n) {
    unsigned char tag = src[i++];
    int t = tag & 3;
    if (t == 0) {
      size_t ln = tag >> 2;
      if (ln < 60) ln += 1;
      else {
        int nb = (int)ln - 59;
        ln = 0;
        for (int k = 0; k < nb; ++k) ln |= (size_t)src[i++] << (8 * k);
        ln += 1;
      }
      if (o + ln > ulen || i + ln > src_n) return -1;
      memcpy(dst + o, src + i, ln);
      o += ln; i += ln;
    } else if (t == 1) {
      size_t ln = ((tag >> 2) & 7) + 4;
      size_t off = ((size_t)(tag & 0xE0) << 3) | src[i++];
      if (off == 0 || off > o) return -1;
      for (size_t k = 0; k < ln; ++k) { unsigned char b = dst[o - off]; dst[o++] = b; }
    } else if (t == 2) {
      size_t ln = (tag >> 2) + 1;
      size_t off = (size_t)src[i] | ((size_t)src[i + 1] << 8);
      i += 2;
      if (off == 0 || off > o) return -1;
      for (size_t k = 0; k < ln; ++k) { unsigned char b = dst[o - off]; dst[o++] = b; }
    } else {
      size_t ln = (tag >> 2) + 1;
      size_t off = (size_t)src[i] | ((size_t)src[i + 1] << 8) | ((size_t)src[i + 2] << 16) | ((size_t)src[i + 3] << 24);
      i += 4;
      if (off == 0 || off > o) return -1;
      for (size_t k = 0; k < ln; ++k) { unsigned char b = dst[o - off]; dst[o++] = b; }
    }
  }
  *out_n = o;
  return o == ulen ? 0 : -1;
}

static int map_pq_type(int ptype, int converted, int rep) {
  int fs = FS_VARCHAR;
  /* BOOLEAN=0 INT32=1 INT64=2 INT96=3 FLOAT=4 DOUBLE=5 BYTE_ARRAY=6 FIXED=7 */
  if (ptype == 0) fs = FS_BOOL;
  else if (ptype == 1) fs = (converted == 6 /*DATE*/ || converted == 16) ? (converted == 6 ? FS_DATE : FS_BIGINT) : FS_BIGINT;
  else if (ptype == 2) fs = (converted == 10 || converted == 9 || converted == 8) ? FS_DATETIME : FS_BIGINT;
  else if (ptype == 3) fs = FS_DATETIME; /* int96 timestamp */
  else if (ptype == 4 || ptype == 5) fs = FS_DOUBLE;
  else fs = FS_VARCHAR;
  if (converted == 6 && ptype == 1) fs = FS_DATE;
  (void)rep;
  return fs;
}

static int parquet_parse_footer(const unsigned char *footer, size_t flen, PqState *st, char *err, int errcap) {
  PCursor c;
  c.b = footer;
  c.n = flen;
  c.i = 0;
  int fid = 0;
  int leaf = 0;
  while (c.i < c.n) {
    unsigned char b = c.b[c.i++];
    if (b == 0) break;
    int ty = b & 0x0f;
    int d = b >> 4;
    if (d == 0) fid = (int)pq_varint(&c);
    else fid += d;
    if (fid == 2 && ty == 9) {
      unsigned char h = c.b[c.i++];
      int et = h & 0x0f; int sz = h >> 4;
      if (sz == 15) sz = (int)pq_varint(&c);
      (void)et;
      for (int si = 0; si < sz; ++si) {
        char name[kMaxName]; int nlen = 0; int ptype = -1, rep = 0, conv = -1, children = 0;
        int sfid = 0;
        while (c.i < c.n) {
          unsigned char sb = c.b[c.i++];
          if (sb == 0) break;
          int st = sb & 0x0f; int sd = sb >> 4;
          if (sd == 0) sfid = (int)pq_varint(&c);
          else sfid += sd;
          if (sfid == 1 && st == 5) ptype = (int)pq_zig(&c);
          else if (sfid == 3 && st == 5) rep = (int)pq_zig(&c);
          else if (sfid == 4 && st == 8) {
            uint64_t ln = pq_varint(&c);
            nlen = (int)ln;
            if (nlen >= kMaxName) nlen = kMaxName - 1;
            memcpy(name, c.b + c.i, (size_t)nlen);
            name[nlen] = 0;
            c.i += ln;
          } else if (sfid == 5 && st == 5) children = (int)pq_zig(&c);
          else if (sfid == 6 && st == 5) conv = (int)pq_zig(&c);
          else pq_skip(&c, st);
        }
        if (children == 0 && ptype >= 0) {
          if (leaf >= kMaxCols) return FS_ERR;
          PqLeaf *L = &st->leaves[leaf];
          memset(L, 0, sizeof(*L));
          memcpy(L->name, name, (size_t)nlen);
          L->type = ptype;
          L->rep = rep;
          L->converted = conv;
          L->ordinal = leaf;
          L->fs_type = map_pq_type(ptype, conv, rep);
          L->nullable = rep != 0;
          leaf++;
        }
      }
    } else if (fid == 4 && ty == 9) {
      unsigned char h = c.b[c.i++];
      int sz = h >> 4;
      if (sz == 15) sz = (int)pq_varint(&c);
      st->nrg = sz;
      if (st->nrg > 8) { set_err(err, errcap, "too many row groups"); return FS_ERR; }
      for (int g = 0; g < sz; ++g) {
        int col_i = 0;
        int sfid = 0;
        while (c.i < c.n) {
          unsigned char sb = c.b[c.i++];
          if (sb == 0) break;
          int stt = sb & 0x0f; int sd = sb >> 4;
          if (sd == 0) sfid = (int)pq_varint(&c);
          else sfid += sd;
          if (sfid == 1 && stt == 9) {
            unsigned char hh = c.b[c.i++];
            int csz = hh >> 4;
            if (csz == 15) csz = (int)pq_varint(&c);
            for (int ci = 0; ci < csz; ++ci) {
              PqChunk ch;
              memset(&ch, 0, sizeof(ch));
              ch.data_off = -1; ch.idx_off = -1;
              int cf = 0;
              while (c.i < c.n) {
                unsigned char cb = c.b[c.i++];
                if (cb == 0) break;
                int ct = cb & 0x0f; int cd = cb >> 4;
                if (cd == 0) cf = (int)pq_varint(&c);
                else cf += cd;
                if (cf == 2 && ct == 6) ch.data_off = pq_zig(&c); /* file_offset fallback */
                else if (cf == 3 && ct == 12) {
                  int mf = 0;
                  while (c.i < c.n) {
                    unsigned char mb = c.b[c.i++];
                    if (mb == 0) break;
                    int mt = mb & 0x0f; int md = mb >> 4;
                    if (md == 0) mf = (int)pq_varint(&c);
                    else mf += md;
                    if (mf == 1 && mt == 5) ch.ptype = (int)pq_zig(&c);
                    else if (mf == 4 && mt == 5) ch.codec = (int)pq_zig(&c);
                    else if (mf == 5 && mt == 6) ch.nvalues = pq_zig(&c);
                    else if (mf == 6 && mt == 6) ch.unc_size = pq_zig(&c);
                    else if (mf == 7 && mt == 6) ch.comp_size = pq_zig(&c);
                    else if (mf == 8 && mt == 6) ch.data_off = pq_zig(&c);
                    else if (mf == 9 && mt == 6) ch.idx_off = pq_zig(&c);
                    else pq_skip(&c, mt);
                  }
                } else pq_skip(&c, ct);
              }
              if (col_i < kMaxCols) st->chunks[g][col_i] = ch;
              col_i++;
            }
          } else if (sfid == 3 && stt == 6) st->rg_rows[g] = pq_zig(&c);
          else pq_skip(&c, stt);
        }
      }
    } else pq_skip(&c, ty);
  }
  st->nleaves = leaf;
  return leaf > 0 ? FS_OK : FS_ERR;
}

static int read_parquet_footer(FILE *fp, long sz, unsigned char **footer, size_t *flen_out, char *err, int errcap) {
  unsigned char magic[4];
  unsigned char tail[8];
  if (sz < 12) { set_err(err, errcap, "not a parquet file"); return FS_ERR; }
  if (fseek(fp, 0, SEEK_SET) != 0 || fread(magic, 1, 4, fp) != 4 || memcmp(magic, "PAR1", 4) != 0) {
    set_err(err, errcap, "not a parquet file");
    return FS_ERR;
  }
  if (fseek(fp, -8, SEEK_END) != 0 || fread(tail, 1, 8, fp) != 8 || memcmp(tail + 4, "PAR1", 4) != 0) {
    set_err(err, errcap, "not a parquet file");
    return FS_ERR;
  }
  uint32_t flen = (uint32_t)tail[0] | ((uint32_t)tail[1] << 8) |
                  ((uint32_t)tail[2] << 16) | ((uint32_t)tail[3] << 24);
  if ((long)flen + 8 > sz || flen > 64u * 1024u * 1024u) {
    set_err(err, errcap, "bad parquet footer");
    return FS_ERR;
  }
  if (fseek(fp, sz - 8 - (long)flen, SEEK_SET) != 0) return FS_ERR;
  unsigned char *buf = (unsigned char *)malloc(flen ? (size_t)flen : 1);
  if (!buf || fread(buf, 1, (size_t)flen, fp) != (size_t)flen) {
    free(buf);
    return FS_ERR;
  }
  *footer = buf;
  *flen_out = (size_t)flen;
  return FS_OK;
}

static int rle_decode_levels(const unsigned char *p, size_t n, int bit_width, int count, unsigned char *out) {
  if (bit_width == 0) { memset(out, 0, (size_t)count); return 0; }
  if (n < 4) return -1;
  uint32_t len = (uint32_t)p[0] | ((uint32_t)p[1] << 8) | ((uint32_t)p[2] << 16) | ((uint32_t)p[3] << 24);
  if (4 + len > n) return -1;
  const unsigned char *b = p + 4;
  size_t i = 0, o = 0;
  while (i < len && (int)o < count) {
    unsigned char h = b[i++];
    if (h & 1) {
      int run = (h >> 1);
      if (i >= len) return -1;
      unsigned char v = b[i++]; /* only bit_width<=8 */
      for (int k = 0; k < run && (int)o < count; ++k) out[o++] = v;
    } else {
      int groups = h >> 1;
      int bytes = groups * bit_width; /* bit packed 8 values per group, bit_width bytes? */
      /* 8 values * bit_width bits */
      int nbytes = (groups * bit_width);
      (void)bytes;
      if (i + (size_t)nbytes > len) return -1;
      for (int g = 0; g < groups; ++g) {
        uint64_t pack = 0;
        for (int bi = 0; bi < bit_width; ++bi) pack |= (uint64_t)b[i++] << (8 * bi);
        for (int k = 0; k < 8 && (int)o < count; ++k) {
          out[o++] = (unsigned char)((pack >> (k * bit_width)) & ((1u << bit_width) - 1));
        }
      }
    }
  }
  return 0;
}

static int parquet_decode_chunk(PqState *st, const PqChunk *ch, int ptype, unsigned char **values, int *nvalues, unsigned char **defs, char *err, int errcap) {
  int64_t off = ch->idx_off >= 0 ? ch->idx_off : ch->data_off;
  const unsigned char *base = NULL;
  size_t base_n = 0;
  free(st->page_window);
  st->page_window = NULL;
  if (off < 0 || (size_t)off >= st->len) { set_err(err, errcap, "bad parquet offset"); return FS_ERR; }
  if (st->data) {
    base = st->data + off;
    base_n = st->len - (size_t)off;
  } else if (st->fp) {
    size_t remain = st->len - (size_t)off;
    size_t want = remain;
    if (ch->comp_size > 0) {
      size_t hint = (size_t)ch->comp_size + 4096;
      if (hint < want) want = hint;
    }
    if (want > 32u * 1024u * 1024u) want = 32u * 1024u * 1024u;
    if (want > remain) want = remain;
    st->page_window = (unsigned char *)malloc(want ? want : 1);
    if (!st->page_window) return FS_ERR;
    if (fseek(st->fp, (long)off, SEEK_SET) != 0 || fread(st->page_window, 1, want, st->fp) != want) {
      set_err(err, errcap, "short parquet read");
      return FS_ERR;
    }
    base = st->page_window;
    base_n = want;
  } else {
    set_err(err, errcap, "bad parquet offset");
    return FS_ERR;
  }
  PCursor c;
  c.b = base;
  c.n = base_n;
  c.i = 0;
  int page_type = -1, unc = 0, comp = 0, num_values = 0, def_len = 0, rep_len = 0, is_comp = 1, enc = 0;
  int fid = 0;
  int is_v2 = 0;
  while (c.i < c.n) {
    unsigned char b = c.b[c.i++];
    if (b == 0) break;
    int ty = b & 0x0f; int d = b >> 4;
    if (d == 0) fid = (int)pq_varint(&c);
    else fid += d;
    if (fid == 1 && ty == 5) page_type = (int)pq_zig(&c);
    else if (fid == 2 && ty == 5) unc = (int)pq_zig(&c);
    else if (fid == 3 && ty == 5) comp = (int)pq_zig(&c);
    else if ((fid == 7 || fid == 8 || fid == 4) && ty == 12) {
      int sf = 0;
      int looks_v2 = 0;
      size_t struct_start = c.i;
      /* peek not easy; parse known v2/v1 fields */
      while (c.i < c.n) {
        unsigned char sb = c.b[c.i++];
        if (sb == 0) break;
        int st = sb & 0x0f; int sd = sb >> 4;
        if (sd == 0) sf = (int)pq_varint(&c);
        else sf += sd;
        if (sf == 1 && st == 5) { num_values = (int)pq_zig(&c); }
        else if (sf == 2 && st == 5) pq_zig(&c);
        else if (sf == 3 && st == 5) pq_zig(&c);
        else if (sf == 4 && st == 5) enc = (int)pq_zig(&c);
        else if (sf == 5 && st == 5) { def_len = (int)pq_zig(&c); looks_v2 = 1; }
        else if (sf == 6 && st == 5) { rep_len = (int)pq_zig(&c); looks_v2 = 1; }
        else if (sf == 7 && (st == 1 || st == 2)) is_comp = st == 1;
        else pq_skip(&c, st);
      }
      if (page_type == 3 || looks_v2) is_v2 = 1;
      (void)struct_start;
    } else pq_skip(&c, ty);
  }
  (void)enc;
  if (page_type < 0) { set_err(err, errcap, "bad parquet page"); return FS_ERR; }
  const unsigned char *body = c.b + c.i;
  if (is_v2 || page_type == 3) {
    if (def_len < 0 || rep_len < 0) return FS_ERR;
    const unsigned char *levels = body;
    const unsigned char *payload = body + rep_len + def_len;
    int payload_len = comp - rep_len - def_len;
    if (payload_len < 0) payload_len = 0;
    int out_len = unc - rep_len - def_len;
    if (out_len < 0) out_len = unc;
    if ((size_t)out_len > st->decoded_cap) {
      unsigned char *np = (unsigned char *)realloc(st->decoded, (size_t)out_len + 8);
      if (!np) return FS_ERR;
      st->decoded = np;
      st->decoded_cap = (size_t)out_len + 8;
    }
    if (is_comp && ch->codec == 1) {
      size_t got = 0;
      if (snappy_decompress(payload, (size_t)payload_len, st->decoded, st->decoded_cap, &got)) {
        set_err(err, errcap, "snappy decompress failed");
        return FS_ERR;
      }
      st->decoded_n = got;
    } else if (!is_comp || ch->codec == 0) {
      memcpy(st->decoded, payload, (size_t)payload_len);
      st->decoded_n = (size_t)payload_len;
    } else {
      set_err(err, errcap, "unsupported parquet compression");
      return FS_UNSUPPORTED;
    }
    {
      unsigned char *copy = (unsigned char *)malloc(st->decoded_n + 1);
      if (!copy) return FS_ERR;
      if (st->decoded_n && st->decoded) memcpy(copy, st->decoded, st->decoded_n);
      *values = copy;
    }
    *nvalues = num_values;
    if (def_len > 0) {
      unsigned char *dlev = (unsigned char *)malloc((size_t)num_values + 8);
      if (!dlev) return FS_ERR;
      int bw = 1;
      if (rle_decode_levels(levels + rep_len, (size_t)def_len, bw, num_values, dlev)) {
        /* levels may be raw bit-packed without 4-byte length; treat all present if decode fails and num_nulls was 0 */
        memset(dlev, 1, (size_t)num_values);
      }
      *defs = dlev;
    } else {
      *defs = NULL;
    }
    (void)ptype;
    return FS_OK;
  }
  set_err(err, errcap, "unsupported parquet page");
  return FS_UNSUPPORTED;
}

static int parquet_infer(const char *path, FsCol *cols, int cap, int *ncol, char *err, int errcap) {
  FILE *fp = fopen(path, "rb");
  if (!fp) { set_err(err, errcap, "cannot open parquet"); return FS_NOT_FOUND; }
  if (fseek(fp, 0, SEEK_END) != 0) { fclose(fp); return FS_ERR; }
  long sz = ftell(fp);
  if (sz < 0 || sz > 512L * 1024 * 1024) { fclose(fp); set_err(err, errcap, "parquet too large"); return FS_ERR; }
  unsigned char *footer = NULL;
  size_t flen = 0;
  int rc = read_parquet_footer(fp, sz, &footer, &flen, err, errcap);
  fclose(fp);
  if (rc != FS_OK) return rc;
  PqState st;
  memset(&st, 0, sizeof(st));
  rc = parquet_parse_footer(footer, flen, &st, err, errcap);
  free(footer);
  if (rc == FS_OK) {
    if (st.nleaves > cap) rc = FS_ERR;
    else {
      for (int i = 0; i < st.nleaves; ++i) {
        memset(&cols[i], 0, sizeof(cols[i]));
        memcpy(cols[i].name, st.leaves[i].name, strlen(st.leaves[i].name));
        cols[i].name_len = (int)strlen(st.leaves[i].name);
        cols[i].type = st.leaves[i].fs_type;
        cols[i].nullable = st.leaves[i].nullable;
        cols[i].ordinal = i;
      }
      *ncol = st.nleaves;
    }
  }
  return rc;
}

/* Patch fs_infer for parquet by wrapping. The earlier fs_infer returns unsupported.
   Provide fs_infer_ex used by tests via a second symbol. */

int fs_infer_any(int kind, const char *path, const FsOptions *opt, FsCol *cols, int cap, int *ncol,
                 char *err, int errcap) {
  if (kind == FS_KIND_PARQUET) return parquet_infer(path, cols, cap, ncol, err, errcap);
  return fs_infer(kind, path, opt, cols, cap, ncol, err, errcap);
}

static int emit_pq_value(int fs_type, int ptype, const unsigned char *p, FsValue *out, Buf *store) {
  out->is_null = 0;
  out->type = fs_type;
  out->str = NULL;
  out->str_len = 0;
  if (ptype == 1) {
    int32_t v;
    memcpy(&v, p, 4);
    if (fs_type == FS_DATE) {
      /* days since unix epoch */
      time_t sec = (time_t)v * 86400;
      struct tm tm;
#if defined(_WIN32)
      gmtime_s(&tm, &sec);
#else
      gmtime_r(&sec, &tm);
#endif
      char tmp[80];
      snprintf(tmp, sizeof(tmp), "%04d-%02d-%02d", tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday);
      buf_reset(store);
      buf_add(store, tmp, strlen(tmp));
      out->str = store->p;
      out->str_len = (int)strlen(tmp);
    } else if (fs_type == FS_DOUBLE) out->f64 = (double)v;
    else { out->type = FS_BIGINT; out->i64 = v; }
    return FS_OK;
  }
  if (ptype == 2) {
    int64_t v;
    memcpy(&v, p, 8);
    if (fs_type == FS_DOUBLE) out->f64 = (double)v;
    else if (fs_type == FS_DATETIME) {
      /* microseconds or millis heuristic: large values are micros */
      int64_t us = v > 100000000000000LL ? v : (v > 100000000000LL ? v * 1000 : v * 1000000);
      time_t sec = (time_t)(us / 1000000);
      struct tm tm;
#if defined(_WIN32)
      gmtime_s(&tm, &sec);
#else
      gmtime_r(&sec, &tm);
#endif
      char tmp[80];
      snprintf(tmp, sizeof(tmp), "%04d-%02d-%02d %02d:%02d:%02d",
               tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday, tm.tm_hour, tm.tm_min, tm.tm_sec);
      buf_reset(store); buf_add(store, tmp, strlen(tmp));
      out->str = store->p; out->str_len = (int)strlen(tmp);
    } else { out->type = FS_BIGINT; out->i64 = v; }
    return FS_OK;
  }
  if (ptype == 0) { out->i64 = p[0] ? 1 : 0; out->type = FS_BOOL; return FS_OK; }
  if (ptype == 4) { float f; memcpy(&f, p, 4); out->f64 = f; out->type = FS_DOUBLE; return FS_OK; }
  if (ptype == 5) { double d; memcpy(&d, p, 8); out->f64 = d; out->type = FS_DOUBLE; return FS_OK; }
  out->is_null = 1;
  return FS_OK;
}

static int ptype_width(int ptype) {
  if (ptype == 0) return 1;
  if (ptype == 1 || ptype == 4) return 4;
  if (ptype == 2 || ptype == 5 || ptype == 3) return ptype == 3 ? 12 : 8;
  return -1;
}

struct PqScan {
  PqState st;
  unsigned char *vals[kMaxCols];
  unsigned char *defs[kMaxCols];
  int nvalues[kMaxCols];
  int widths[kMaxCols];
  int ptypes[kMaxCols];
  int fs_types[kMaxCols];
  int proj[kMaxCols];
  int nproj;
  int64_t row;
  int64_t rows;
};

static void parquet_scan_free(PqScan *sc) {
  if (!sc) return;
  if (sc->st.fp) fclose(sc->st.fp);
  free(sc->st.data);
  free(sc->st.decoded);
  free(sc->st.page_window);
  for (int i = 0; i < kMaxCols; ++i) {
    free(sc->vals[i]);
    free(sc->defs[i]);
  }
  free(sc);
}

static int parquet_open_scan(const char *path, const FsCol *cols, int ncol, PqScan **out, char *err, int errcap) {
  FILE *fp = fopen(path, "rb");
  if (!fp) return FS_NOT_FOUND;
  if (fseek(fp, 0, SEEK_END) != 0) { fclose(fp); return FS_ERR; }
  long sz = ftell(fp);
  if (sz < 0 || sz > 512L * 1024 * 1024) { fclose(fp); set_err(err, errcap, "parquet too large"); return FS_ERR; }
  unsigned char *footer = NULL;
  size_t flen = 0;
  int rc = read_parquet_footer(fp, sz, &footer, &flen, err, errcap);
  if (rc != FS_OK) { fclose(fp); return rc; }
  PqScan *sc = (PqScan *)calloc(1, sizeof(PqScan));
  if (!sc) { free(footer); fclose(fp); return FS_ERR; }
  rc = parquet_parse_footer(footer, flen, &sc->st, err, errcap);
  free(footer);
  if (rc != FS_OK) { sc->st.fp = NULL; parquet_scan_free(sc); fclose(fp); return rc; }
  sc->st.fp = fp;
  sc->st.len = (size_t)sz;
  sc->st.data = NULL;
  sc->rows = 0;
  for (int g = 0; g < sc->st.nrg; ++g) sc->rows += sc->st.rg_rows[g];
  sc->nproj = ncol;
  for (int i = 0; i < ncol; ++i) {
    int found = -1;
    for (int j = 0; j < sc->st.nleaves; ++j) {
      if ((int)strlen(sc->st.leaves[j].name) == cols[i].name_len &&
          memcmp(sc->st.leaves[j].name, cols[i].name, (size_t)cols[i].name_len) == 0) { found = j; break; }
    }
    if (found < 0 && cols[i].ordinal >= 0 && cols[i].ordinal < sc->st.nleaves) found = cols[i].ordinal;
    sc->proj[i] = found;
    if (found >= 0 && sc->st.nrg > 0) {
      unsigned char *defs = NULL;
      int nv = 0;
      rc = parquet_decode_chunk(&sc->st, &sc->st.chunks[0][found], sc->st.leaves[found].type,
                                &sc->vals[i], &nv, &defs, err, errcap);
      if (rc != FS_OK) { parquet_scan_free(sc); return rc; }
      sc->defs[i] = defs;
      sc->nvalues[i] = nv;
      sc->ptypes[i] = sc->st.leaves[found].type;
      sc->fs_types[i] = cols[i].type ? cols[i].type : sc->st.leaves[found].fs_type;
      sc->widths[i] = ptype_width(sc->ptypes[i]);
    } else {
      sc->vals[i] = NULL;
      sc->nvalues[i] = 0;
      sc->widths[i] = 4;
      sc->ptypes[i] = 1;
      sc->fs_types[i] = cols[i].type;
    }
  }
  *out = sc;
  return FS_OK;
}

static int parquet_next(PqScan *sc, FsValue *vals, int ncol, Buf *stores) {
  if (sc->row >= sc->rows) return FS_END;
  for (int i = 0; i < ncol; ++i) {
    vals[i].is_null = 1;
    vals[i].type = sc->fs_types[i];
    vals[i].str = NULL;
    vals[i].str_len = 0;
    if (!sc->vals[i] || sc->widths[i] < 0) continue;
    int defined = 1;
    if (sc->defs[i]) defined = sc->defs[i][sc->row] != 0;
    if (!defined) continue;
    /* count present values before this row to index into plain values if nulls exist */
    int present_before = 0;
    if (sc->defs[i]) {
      for (int64_t r = 0; r < sc->row; ++r) if (sc->defs[i][r]) present_before++;
    } else present_before = (int)sc->row;
    const unsigned char *p = sc->vals[i] + (size_t)present_before * (size_t)sc->widths[i];
    emit_pq_value(sc->fs_types[i], sc->ptypes[i], p, &vals[i], &stores[i]);
  }
  sc->row++;
  return FS_OK;
}

/* glob */
#if !defined(_WIN32)
static int glob_collect(const char *pattern, char ***out, int *nout, char *err, int errcap) {
  const char *slash = strrchr(pattern, '/');
  char dir[4096];
  const char *pat = pattern;
  if (slash) {
    size_t dl = (size_t)(slash - pattern);
    if (dl == 0) { strcpy(dir, "/"); }
    else { if (dl >= sizeof(dir)) return FS_ERR; memcpy(dir, pattern, dl); dir[dl] = 0; }
    pat = slash + 1;
  } else strcpy(dir, ".");
  DIR *d = opendir(dir);
  if (!d) { set_err(err, errcap, "cannot open directory"); return FS_NOT_FOUND; }
  int cap = 16, n = 0;
  char **arr = (char **)malloc(sizeof(char *) * (size_t)cap);
  struct dirent *de;
  while ((de = readdir(d))) {
    if (strcmp(de->d_name, ".") == 0 || strcmp(de->d_name, "..") == 0) continue;
    if (fnmatch(pat, de->d_name, 0) != 0) continue;
    char full[4096];
    snprintf(full, sizeof(full), "%s/%s", dir, de->d_name);
    struct stat st;
    if (stat(full, &st) != 0) continue;
    if (S_ISDIR(st.st_mode)) continue;
    if (n >= cap) {
      cap *= 2;
      char **na = (char **)realloc(arr, sizeof(char *) * (size_t)cap);
      if (!na) { closedir(d); return FS_ERR; }
      arr = na;
    }
    arr[n++] = strdup(full);
  }
  closedir(d);
  *out = arr;
  *nout = n;
  return FS_OK;
}
#endif

int fs_open(int kind, const char *path, const FsOptions *opt, const FsCol *cols, int ncol,
            FsReader **out, char *err, int errcap) {
  if (!opt || !out) return FS_ERR;
  int ck = fs_check_local_path(path, err, errcap);
  if (ck) return ck;
  if (kind == FS_KIND_GLOB) {
#if defined(_WIN32)
    set_err(err, errcap, "glob is not supported on this platform");
    return FS_UNSUPPORTED;
#else
    FsReaderImpl *r = (FsReaderImpl *)calloc(1, sizeof(FsReaderImpl));
    if (!r) return FS_ERR;
    r->kind = FS_KIND_GLOB;
    r->opt = *opt;
    r->ncol = 1;
    memcpy(r->cols[0].name, "file", 4);
    r->cols[0].name_len = 4;
    r->cols[0].type = FS_VARCHAR;
    int rc = glob_collect(path, &r->gpaths, &r->ng, err, errcap);
    if (rc != FS_OK) { free(r); return rc; }
    *out = (FsReader *)r;
    return FS_OK;
#endif
  }
  if (kind == FS_KIND_PARQUET) {
    FsReaderImpl *r = (FsReaderImpl *)calloc(1, sizeof(FsReaderImpl));
    if (!r) return FS_ERR;
    r->kind = kind;
    r->opt = *opt;
    r->ncol = ncol;
    for (int i = 0; i < ncol && i < kMaxCols; ++i) r->cols[i] = cols[i];
    snprintf(r->path, sizeof(r->path), "%s", path);
    stat_path(path, &r->sig);
    PqScan *sc = NULL;
    int rc = parquet_open_scan(path, cols, ncol, &sc, err, errcap);
    if (rc != FS_OK) { free(r); return rc; }
    r->pq = sc;
    *out = (FsReader *)r;
    return FS_OK;
  }
  FsReaderImpl *r = (FsReaderImpl *)calloc(1, sizeof(FsReaderImpl));
  if (!r) return FS_ERR;
  int rc = open_common(r, path, opt);
  if (rc != FS_OK) { free(r); set_err(err, errcap, "cannot open file"); return rc; }
  r->kind = kind;
  r->ncol = ncol;
  for (int i = 0; i < ncol && i < kMaxCols; ++i) r->cols[i] = cols[i];
  *out = (FsReader *)r;
  return FS_OK;
}

int fs_next(FsReader *reader, FsValue *vals, int ncol) {
  FsReaderImpl *r = as_impl(reader);
  if (!r) return FS_ERR;
  if (r->kind == FS_KIND_CSV) return csv_next_impl(r, vals, ncol);
  if (r->kind == FS_KIND_JSONL) return jsonl_next_impl(r, vals, ncol);
  if (r->kind == FS_KIND_PARQUET) {
    PqScan *sc = (PqScan *)r->pq;
    if (!r->fields_ready) { memset(r->fields, 0, sizeof(r->fields)); r->fields_ready = 1; }
    return parquet_next(sc, vals, ncol, r->fields);
  }
  if (r->kind == FS_KIND_GLOB) {
    if (r->gi >= r->ng) return FS_END;
    vals[0].is_null = 0;
    vals[0].type = FS_VARCHAR;
    vals[0].str = r->gpaths[r->gi];
    vals[0].str_len = (int)strlen(r->gpaths[r->gi]);
    r->gi++;
    return FS_OK;
  }
  return FS_UNSUPPORTED;
}

void fs_close(FsReader *reader) {
  FsReaderImpl *r = as_impl(reader);
  if (!r) return;
  if (r->fp) fclose(r->fp);
  for (int i = 0; i < 512; ++i) buf_free(&r->fields[i]);
  if (r->gpaths) {
    for (int i = 0; i < r->ng; ++i) free(r->gpaths[i]);
    free(r->gpaths);
  }
  if (r->pq) {
    parquet_scan_free((PqScan *)r->pq);
  }
  free(r);
}
