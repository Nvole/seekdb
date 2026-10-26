#pragma once

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

enum FsKind {
  FS_KIND_NONE = 0,
  FS_KIND_CSV = 1,
  FS_KIND_JSONL = 2,
  FS_KIND_PARQUET = 3,
  FS_KIND_GLOB = 4
};

enum FsType {
  FS_NULL = 0,
  FS_BIGINT = 1,
  FS_DOUBLE = 2,
  FS_BOOL = 3,
  FS_VARCHAR = 4,
  FS_DATE = 5,
  FS_DATETIME = 6
};

enum FsStatus {
  FS_OK = 0,
  FS_END = 1,
  FS_ERR = 2,
  FS_UNSUPPORTED = 3,
  FS_BAD_ROW = 4,
  FS_CHANGED = 5,
  FS_NESTED = 6,
  FS_NOT_FOUND = 7,
  FS_EXISTS = 8
};

typedef struct FsCol {
  char name[256];
  int name_len;
  int type;
  int nullable;
  int ordinal;
} FsCol;

typedef struct FsValue {
  int is_null;
  int type;
  int64_t i64;
  double f64;
  const char *str;
  int str_len;
} FsValue;

typedef struct FsOptions {
  int header;
  char delim;
  char quote;
  char escape;
  const char *nullstr;
  int nullstr_len;
  int sample_size;
  const char *json_format;
  int json_format_len;
  int64_t expect_size;
  int64_t expect_mtime_sec;
  int64_t expect_mtime_nsec;
} FsOptions;

typedef struct FsReader FsReader;

void fs_options_init(FsOptions *opt);
int fs_stat_file(const char *path, int64_t *size, int64_t *mtime_sec,
                 int64_t *mtime_nsec, uint64_t *ino, char *err, int errcap);
int fs_check_local_path(const char *path, char *err, int errcap);

int fs_infer(int kind, const char *path, const FsOptions *opt,
             FsCol *cols, int cap, int *ncol, char *err, int errcap);
int fs_infer_any(int kind, const char *path, const FsOptions *opt,
                 FsCol *cols, int cap, int *ncol, char *err, int errcap);
int fs_open(int kind, const char *path, const FsOptions *opt,
            const FsCol *cols, int ncol, FsReader **out, char *err, int errcap);
int fs_next(FsReader *reader, FsValue *vals, int ncol);
const char *fs_error(const FsReader *reader);
int fs_lineno(const FsReader *reader);
void fs_close(FsReader *reader);

const char *fs_type_name(int type);

#ifdef __cplusplus
}
#endif
