/*
 * Copyright (c) 2025 OceanBase.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

#define USING_LOG_PREFIX SQL_ENG

#include "sql/engine/expr/ob_expr_ai/ob_ai_column.h"

#include <pthread.h>
#include <unistd.h>
#include <atomic>
#include <algorithm>
#include <cctype>
#include <cerrno>
#include <cstring>

#include "lib/oblog/ob_log.h"
#include "lib/string/ob_sql_string.h"
#include "lib/time/ob_time_utility.h"
#include "lib/allocator/ob_malloc.h"
#include "share/ob_errno.h"
#include "share/schema/ob_column_schema.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/schema/ob_table_schema.h"
#include "observer/ob_server_struct.h"
#include "sql/engine/dml/ob_dml_ctx_define.h"
#include "sql/engine/dml/ob_table_modify_op.h"
#include "sql/engine/expr/ob_expr.h"
#include "sql/engine/expr/ob_expr_ai/ob_ai_func.h"
#include "sql/engine/expr/ob_expr_ai/ob_ai_func_utils.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/ob_sql_context.h"
#include "sql/parser/ob_item_type.h"
#include "sql/resolver/ddl/ob_alter_table_resolver.h"
#include "sql/resolver/ddl/ob_ddl_resolver.h"
#include "sql/resolver/dml/ob_del_upd_resolver.h"
#include "sql/resolver/dml/ob_del_upd_stmt.h"
#include "sql/resolver/dml/ob_dml_stmt.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "sql/session/ob_sql_session_info.h"
#include "query/ai/ob_ai_endpoint_resolver.h"
#include "share/rc/ob_server_runtime.h"

namespace oceanbase {
namespace sql {

using namespace common;
using namespace share;
using namespace share::schema;

namespace {

constexpr int64_t AI_RETRY_DELAY_US = 400 * 1000;

struct AiDef {
  ObString database_name_;
  ObString table_name_;
  ObString column_name_;
  ObString state_name_;
  ObString request_name_;
  ObString spec_name_;
  ObString model_name_;
  ObString input_expr_;
  ObString normalized_expr_;
  ObString type_prompt_;
  ObString depend_names_;
  uint64_t table_id_;
  uint64_t column_id_;
  uint64_t state_column_id_;
  uint64_t request_column_id_;
  uint64_t spec_column_id_;
  int64_t max_retries_;
  int64_t def_version_;
  int64_t vector_dim_;
  int func_type_;
  bool flushed_;
};

struct AiTask {
  int64_t task_id_;
  uint64_t table_id_;
  uint64_t column_id_;
  int64_t retry_count_;
  int64_t max_retries_;
  int64_t def_version_;
  int state_;
  ObString request_id_;
  ObString rowkey_predicate_;
  ObString payload_;
  ObString database_name_;
  ObString table_name_;
};

class AiCatalog {
public:
  static AiCatalog &instance() {
    static AiCatalog cat;
    return cat;
  }
  int copy_str(const ObString &in, ObString &out) {
    int ret = OB_SUCCESS;
    if (in.empty()) {
      out.reset();
    } else {
      char *buf = static_cast<char *>(allocator_.alloc(in.length()));
      if (OB_ISNULL(buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else {
        MEMCPY(buf, in.ptr(), in.length());
        out.assign_ptr(buf, in.length());
      }
    }
    return ret;
  }
  lib::ObMutex &lock() { return lock_; }
  ObIArray<AiDef> &defs() { return defs_; }
  ObIArray<AiDef> &pending() { return pending_; }
  ObArenaAllocator &allocator() { return allocator_; }
  AiDef *find_mutable(uint64_t table_id, uint64_t column_id) {
    for (int64_t i = 0; i < defs_.count(); ++i) {
      if (defs_.at(i).table_id_ == table_id && defs_.at(i).column_id_ == column_id) {
        return &defs_.at(i);
      }
    }
    return nullptr;
  }
  const AiDef *find_by_name(const ObString &db, const ObString &table, const ObString &column) const {
    for (int64_t i = 0; i < defs_.count(); ++i) {
      const AiDef &d = defs_.at(i);
      if (0 == d.database_name_.case_compare(db) &&
          0 == d.table_name_.case_compare(table) &&
          0 == d.column_name_.case_compare(column)) {
        return &d;
      }
    }
    return nullptr;
  }
private:
  AiCatalog() : allocator_(ObModIds::OB_SQL_EXPR), lock_(), defs_(), pending_() {}
  ObArenaAllocator allocator_;
  lib::ObMutex lock_;
  ObArray<AiDef> defs_;
  ObArray<AiDef> pending_;
};

thread_local int g_ai_depth = 0;
thread_local bool g_retry_pending = false;
thread_local uint64_t g_retry_session = 0;
thread_local char g_retry_db[128];
thread_local char g_retry_table[128];
thread_local char g_retry_column[128];
thread_local char g_retry_where[4096];

struct DepthGuard {
  DepthGuard() { ++g_ai_depth; }
  ~DepthGuard() { --g_ai_depth; }
};

int sql_proxy_write(const ObSqlString &sql, int64_t &affected) {
  int ret = OB_SUCCESS;
  affected = 0;
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(GCTX.sql_proxy_->write(sql.ptr(), 0, affected))) {
    LOG_WARN("ai column sql write failed", K(ret), K(sql));
  }
  return ret;
}

int sql_proxy_read(const ObSqlString &sql, ObMySQLProxy::MySQLResult &res) {
  int ret = OB_SUCCESS;
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(GCTX.sql_proxy_->read(res, sql.ptr(), 0))) {
    LOG_WARN("ai column sql read failed", K(ret), K(sql));
  }
  return ret;
}

int nested_write(ObTableModifyOp &op, const ObSqlString &sql) {
  int ret = OB_SUCCESS;
  DepthGuard guard;
  if (OB_FAIL(op.open_inner_conn())) {
  } else if (OB_FAIL(op.begin_nested_session(false))) {
  } else {
    ret = op.execute_write(sql.ptr());
    int end_ret = op.end_nested_session();
    if (OB_SUCC(ret)) {
      ret = end_ret;
    }
  }
  return ret;
}

void append_escaped(ObSqlString &sql, const ObString &text) {
  for (int64_t i = 0; i < text.length(); ++i) {
    char c = text.ptr()[i];
    if (c == '\'' || c == '\\') {
      sql.append("\\");
    }
    sql.append(&c, 1);
  }
}

int quote_literal(ObSqlString &sql, const ObString &text) {
  int ret = OB_SUCCESS;
  if (OB_FAIL(sql.append("'"))) {
  } else {
    append_escaped(sql, text);
    ret = sql.append("'");
  }
  return ret;
}

bool is_ident_char(char c) {
  return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
         (c >= '0' && c <= '9') || c == '_' || c == '$';
}

int64_t next_task_id() {
  static std::atomic<int64_t> seq(1);
  return (ObTimeUtility::current_time() << 12) ^ seq.fetch_add(1);
}

ObString make_request_id(char *buf, int64_t buf_len, int64_t &out_len) {
  uint64_t v = static_cast<uint64_t>(next_task_id());
  out_len = snprintf(buf, buf_len, "r%llx", static_cast<unsigned long long>(v));
  return ObString(out_len, buf);
}

const char *type_prompt_for(const ObColumnSchemaV2 &column) {
  if (column.is_json()) {
    return "Return exactly one valid JSON value";
  } else if (ob_is_int_tc(column.get_data_type())) {
    return "Return exactly one base-10 integer";
  }
  return "";
}

ObString nth_call_arg(const ObString &sql, int nth) {
  int depth = 0;
  bool in_sq = false;
  int arg = 0;
  int64_t start = -1;
  for (int64_t i = 0; i < sql.length(); ++i) {
    char c = sql.ptr()[i];
    if (in_sq) {
      if (c == '\\' && i + 1 < sql.length()) {
        ++i;
      } else if (c == '\'') {
        in_sq = false;
      }
      continue;
    }
    if (c == '\'') {
      in_sq = true;
      continue;
    }
    if (c == '(') {
      if (depth == 0) {
        start = i + 1;
      }
      ++depth;
    } else if (c == ')') {
      --depth;
      if (depth == 0 && arg == nth) {
        int64_t end = i;
        while (start < end && isspace(sql.ptr()[start])) {
          ++start;
        }
        while (end > start && isspace(sql.ptr()[end - 1])) {
          --end;
        }
        return ObString(end - start, sql.ptr() + start);
      }
    } else if (c == ',' && depth == 1) {
      if (arg == nth) {
        int64_t end = i;
        while (start < end && isspace(sql.ptr()[start])) {
          ++start;
        }
        while (end > start && isspace(sql.ptr()[end - 1])) {
          --end;
        }
        return ObString(end - start, sql.ptr() + start);
      }
      ++arg;
      start = i + 1;
    }
  }
  return ObString();
}

int walk_depend_names(ParseNode *node, ObSqlString &names) {
  int ret = OB_SUCCESS;
  if (OB_ISNULL(node)) {
  } else if (T_COLUMN_REF == node->type_ && node->num_child_ >= 3 &&
             OB_NOT_NULL(node->children_[2]) &&
             OB_NOT_NULL(node->children_[2]->str_value_)) {
    ObString name(node->children_[2]->str_len_, node->children_[2]->str_value_);
    if (!names.empty() && OB_FAIL(names.append(","))) {
    } else if (OB_FAIL(names.append(name))) {
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < node->num_child_; ++i) {
      if (OB_FAIL(walk_depend_names(node->children_[i], names))) {
      }
    }
  }
  return ret;
}

bool column_is_ai(const ObTableSchema &schema, uint64_t column_id) {
  lib::ObMutexGuard guard(AiCatalog::instance().lock());
  return nullptr != AiCatalog::instance().find_mutable(schema.get_table_id(), column_id);
}

int fill_varchar_col(ObColumnSchemaV2 &col, uint64_t id, const ObString &name) {
  int ret = OB_SUCCESS;
  col.reset();
  col.set_column_id(id);
  col.set_data_type(ObVarcharType);
  col.set_data_length(256);
  col.set_charset_type(CHARSET_UTF8MB4);
  col.set_collation_type(CS_TYPE_UTF8MB4_BIN);
  col.set_nullable(true);
  col.set_is_hidden(true);
  if (OB_FAIL(col.set_column_name(name))) {
  }
  return ret;
}

int fill_int_col(ObColumnSchemaV2 &col, uint64_t id, const ObString &name) {
  int ret = OB_SUCCESS;
  col.reset();
  col.set_column_id(id);
  col.set_data_type(ObIntType);
  col.set_nullable(true);
  col.set_is_hidden(true);
  col.set_accuracy(ObAccuracy::DDL_DEFAULT_ACCURACY[ObIntType]);
  if (OB_FAIL(col.set_column_name(name))) {
  }
  return ret;
}

int load_guard(ObSchemaGetterGuard &guard) {
  int ret = OB_SUCCESS;
  if (OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(GCTX.schema_service_->get_runtime_schema_guard(guard))) {
    LOG_WARN("get schema guard failed", K(ret));
  }
  return ret;
}

int lookup_table(ObSchemaGetterGuard &guard,
                 const ObString &db,
                 const ObString &table,
                 uint64_t table_id_hint,
                 const ObTableSchema *&schema) {
  int ret = OB_SUCCESS;
  schema = nullptr;
  if (OB_INVALID_ID != table_id_hint && table_id_hint > 0) {
    if (OB_FAIL(guard.get_table_schema(table_id_hint, schema))) {
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(schema)) {
    if (OB_FAIL(guard.get_table_schema(db, table, false, schema, true))) {
    }
  }
  return ret;
}

int persist_def(const AiDef &def) {
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected = 0;
  if (OB_FAIL(sql.append("DELETE FROM oceanbase.__all_ai_column WHERE table_id = "))) {
  } else if (OB_FAIL(sql.append_fmt("%lu AND column_id = %lu", def.table_id_, def.column_id_))) {
  } else if (OB_FAIL(sql_proxy_write(sql, affected))) {
  }
  sql.reset();
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(sql.append(
      "INSERT INTO oceanbase.__all_ai_column(table_id, column_id, column_name, database_name, "
      "table_name, max_retries, state_column_id, request_column_id, result_spec_column_id, "
      "state_column_name, request_column_name, spec_column_name, normalized_expr, input_expr, "
      "type_prompt, model_name, func_type, def_version, vector_dim, depend_column_ids) VALUES ("))) {
  } else if (OB_FAIL(sql.append_fmt("%lu, %lu, ", def.table_id_, def.column_id_))) {
  } else if (OB_FAIL(quote_literal(sql, def.column_name_))) {
  } else if (OB_FAIL(sql.append(", "))) {
  } else if (OB_FAIL(quote_literal(sql, def.database_name_))) {
  } else if (OB_FAIL(sql.append(", "))) {
  } else if (OB_FAIL(quote_literal(sql, def.table_name_))) {
  } else if (OB_FAIL(sql.append_fmt(", %ld, %lu, %lu, %lu, ",
                                    def.max_retries_, def.state_column_id_,
                                    def.request_column_id_, def.spec_column_id_))) {
  } else if (OB_FAIL(quote_literal(sql, def.state_name_))) {
  } else if (OB_FAIL(sql.append(", "))) {
  } else if (OB_FAIL(quote_literal(sql, def.request_name_))) {
  } else if (OB_FAIL(sql.append(", "))) {
  } else if (OB_FAIL(quote_literal(sql, def.spec_name_))) {
  } else if (OB_FAIL(sql.append(", "))) {
  } else if (OB_FAIL(quote_literal(sql, def.normalized_expr_))) {
  } else if (OB_FAIL(sql.append(", "))) {
  } else if (OB_FAIL(quote_literal(sql, def.input_expr_))) {
  } else if (OB_FAIL(sql.append(", "))) {
  } else if (OB_FAIL(quote_literal(sql, def.type_prompt_))) {
  } else if (OB_FAIL(sql.append(", "))) {
  } else if (OB_FAIL(quote_literal(sql, def.model_name_))) {
  } else if (OB_FAIL(sql.append_fmt(", %d, %ld, %ld, ", def.func_type_, def.def_version_, def.vector_dim_))) {
  } else if (OB_FAIL(quote_literal(sql, def.depend_names_))) {
  } else if (OB_FAIL(sql.append(")"))) {
  } else if (OB_FAIL(sql_proxy_write(sql, affected))) {
  }
  return ret;
}

int reload_catalog_locked() {
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObMySQLProxy::MySQLResult res;
  if (OB_FAIL(sql.append(
      "SELECT table_id, column_id, column_name, database_name, table_name, max_retries, "
      "state_column_id, request_column_id, result_spec_column_id, state_column_name, "
      "request_column_name, spec_column_name, normalized_expr, input_expr, type_prompt, "
      "model_name, func_type, def_version, vector_dim, depend_column_ids "
      "FROM oceanbase.__all_ai_column"))) {
  } else if (OB_FAIL(sql_proxy_read(sql, res))) {
  } else if (OB_ISNULL(res.get_result())) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    AiCatalog::instance().defs().reset();
    common::sqlclient::ObMySQLResult *result = res.get_result();
    while (OB_SUCC(ret) && OB_SUCC(result->next())) {
      AiDef def;
      MEMSET(&def, 0, sizeof(def));
      ObString column_name, db, table, state_name, request_name, spec_name;
      ObString normalized, input_expr, prompt, model, depends;
      int64_t table_id = 0, column_id = 0, state_id = 0, request_id = 0, spec_id = 0;
      int64_t max_retries = 0, func_type = 0, def_version = 0, vector_dim = 0;
      EXTRACT_INT_FIELD_MYSQL(*result, "table_id", table_id, int64_t);
      EXTRACT_INT_FIELD_MYSQL(*result, "column_id", column_id, int64_t);
      EXTRACT_VARCHAR_FIELD_MYSQL(*result, "column_name", column_name);
      EXTRACT_VARCHAR_FIELD_MYSQL(*result, "database_name", db);
      EXTRACT_VARCHAR_FIELD_MYSQL(*result, "table_name", table);
      EXTRACT_INT_FIELD_MYSQL(*result, "max_retries", max_retries, int64_t);
      EXTRACT_INT_FIELD_MYSQL(*result, "state_column_id", state_id, int64_t);
      EXTRACT_INT_FIELD_MYSQL(*result, "request_column_id", request_id, int64_t);
      EXTRACT_INT_FIELD_MYSQL(*result, "result_spec_column_id", spec_id, int64_t);
      EXTRACT_VARCHAR_FIELD_MYSQL(*result, "state_column_name", state_name);
      EXTRACT_VARCHAR_FIELD_MYSQL(*result, "request_column_name", request_name);
      EXTRACT_VARCHAR_FIELD_MYSQL(*result, "spec_column_name", spec_name);
      EXTRACT_VARCHAR_FIELD_MYSQL(*result, "normalized_expr", normalized);
      EXTRACT_VARCHAR_FIELD_MYSQL(*result, "input_expr", input_expr);
      EXTRACT_VARCHAR_FIELD_MYSQL(*result, "type_prompt", prompt);
      EXTRACT_VARCHAR_FIELD_MYSQL(*result, "model_name", model);
      EXTRACT_INT_FIELD_MYSQL(*result, "func_type", func_type, int64_t);
      EXTRACT_INT_FIELD_MYSQL(*result, "def_version", def_version, int64_t);
      EXTRACT_INT_FIELD_MYSQL(*result, "vector_dim", vector_dim, int64_t);
      EXTRACT_VARCHAR_FIELD_MYSQL(*result, "depend_column_ids", depends);
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(AiCatalog::instance().copy_str(column_name, def.column_name_))) {
      } else if (OB_FAIL(AiCatalog::instance().copy_str(db, def.database_name_))) {
      } else if (OB_FAIL(AiCatalog::instance().copy_str(table, def.table_name_))) {
      } else if (OB_FAIL(AiCatalog::instance().copy_str(state_name, def.state_name_))) {
      } else if (OB_FAIL(AiCatalog::instance().copy_str(request_name, def.request_name_))) {
      } else if (OB_FAIL(AiCatalog::instance().copy_str(spec_name, def.spec_name_))) {
      } else if (OB_FAIL(AiCatalog::instance().copy_str(normalized, def.normalized_expr_))) {
      } else if (OB_FAIL(AiCatalog::instance().copy_str(input_expr, def.input_expr_))) {
      } else if (OB_FAIL(AiCatalog::instance().copy_str(prompt, def.type_prompt_))) {
      } else if (OB_FAIL(AiCatalog::instance().copy_str(model, def.model_name_))) {
      } else if (OB_FAIL(AiCatalog::instance().copy_str(depends, def.depend_names_))) {
      } else {
        def.table_id_ = static_cast<uint64_t>(table_id);
        def.column_id_ = static_cast<uint64_t>(column_id);
        def.state_column_id_ = static_cast<uint64_t>(state_id);
        def.request_column_id_ = static_cast<uint64_t>(request_id);
        def.spec_column_id_ = static_cast<uint64_t>(spec_id);
        def.max_retries_ = max_retries;
        def.func_type_ = static_cast<int>(func_type);
        def.def_version_ = def_version;
        def.vector_dim_ = vector_dim;
        def.flushed_ = true;
        if (OB_FAIL(AiCatalog::instance().defs().push_back(def))) {
        }
      }
    }
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

int ensure_catalog() {
  int ret = OB_SUCCESS;
  lib::ObMutexGuard guard(AiCatalog::instance().lock());
  if (AiCatalog::instance().defs().empty()) {
    ret = reload_catalog_locked();
  }
  return ret;
}

int print_datum_literal(const ObDatum &datum, const ObExpr *expr, ObSqlString &sql) {
  int ret = OB_SUCCESS;
  if (datum.is_null()) {
    ret = sql.append("NULL");
  } else if (ob_is_int_tc(expr->datum_meta_.type_)) {
    ret = sql.append_fmt("%ld", datum.get_int());
  } else if (ob_is_uint_tc(expr->datum_meta_.type_)) {
    ret = sql.append_fmt("%lu", datum.get_uint());
  } else if (ob_is_string_tc(expr->datum_meta_.type_)) {
    ObString s = datum.get_string();
    ret = quote_literal(sql, s);
  } else {
    ret = sql.append_fmt("%ld", datum.get_int());
  }
  return ret;
}

int build_rowkey_predicate(ObTableModifyOp &op,
                           const ObDMLBaseCtDef &ctdef,
                           bool use_new_row,
                           const ObTableSchema &schema,
                           ObSqlString &predicate) {
  int ret = OB_SUCCESS;
  const ExprFixedArray &row = use_new_row ? ctdef.new_row_ : ctdef.old_row_;
  bool first = true;
  for (int64_t i = 0; OB_SUCC(ret) && i < schema.get_rowkey_column_num(); ++i) {
    const ObColumnSchemaV2 *pk = schema.get_rowkey_column(i);
    if (OB_ISNULL(pk) || pk->is_hidden()) {
    } else {
      int64_t idx = -1;
      for (int64_t j = 0; j < ctdef.column_ids_.count(); ++j) {
        if (ctdef.column_ids_.at(j) == pk->get_column_id()) {
          idx = j;
          break;
        }
      }
      if (idx < 0 || idx >= row.count() || OB_ISNULL(row.at(idx))) {
        ret = OB_ERR_UNEXPECTED;
      } else {
        ObDatum *datum = nullptr;
        if (!first && OB_FAIL(predicate.append(" AND "))) {
        } else if (OB_FAIL(predicate.append("`"))) {
        } else if (OB_FAIL(predicate.append(pk->get_column_name_str()))) {
        } else if (OB_FAIL(predicate.append("` = "))) {
        } else if (OB_FAIL(row.at(idx)->eval(op.get_eval_ctx(), datum))) {
        } else if (OB_ISNULL(datum)) {
          ret = OB_ERR_UNEXPECTED;
        } else if (OB_FAIL(print_datum_literal(*datum, row.at(idx), predicate))) {
        } else {
          first = false;
        }
      }
    }
  }
  return ret;
}

bool depend_changed(ObTableModifyOp &op,
                    const ObDMLBaseCtDef &ctdef,
                    const ObTableSchema &schema,
                    const AiDef &def) {
  if (def.depend_names_.empty()) {
    return false;
  }
  ObSEArray<uint64_t, 4> depend_ids;
  const ObColumnSchemaV2 *col = nullptr;
  for (int64_t i = 0; i < schema.get_column_count(); ++i) {
    col = schema.get_column_schema_by_idx(i);
    if (OB_NOT_NULL(col)) {
      ObString name = col->get_column_name_str();
      // depend_names_ is comma separated
      const char *p = def.depend_names_.ptr();
      int64_t remain = def.depend_names_.length();
      while (remain > 0) {
        const char *comma = static_cast<const char *>(memchr(p, ',', remain));
        int64_t len = comma ? comma - p : remain;
        ObString part(len, p);
        if (0 == part.case_compare(name)) {
          depend_ids.push_back(col->get_column_id());
          break;
        }
        if (!comma) {
          break;
        }
        p = comma + 1;
        remain = def.depend_names_.length() - (p - def.depend_names_.ptr());
      }
    }
  }
  for (int64_t i = 0; i < schema.get_rowkey_column_num(); ++i) {
    const ObColumnSchemaV2 *pk = schema.get_rowkey_column(i);
    if (OB_NOT_NULL(pk) && !pk->is_hidden()) {
      depend_ids.push_back(pk->get_column_id());
    }
  }
  for (int64_t i = 0; i < depend_ids.count(); ++i) {
    int64_t idx = -1;
    for (int64_t j = 0; j < ctdef.column_ids_.count(); ++j) {
      if (ctdef.column_ids_.at(j) == depend_ids.at(i)) {
        idx = j;
        break;
      }
    }
    if (idx >= 0 && idx < ctdef.old_row_.count() && idx < ctdef.new_row_.count() &&
        OB_NOT_NULL(ctdef.old_row_.at(idx)) && OB_NOT_NULL(ctdef.new_row_.at(idx))) {
      ObDatum *old_d = nullptr;
      ObDatum *new_d = nullptr;
      if (OB_SUCCESS == ctdef.old_row_.at(idx)->eval(op.get_eval_ctx(), old_d) &&
          OB_SUCCESS == ctdef.new_row_.at(idx)->eval(op.get_eval_ctx(), new_d) &&
          OB_NOT_NULL(old_d) && OB_NOT_NULL(new_d)) {
        if (old_d->is_null() != new_d->is_null()) {
          return true;
        }
        if (!old_d->is_null() && old_d->len_ == new_d->len_) {
          if (0 != MEMCMP(old_d->ptr_, new_d->ptr_, old_d->len_)) {
            return true;
          }
        } else if (!old_d->is_null()) {
          return true;
        }
      }
    }
  }
  return false;
}

int insert_tasks_for_row(ObTableModifyOp &op,
                         const ObTableSchema &schema,
                         const ObString &predicate,
                         const ObString &request_id,
                         const uint64_t *only_cols,
                         int64_t only_cnt) {
  int ret = OB_SUCCESS;
  ObSqlString del;
  if (OB_FAIL(del.append_fmt(
      "DELETE FROM oceanbase.__all_ai_column_task WHERE table_id = %lu AND rowkey_predicate = ",
      schema.get_table_id()))) {
  } else if (OB_FAIL(quote_literal(del, predicate))) {
  } else if (only_cnt > 0 && OB_NOT_NULL(only_cols)) {
    if (OB_FAIL(del.append(" AND column_id IN ("))) {
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < only_cnt; ++i) {
      if (i > 0 && OB_FAIL(del.append(","))) {
      } else if (OB_FAIL(del.append_fmt("%lu", only_cols[i]))) {
      }
    }
    if (OB_SUCC(ret)) {
      ret = del.append(")");
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(nested_write(op, del))) {
  }
  lib::ObMutexGuard guard(AiCatalog::instance().lock());
  for (int64_t i = 0; OB_SUCC(ret) && i < AiCatalog::instance().defs().count(); ++i) {
    const AiDef &def = AiCatalog::instance().defs().at(i);
    if (def.table_id_ != schema.get_table_id()) {
      continue;
    }
    if (only_cnt > 0 && OB_NOT_NULL(only_cols)) {
      bool keep = false;
      for (int64_t c = 0; c < only_cnt; ++c) {
        if (only_cols[c] == def.column_id_) {
          keep = true;
        }
      }
      if (!keep) {
        continue;
      }
    }
    ObSqlString sql;
    if (OB_FAIL(sql.append(
        "INSERT INTO oceanbase.__all_ai_column_task(task_id, table_id, column_id, request_id, "
        "rowkey_predicate, state, retry_count, max_retries, eligible_ts, def_version, payload, "
        "database_name, table_name) VALUES ("))) {
    } else if (OB_FAIL(sql.append_fmt("%ld, %lu, %lu, ", next_task_id(), def.table_id_, def.column_id_))) {
    } else if (OB_FAIL(quote_literal(sql, request_id))) {
    } else if (OB_FAIL(sql.append(", "))) {
    } else if (OB_FAIL(quote_literal(sql, predicate))) {
    } else if (OB_FAIL(sql.append_fmt(", %d, 0, %ld, now(6), %ld, NULL, ",
                                      AI_COL_TASK_PENDING, def.max_retries_, def.def_version_))) {
    } else if (OB_FAIL(quote_literal(sql, def.database_name_))) {
    } else if (OB_FAIL(sql.append(", "))) {
    } else if (OB_FAIL(quote_literal(sql, def.table_name_))) {
    } else if (OB_FAIL(sql.append(")"))) {
    } else if (OB_FAIL(nested_write(op, sql))) {
    }
  }
  return ret;
}

int resolve_ids_from_schema(AiDef &def, const ObTableSchema &schema) {
  int ret = OB_SUCCESS;
  const ObColumnSchemaV2 *col = schema.get_column_schema(def.column_name_);
  const ObColumnSchemaV2 *state = schema.get_column_schema(def.state_name_);
  const ObColumnSchemaV2 *request = schema.get_column_schema(def.request_name_);
  const ObColumnSchemaV2 *spec = schema.get_column_schema(def.spec_name_);
  if (OB_ISNULL(col) || OB_ISNULL(state) || OB_ISNULL(request) || OB_ISNULL(spec)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ai hidden columns missing after ddl", K(ret), K(def.column_name_));
  } else {
    def.table_id_ = schema.get_table_id();
    def.column_id_ = col->get_column_id();
    def.state_column_id_ = state->get_column_id();
    def.request_column_id_ = request->get_column_id();
    def.spec_column_id_ = spec->get_column_id();
  }
  return ret;
}

bool ident_in_sql(const ObString &sql, const ObString &ident) {
  const char *s = sql.ptr();
  int64_t n = sql.length();
  bool in_sq = false, in_dq = false, in_bt = false;
  for (int64_t i = 0; i < n; ++i) {
    char c = s[i];
    if (in_sq) {
      if (c == '\\' && i + 1 < n) { ++i; }
      else if (c == '\'') { in_sq = false; }
      continue;
    }
    if (in_dq) {
      if (c == '"') { in_dq = false; }
      continue;
    }
    if (in_bt) {
      if (c == '`') { in_bt = false; }
      continue;
    }
    if (c == '\'') { in_sq = true; continue; }
    if (c == '"') { in_dq = true; continue; }
    if (c == '`') { in_bt = true; continue; }
    if (!is_ident_char(c)) {
      continue;
    }
    int64_t j = i;
    while (j < n && is_ident_char(s[j])) { ++j; }
    ObString word(j - i, s + i);
    if (0 == word.case_compare(ident)) {
      return true;
    }
    i = j - 1;
  }
  return false;
}

int call_model(const AiDef &def, const ObString &user_text, ObIAllocator &alloc, ObString &result, bool &ok) {
  int ret = OB_SUCCESS;
  ok = false;
  result.reset();
  ObAIFuncExprInfo *info = nullptr;
  ObAiModelEndpointInfo endpoint;
  query::ObIAiEndpointResolver *resolver =
      ::oceanbase::share::server_service<::oceanbase::query::ObIAiEndpointResolver>();
  if (OB_ISNULL(resolver)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(ObAIFuncUtils::get_ai_func_info(alloc, def.model_name_, info))) {
  } else if (OB_FAIL(resolver->resolve_by_model_name(def.model_name_, alloc, endpoint))) {
  } else if (AI_COL_FUNC_EMBED == def.func_type_) {
    ObAIFuncModel model(alloc, *info, endpoint);
    if (OB_FAIL(model.call_dense_embedding(const_cast<ObString &>(user_text), nullptr, result))) {
    } else {
      ok = true;
    }
  } else {
    ObArray<ObString> headers;
    ObJsonObject *body = nullptr;
    ObJsonObject *response = nullptr;
    ObIJsonBase *result_base = nullptr;
    ObAIFuncIComplete *provider = nullptr;
    ObAIFuncClient client;
    ObString access_key;
    ObString system_prompt = def.type_prompt_;
    ObString user_prompt = user_text;
    ObString request_model = endpoint.get_request_model_name();
    ObString result_str;
    if (OB_FAIL(ObAIFuncUtils::get_complete_provider(alloc, endpoint.get_provider(), provider))) {
    } else if (OB_FAIL(endpoint.get_unencrypted_access_key(alloc, access_key))) {
    } else if (OB_FAIL(provider->get_header(alloc, access_key, headers))) {
    } else if (OB_FAIL(provider->get_body(alloc, request_model, system_prompt, user_prompt, nullptr, body))) {
    } else if (OB_FAIL(client.send_post(alloc, endpoint.get_url(), headers, body, response))) {
    } else if (OB_FAIL(provider->parse_output(alloc, response, result_base))) {
    } else if (OB_FAIL(ObAIFuncJsonUtils::print_json_to_str(alloc, result_base, result_str))) {
    } else {
      result = result_str;
      ok = true;
    }
  }
  if (OB_FAIL(ret)) {
    ok = false;
    ret = OB_SUCCESS;
  }
  return ret;
}

bool contains_text(const ObString &s, const char *pat) {
  if (OB_ISNULL(pat)) {
    return false;
  }
  int64_t n = static_cast<int64_t>(strlen(pat));
  if (n <= 0 || s.length() < n || OB_ISNULL(s.ptr())) {
    return false;
  }
  for (int64_t i = 0; i + n <= s.length(); ++i) {
    if (0 == MEMCMP(s.ptr() + i, pat, n)) {
      return true;
    }
  }
  return false;
}

bool parse_i64(const ObString &text, int64_t &out) {
  if (text.empty()) {
    return false;
  }
  char buf[64];
  int64_t n = std::min(text.length(), static_cast<int64_t>(sizeof(buf) - 1));
  MEMCPY(buf, text.ptr(), n);
  buf[n] = '\0';
  char *end = nullptr;
  errno = 0;
  long v = strtol(buf, &end, 10);
  if (errno != 0 || end == buf || *end != '\0') {
    return false;
  }
  out = static_cast<int64_t>(v);
  return true;
}

int publish_group(const ObString &request_id,
                  const ObString &predicate,
                  uint64_t table_id,
                  const ObIArray<AiTask> &tasks,
                  const ObIArray<ObString> &values,
                  const ObIArray<int> &states) {
  int ret = OB_SUCCESS;
  if (tasks.empty()) {
    return ret;
  }
  const AiTask &head = tasks.at(0);
  ObSqlString sql;
  int64_t affected = 0;
  if (OB_FAIL(sql.append_fmt("UPDATE `%.*s`.`%.*s` SET ",
                             head.database_name_.length(), head.database_name_.ptr(),
                             head.table_name_.length(), head.table_name_.ptr()))) {
  }
  bool first = true;
  lib::ObMutexGuard guard(AiCatalog::instance().lock());
  for (int64_t i = 0; OB_SUCC(ret) && i < tasks.count(); ++i) {
    const AiDef *def = AiCatalog::instance().find_mutable(tasks.at(i).table_id_, tasks.at(i).column_id_);
    if (OB_ISNULL(def)) {
      continue;
    }
    if (!first && OB_FAIL(sql.append(", "))) {
    }
    first = false;
    if (OB_FAIL(ret)) {
    } else if (states.at(i) == AI_COL_ROW_READY) {
      if (OB_FAIL(sql.append_fmt("`%.*s` = ", def->column_name_.length(), def->column_name_.ptr()))) {
      } else if (OB_FAIL(sql.append(values.at(i)))) {
      }
    } else if (OB_FAIL(sql.append_fmt("`%.*s` = NULL", def->column_name_.length(), def->column_name_.ptr()))) {
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(sql.append_fmt(", `%.*s` = %d, `%.*s` = ",
                                      def->state_name_.length(), def->state_name_.ptr(),
                                      states.at(i),
                                      def->request_name_.length(), def->request_name_.ptr()))) {
    } else if (OB_FAIL(quote_literal(sql, request_id))) {
    } else if (OB_FAIL(sql.append_fmt(", `%.*s` = %ld",
                                      def->spec_name_.length(), def->spec_name_.ptr(),
                                      tasks.at(i).def_version_))) {
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(sql.append(" WHERE "))) {
  } else if (OB_FAIL(sql.append(predicate))) {
  } else if (OB_FAIL(sql.append(" AND ("))) {
  }
  // stale guard: every column's request id is null or still this request
  bool guard_first = true;
  for (int64_t i = 0; OB_SUCC(ret) && i < tasks.count(); ++i) {
    const AiDef *def = AiCatalog::instance().find_mutable(tasks.at(i).table_id_, tasks.at(i).column_id_);
    if (OB_ISNULL(def)) {
      continue;
    }
    if (!guard_first && OB_FAIL(sql.append(" AND "))) {
    }
    guard_first = false;
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(sql.append_fmt("(`%.*s` IS NULL OR `%.*s` = ",
                                      def->request_name_.length(), def->request_name_.ptr(),
                                      def->request_name_.length(), def->request_name_.ptr()))) {
    } else if (OB_FAIL(quote_literal(sql, request_id))) {
    } else if (OB_FAIL(sql.append_fmt(") AND (`%.*s` IS NULL OR `%.*s` = %ld)",
                                      def->spec_name_.length(), def->spec_name_.ptr(),
                                      def->spec_name_.length(), def->spec_name_.ptr(),
                                      tasks.at(i).def_version_))) {
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(sql.append(")"))) {
  } else if (OB_FAIL(sql_proxy_write(sql, affected))) {
  } else if (affected > 0) {
    ObSqlString del;
    if (OB_FAIL(del.append_fmt(
        "DELETE FROM oceanbase.__all_ai_column_task WHERE table_id = %lu AND request_id = ",
        table_id))) {
    } else if (OB_FAIL(quote_literal(del, request_id))) {
    } else if (OB_FAIL(del.append(" AND rowkey_predicate = "))) {
    } else if (OB_FAIL(quote_literal(del, predicate))) {
    } else if (OB_FAIL(sql_proxy_write(del, affected))) {
    }
  } else {
    ObSqlString del;
    if (OB_FAIL(del.append_fmt(
        "DELETE FROM oceanbase.__all_ai_column_task WHERE table_id = %lu AND request_id = ",
        table_id))) {
    } else if (OB_FAIL(quote_literal(del, request_id))) {
    } else if (OB_FAIL(del.append(" AND rowkey_predicate = "))) {
    } else if (OB_FAIL(quote_literal(del, predicate))) {
    } else if (OB_FAIL(sql_proxy_write(del, affected))) {
    }
  }
  return ret;
}

int defer_group(const ObIArray<AiTask> &group, const ObIArray<int> &kinds, const ObIArray<ObString> &values) {
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected = 0;
  if (group.empty()) {
    return ret;
  }
  if (OB_FAIL(sql.append_fmt(
      "UPDATE oceanbase.__all_ai_column_task SET state = %d, "
      "eligible_ts = DATE_ADD(now(6), INTERVAL %ld MICROSECOND), retry_count = CASE task_id ",
      AI_COL_TASK_RETRY, AI_RETRY_DELAY_US))) {
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < group.count() && i < kinds.count(); ++i) {
    int64_t retry_count = (0 == kinds.at(i)) ? group.at(i).retry_count_ + 1 : group.at(i).retry_count_;
    if (OB_FAIL(sql.append_fmt("WHEN %ld THEN %ld ", group.at(i).task_id_, retry_count))) {
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(sql.append("ELSE retry_count END, payload = CASE task_id "))) {
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < group.count() && i < kinds.count(); ++i) {
    if (OB_FAIL(sql.append_fmt("WHEN %ld THEN ", group.at(i).task_id_))) {
    } else if (1 == kinds.at(i) && i < values.count() && !values.at(i).empty()) {
      if (OB_FAIL(quote_literal(sql, values.at(i)))) {
      } else if (OB_FAIL(sql.append(" "))) {
      }
    } else if (OB_FAIL(sql.append("NULL "))) {
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(sql.append("ELSE payload END WHERE task_id IN ("))) {
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < group.count(); ++i) {
    if (i > 0 && OB_FAIL(sql.append(","))) {
    } else if (OB_FAIL(sql.append_fmt("%ld", group.at(i).task_id_))) {
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(sql.append(")"))) {
  } else if (OB_FAIL(sql_proxy_write(sql, affected))) {
  }
  return ret;
}

int drop_request_tasks(uint64_t table_id, const ObString &request_id, const ObString &predicate) {
  int ret = OB_SUCCESS;
  ObSqlString del;
  int64_t affected = 0;
  if (OB_FAIL(del.append_fmt(
      "DELETE FROM oceanbase.__all_ai_column_task WHERE table_id = %lu AND request_id = ",
      table_id))) {
  } else if (OB_FAIL(quote_literal(del, request_id))) {
  } else if (OB_FAIL(del.append(" AND rowkey_predicate = "))) {
  } else if (OB_FAIL(quote_literal(del, predicate))) {
  } else if (OB_FAIL(sql_proxy_write(del, affected))) {
  }
  return ret;
}

int eval_input(const AiDef &def, const ObString &predicate, ObIAllocator &alloc, ObString &input) {
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObMySQLProxy::MySQLResult res;
  if (OB_FAIL(sql.append_fmt("SELECT %.*s AS ai_input FROM `%.*s`.`%.*s` WHERE %.*s",
                             def.input_expr_.length(), def.input_expr_.ptr(),
                             def.database_name_.length(), def.database_name_.ptr(),
                             def.table_name_.length(), def.table_name_.ptr(),
                             predicate.length(), predicate.ptr()))) {
  } else if (OB_FAIL(sql_proxy_read(sql, res))) {
  } else if (OB_ISNULL(res.get_result())) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(res.get_result()->next())) {
  } else {
    ObString val;
    if (OB_FAIL(res.get_result()->get_varchar("ai_input", val))) {
      int64_t iv = 0;
      ret = OB_SUCCESS;
      if (OB_SUCC(res.get_result()->get_int("ai_input", iv))) {
        char *buf = static_cast<char *>(alloc.alloc(32));
        if (OB_ISNULL(buf)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
        } else {
          int n = snprintf(buf, 32, "%ld", iv);
          input.assign_ptr(buf, n);
          ret = OB_SUCCESS;
        }
      }
    } else {
      char *buf = static_cast<char *>(alloc.alloc(val.length()));
      if (OB_ISNULL(buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else {
        MEMCPY(buf, val.ptr(), val.length());
        input.assign_ptr(buf, val.length());
      }
    }
  }
  return ret;
}

void *worker_main(void *) {
  while (true) {
    ::usleep(50 * 1000);
    ObArenaAllocator alloc(ObModIds::OB_SQL_EXPR);
    ObSqlString sql;
    ObMySQLProxy::MySQLResult res;
    if (OB_ISNULL(GCTX.sql_proxy_)) {
      continue;
    }
    sql.append("UPDATE oceanbase.__all_ai_column_task SET state = 0 WHERE state = 1 AND eligible_ts <= now(6)");
    int64_t affected = 0;
    sql_proxy_write(sql, affected);
    sql.reset();
    if (OB_FAIL(ensure_catalog())) {
      continue;
    }
    if (OB_SUCCESS != sql.append(
        "SELECT task_id, table_id, column_id, request_id, rowkey_predicate, state, retry_count, "
        "max_retries, def_version, payload, database_name, table_name FROM oceanbase.__all_ai_column_task "
        "WHERE state IN (0, 3) AND eligible_ts <= now(6) ORDER BY task_id LIMIT 32")) {
      continue;
    }
    if (OB_SUCCESS != sql_proxy_read(sql, res) || OB_ISNULL(res.get_result())) {
      continue;
    }
    ObArray<AiTask> tasks;
    common::sqlclient::ObMySQLResult *result = res.get_result();
    int ret = OB_SUCCESS;
    while (OB_SUCC(ret) && OB_SUCC(result->next())) {
      AiTask task;
      MEMSET(&task, 0, sizeof(task));
      int64_t task_id = 0, table_id = 0, column_id = 0, retry_count = 0, max_retries = 0, def_version = 0, state = 0;
      ObString request_id, predicate, payload, db, table;
      EXTRACT_INT_FIELD_MYSQL(*result, "task_id", task_id, int64_t);
      EXTRACT_INT_FIELD_MYSQL(*result, "table_id", table_id, int64_t);
      EXTRACT_INT_FIELD_MYSQL(*result, "column_id", column_id, int64_t);
      EXTRACT_VARCHAR_FIELD_MYSQL(*result, "request_id", request_id);
      EXTRACT_VARCHAR_FIELD_MYSQL(*result, "rowkey_predicate", predicate);
      EXTRACT_INT_FIELD_MYSQL(*result, "state", state, int64_t);
      EXTRACT_INT_FIELD_MYSQL(*result, "retry_count", retry_count, int64_t);
      EXTRACT_INT_FIELD_MYSQL(*result, "max_retries", max_retries, int64_t);
      EXTRACT_INT_FIELD_MYSQL(*result, "def_version", def_version, int64_t);
      if (OB_FAIL(result->get_varchar("payload", payload))) {
        payload.reset();
        ret = OB_SUCCESS;
      }
      EXTRACT_VARCHAR_FIELD_MYSQL(*result, "database_name", db);
      EXTRACT_VARCHAR_FIELD_MYSQL(*result, "table_name", table);
      if (OB_FAIL(ret)) {
        break;
      }
      task.task_id_ = task_id;
      task.table_id_ = static_cast<uint64_t>(table_id);
      task.column_id_ = static_cast<uint64_t>(column_id);
      task.retry_count_ = retry_count;
      task.max_retries_ = max_retries;
      task.def_version_ = def_version;
      task.state_ = static_cast<int>(state);
      auto copy = [&](const ObString &in, ObString &out) {
        char *b = static_cast<char *>(alloc.alloc(in.length() + 1));
        if (OB_NOT_NULL(b) && in.length() > 0) {
          MEMCPY(b, in.ptr(), in.length());
          out.assign_ptr(b, in.length());
        }
      };
      copy(request_id, task.request_id_);
      copy(predicate, task.rowkey_predicate_);
      copy(payload, task.payload_);
      copy(db, task.database_name_);
      copy(table, task.table_name_);
      tasks.push_back(task);
    }
    // group by request + predicate
    ObArray<int64_t> used;
    for (int64_t i = 0; i < tasks.count(); ++i) {
      bool seen = false;
      for (int64_t u = 0; u < used.count(); ++u) {
        if (used.at(u) == i) { seen = true; }
      }
      if (seen) {
        continue;
      }
      ObArray<AiTask> group;
      ObArray<ObString> values;
      ObArray<int> row_states;
      group.push_back(tasks.at(i));
      used.push_back(i);
      for (int64_t j = i + 1; j < tasks.count(); ++j) {
        if (tasks.at(j).table_id_ == tasks.at(i).table_id_ &&
            0 == tasks.at(j).request_id_.compare(tasks.at(i).request_id_) &&
            0 == tasks.at(j).rowkey_predicate_.compare(tasks.at(i).rowkey_predicate_)) {
          group.push_back(tasks.at(j));
          used.push_back(j);
        }
      }
      bool retry_later = false;
      bool drop_group = false;
      ObArray<int> kinds;
      for (int64_t g = 0; g < group.count(); ++g) {
        AiTask &task = group.at(g);
        AiDef def;
        bool has_def = false;
        {
          lib::ObMutexGuard glock(AiCatalog::instance().lock());
          const AiDef *found = AiCatalog::instance().find_mutable(task.table_id_, task.column_id_);
          if (OB_NOT_NULL(found)) {
            def = *found;
            has_def = true;
          }
        }
        ObString value;
        int row_state = AI_COL_ROW_FAILED;
        int kind = 2;
        if (!has_def || def.def_version_ != task.def_version_) {
          kind = 3;
          drop_group = true;
        } else if (!task.payload_.empty()) {
          value = task.payload_;
          row_state = AI_COL_ROW_READY;
          kind = 1;
        } else {
          ObString input;
          ObString model_out;
          bool ok = false;
          int ev = eval_input(def, task.rowkey_predicate_, alloc, input);
          if (OB_SUCCESS != ev) {
            kind = 3;
            drop_group = true;
          } else if (OB_SUCCESS != call_model(def, input, alloc, model_out, ok) || !ok) {
            if (task.retry_count_ < task.max_retries_) {
              kind = 0;
              retry_later = true;
            } else {
              kind = 2;
              row_state = AI_COL_ROW_FAILED;
            }
          } else {
            bool typed_ok = true;
            ObSqlString literal;
            ObString raw = model_out;
            if (raw.length() >= 2 && OB_NOT_NULL(raw.ptr()) && raw.ptr()[0] == '"' &&
                raw.ptr()[raw.length() - 1] == '"') {
              raw.assign_ptr(raw.ptr() + 1, raw.length() - 2);
            }
            if (contains_text(def.type_prompt_, "base-10")) {
              char tmp[128];
              int64_t n = std::min(raw.length(), static_cast<int64_t>(sizeof(tmp) - 1));
              if (n > 0 && OB_NOT_NULL(raw.ptr())) {
                MEMCPY(tmp, raw.ptr(), n);
              }
              tmp[n] = 0;
              char *end = nullptr;
              long v = strtol(tmp, &end, 10);
              if (end == tmp || *end != '\0') {
                typed_ok = false;
              } else {
                literal.append_fmt("%ld", v);
              }
            } else if (AI_COL_FUNC_EMBED == def.func_type_) {
              literal.append(raw);
            } else if (contains_text(def.type_prompt_, "JSON")) {
              literal.append("CAST(");
              quote_literal(literal, raw);
              literal.append(" AS JSON)");
            } else {
              quote_literal(literal, raw);
            }
            if (!typed_ok) {
              if (task.retry_count_ < task.max_retries_) {
                kind = 0;
                retry_later = true;
              } else {
                kind = 2;
                row_state = AI_COL_ROW_FAILED;
              }
            } else {
              char *b = static_cast<char *>(alloc.alloc(literal.length() + 1));
              if (OB_NOT_NULL(b) && literal.length() > 0) {
                MEMCPY(b, literal.ptr(), literal.length());
                value.assign_ptr(b, literal.length());
              }
              row_state = AI_COL_ROW_READY;
              kind = 1;
            }
          }
        }
        values.push_back(value);
        row_states.push_back(row_state);
        kinds.push_back(kind);
      }
      if (drop_group) {
        drop_request_tasks(group.at(0).table_id_, group.at(0).request_id_, group.at(0).rowkey_predicate_);
      } else if (retry_later) {
        defer_group(group, kinds, values);
      } else if (group.count() == values.count()) {
        publish_group(group.at(0).request_id_, group.at(0).rowkey_predicate_,
                      group.at(0).table_id_, group, values, row_states);
      }
    }
  }
  return nullptr;
}

int print_retry_expr(ParseNode *node, const ObString &state_name, ObSqlString &out);

int print_literal_node(ParseNode *node, ObSqlString &out) {
  int ret = OB_SUCCESS;
  if (OB_ISNULL(node)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (T_INT == node->type_ || T_UINT64 == node->type_) {
    ret = out.append_fmt("%ld", node->value_);
  } else if (OB_NOT_NULL(node->str_value_)) {
    ObString s(node->str_len_, node->str_value_);
    ret = quote_literal(out, s);
  } else {
    ret = OB_NOT_SUPPORTED;
  }
  return ret;
}

bool is_unquoted_status(ParseNode *node) {
  if (OB_ISNULL(node) || T_COLUMN_REF != node->type_ || node->num_child_ < 3 ||
      OB_ISNULL(node->children_[2]) || OB_ISNULL(node->children_[2]->str_value_)) {
    return false;
  }
  ObString name(node->children_[2]->str_len_, node->children_[2]->str_value_);
  return 0 == name.case_compare("status") && 0 == node->children_[2]->is_input_quoted_;
}

int print_retry_expr(ParseNode *node, const ObString &state_name, ObSqlString &out) {
  int ret = OB_SUCCESS;
  if (OB_ISNULL(node)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (T_OP_AND == node->type_ || T_OP_OR == node->type_) {
    if (node->num_child_ < 2) {
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_FAIL(out.append("("))) {
    } else if (OB_FAIL(print_retry_expr(node->children_[0], state_name, out))) {
    } else if (OB_FAIL(out.append(T_OP_AND == node->type_ ? " AND " : " OR "))) {
    } else if (OB_FAIL(print_retry_expr(node->children_[1], state_name, out))) {
    } else if (OB_FAIL(out.append(")"))) {
    }
  } else if (T_OP_EQ == node->type_ && node->num_child_ >= 2 && is_unquoted_status(node->children_[0])) {
    ParseNode *rhs = node->children_[1];
    ObString val;
    if (OB_NOT_NULL(rhs) && OB_NOT_NULL(rhs->str_value_)) {
      val.assign_ptr(rhs->str_value_, rhs->str_len_);
    }
    if (0 == val.case_compare("FAILED")) {
      ret = out.append_fmt("`%.*s` = %d", state_name.length(), state_name.ptr(), AI_COL_ROW_FAILED);
    } else if (0 == val.case_compare("READY")) {
      ret = out.append_fmt("`%.*s` = %d", state_name.length(), state_name.ptr(), AI_COL_ROW_READY);
    } else if (0 == val.case_compare("PENDING")) {
      ret = out.append_fmt("(`%.*s` IS NULL OR `%.*s` = %d)",
                           state_name.length(), state_name.ptr(),
                           state_name.length(), state_name.ptr(), AI_COL_ROW_PENDING);
    } else {
      ret = OB_ERR_PARSER_SYNTAX;
    }
  } else if (T_OP_EQ == node->type_ && node->num_child_ >= 2) {
    ParseNode *lhs = node->children_[0];
    if (OB_ISNULL(lhs) || T_COLUMN_REF != lhs->type_ || lhs->num_child_ < 3 ||
        OB_ISNULL(lhs->children_[2])) {
      ret = OB_NOT_SUPPORTED;
    } else if (OB_FAIL(out.append("`"))) {
    } else if (OB_FAIL(out.append(lhs->children_[2]->str_value_, lhs->children_[2]->str_len_))) {
    } else if (OB_FAIL(out.append("` = "))) {
    } else if (OB_FAIL(print_literal_node(node->children_[1], out))) {
    }
  } else if (T_OP_IN == node->type_ && node->num_child_ >= 2) {
    ParseNode *lhs = node->children_[0];
    ParseNode *list = node->children_[1];
    if (OB_ISNULL(lhs) || T_COLUMN_REF != lhs->type_ || lhs->num_child_ < 3) {
      ret = OB_NOT_SUPPORTED;
    } else if (OB_FAIL(out.append("`"))) {
    } else if (OB_FAIL(out.append(lhs->children_[2]->str_value_, lhs->children_[2]->str_len_))) {
    } else if (OB_FAIL(out.append("` IN ("))) {
    } else if (OB_ISNULL(list)) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < list->num_child_; ++i) {
        if (i > 0 && OB_FAIL(out.append(", "))) {
        } else if (OB_FAIL(print_literal_node(list->children_[i], out))) {
        }
      }
      if (OB_SUCC(ret)) {
        ret = out.append(")");
      }
    }
  } else if (OB_NOT_NULL(node->str_value_) && node->str_len_ > 0) {
    ret = out.append(node->str_value_, node->str_len_);
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "AI COLUMN RETRY predicate");
  }
  return ret;
}

} // namespace

int ObAiColumnService::define_column(const ObString &database_name,
                                     const ObString &table_name,
                                     ObColumnSchemaV2 &column,
                                     ParseNode *def_node,
                                     ObIAllocator &allocator) {
  int ret = OB_SUCCESS;
  UNUSED(allocator);
  if (OB_ISNULL(def_node) || T_AI_COLUMN_DEF != def_node->type_ || def_node->num_child_ < 1 ||
      OB_ISNULL(def_node->children_[0])) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    ParseNode *func = def_node->children_[0];
    ParseNode *opts = def_node->num_child_ > 1 ? def_node->children_[1] : nullptr;
    AiDef def;
    MEMSET(&def, 0, sizeof(def));
    def.max_retries_ = 0;
    def.def_version_ = 1;
    def.vector_dim_ = 0;
    if (column.get_extended_type_info().count() > 0) {
      ObString info = column.get_extended_type_info().at(0);
      const char *lp = OB_ISNULL(info.ptr()) ? nullptr : static_cast<const char *>(memchr(info.ptr(), '(', info.length()));
      if (OB_NOT_NULL(lp)) {
        def.vector_dim_ = atoi(lp + 1);
      }
    }
    if (T_FUN_SYS_AI_COMPLETE == func->type_) {
      def.func_type_ = AI_COL_FUNC_COMPLETE;
    } else if (T_FUN_SYS_AI_EMBED == func->type_) {
      def.func_type_ = AI_COL_FUNC_EMBED;
    } else {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "AI COLUMN expression");
    }
    if (OB_SUCC(ret) && func->num_child_ >= 2 && OB_NOT_NULL(func->children_[0]) &&
        OB_NOT_NULL(func->children_[0]->str_value_)) {
      ObString model(func->children_[0]->str_len_, func->children_[0]->str_value_);
      if (OB_FAIL(AiCatalog::instance().copy_str(model, def.model_name_))) {
      }
    }
    if (OB_SUCC(ret) && func->num_child_ >= 3 && OB_NOT_NULL(func->children_[2]) &&
        (T_INT == func->children_[2]->type_ || T_UINT64 == func->children_[2]->type_)) {
      int64_t got = func->children_[2]->value_;
      int64_t expected = def.vector_dim_;
      if (expected > 0 && got != expected) {
        ret = OB_ERR_INVALID_VECTOR_DIM;
        LOG_USER_ERROR(OB_ERR_INVALID_VECTOR_DIM, static_cast<uint32_t>(expected), static_cast<uint32_t>(got));
      }
    }
    ObSqlString depends;
    if (OB_SUCC(ret) && func->num_child_ >= 2) {
      if (OB_FAIL(walk_depend_names(func->children_[1], depends))) {
      }
    }
    if (OB_SUCC(ret)) {
      // reject dependence on another AI column registered earlier in this table
      lib::ObMutexGuard guard(AiCatalog::instance().lock());
      for (int64_t i = 0; OB_SUCC(ret) && i < AiCatalog::instance().pending().count(); ++i) {
        const AiDef &prev = AiCatalog::instance().pending().at(i);
        if (0 == prev.database_name_.case_compare(database_name) &&
            0 == prev.table_name_.case_compare(table_name)) {
          const char *p = depends.ptr();
          int64_t remain = depends.length();
          while (OB_SUCC(ret) && remain > 0 && OB_NOT_NULL(p)) {
            const char *comma = static_cast<const char *>(memchr(p, ',', remain));
            int64_t len = comma ? static_cast<int64_t>(comma - p) : remain;
            ObString part(len, p);
            if (0 == part.case_compare(prev.column_name_)) {
              ret = OB_NOT_SUPPORTED;
            }
            if (OB_ISNULL(comma)) {
              break;
            }
            p = comma + 1;
            remain = depends.length() - static_cast<int64_t>(p - depends.ptr());
          }
        }
      }
    }
    const char *prompt = type_prompt_for(column);
    ObSqlString normalized;
    if (OB_FAIL(ret)) {
    } else if (OB_NOT_NULL(func->str_value_) &&
               OB_FAIL(AiCatalog::instance().copy_str(
                   nth_call_arg(ObString(func->str_len_, func->str_value_), 1),
                   def.input_expr_))) {
    } else if (OB_FAIL(normalized.append(def.input_expr_))) {
    } else if (prompt[0] != '\0' && OB_FAIL(normalized.append(" "))) {
    } else if (prompt[0] != '\0' && OB_FAIL(normalized.append(prompt))) {
    } else if (OB_FAIL(AiCatalog::instance().copy_str(normalized.string(), def.normalized_expr_))) {
    } else if (OB_FAIL(AiCatalog::instance().copy_str(ObString(prompt), def.type_prompt_))) {
    } else if (OB_FAIL(AiCatalog::instance().copy_str(depends.string(), def.depend_names_))) {
    } else if (OB_FAIL(AiCatalog::instance().copy_str(database_name, def.database_name_))) {
    } else if (OB_FAIL(AiCatalog::instance().copy_str(table_name, def.table_name_))) {
    } else if (OB_FAIL(AiCatalog::instance().copy_str(column.get_column_name_str(), def.column_name_))) {
    }
    if (OB_SUCC(ret) && OB_NOT_NULL(opts)) {
      for (int64_t i = 0; OB_SUCC(ret) && i < opts->num_child_; ++i) {
        ParseNode *opt = opts->children_[i];
        if (OB_ISNULL(opt) || opt->num_child_ < 2 || OB_ISNULL(opt->children_[0]) || OB_ISNULL(opt->children_[1])) {
          ret = OB_ERR_PARSER_SYNTAX;
        } else {
          ObString key(opt->children_[0]->str_len_, opt->children_[0]->str_value_);
          ParseNode *val = opt->children_[1];
          if (0 == key.case_compare("sync_mode")) {
            ObString mode(val->str_len_, val->str_value_);
            if (0 != mode.case_compare("async")) {
              ret = OB_NOT_SUPPORTED;
              LOG_USER_ERROR(OB_NOT_SUPPORTED, "AI COLUMN sync_mode other than async");
            }
          } else if (0 == key.case_compare("max_retries")) {
            def.max_retries_ = val->value_;
          }
        }
      }
    }
    char name_buf[3][128];
    int n0 = snprintf(name_buf[0], sizeof(name_buf[0]), "__ai_st_%lu", column.get_column_id());
    int n1 = snprintf(name_buf[1], sizeof(name_buf[1]), "__ai_rq_%lu", column.get_column_id());
    int n2 = snprintf(name_buf[2], sizeof(name_buf[2]), "__ai_sp_%lu", column.get_column_id());
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(AiCatalog::instance().copy_str(ObString(n0, name_buf[0]), def.state_name_))) {
    } else if (OB_FAIL(AiCatalog::instance().copy_str(ObString(n1, name_buf[1]), def.request_name_))) {
    } else if (OB_FAIL(AiCatalog::instance().copy_str(ObString(n2, name_buf[2]), def.spec_name_))) {
    } else {
      lib::ObMutexGuard guard(AiCatalog::instance().lock());
      // replace existing pending def for the same column
      bool replaced = false;
      for (int64_t i = 0; i < AiCatalog::instance().pending().count(); ++i) {
        AiDef &old = AiCatalog::instance().pending().at(i);
        if (0 == old.database_name_.case_compare(def.database_name_) &&
            0 == old.table_name_.case_compare(def.table_name_) &&
            0 == old.column_name_.case_compare(def.column_name_)) {
          old = def;
          replaced = true;
        }
      }
      if (!replaced && OB_FAIL(AiCatalog::instance().pending().push_back(def))) {
      }
    }
  }
  return ret;
}

int ObAiColumnService::append_hidden_columns(ObColumnSchemaV2 &result_column,
                                             const uint64_t hidden_ids[3],
                                             ObIArray<ObColumnSchemaV2> &resolved_cols) {
  int ret = OB_SUCCESS;
  AiDef *found = nullptr;
  {
    lib::ObMutexGuard guard(AiCatalog::instance().lock());
    for (int64_t i = 0; i < AiCatalog::instance().pending().count(); ++i) {
      AiDef &def = AiCatalog::instance().pending().at(i);
      if (0 == def.column_name_.case_compare(result_column.get_column_name_str())) {
        found = &def;
        break;
      }
    }
  }
  if (OB_ISNULL(found)) {
  } else {
    ObColumnSchemaV2 state_col, request_col, spec_col;
    if (OB_FAIL(fill_int_col(state_col, hidden_ids[0], found->state_name_))) {
    } else if (OB_FAIL(fill_varchar_col(request_col, hidden_ids[1], found->request_name_))) {
    } else if (OB_FAIL(fill_int_col(spec_col, hidden_ids[2], found->spec_name_))) {
    } else if (OB_FAIL(resolved_cols.push_back(state_col))) {
    } else if (OB_FAIL(resolved_cols.push_back(request_col))) {
    } else if (OB_FAIL(resolved_cols.push_back(spec_col))) {
    }
  }
  return ret;
}

int ObAiColumnService::build_hidden_alter_columns(const ObColumnSchemaV2 &result_column,
                                                   const ObTableSchema *base_schema,
                                                   ObIArray<AlterColumnSchema> &hidden_cols) {
  int ret = OB_SUCCESS;
  const AiDef *found = nullptr;
  {
    lib::ObMutexGuard guard(AiCatalog::instance().lock());
    for (int64_t i = 0; i < AiCatalog::instance().pending().count(); ++i) {
      const AiDef &def = AiCatalog::instance().pending().at(i);
      if (0 == def.column_name_.case_compare(result_column.get_column_name_str())) {
        found = &AiCatalog::instance().pending().at(i);
      }
    }
  }
  if (OB_ISNULL(found)) {
  } else {
    uint64_t base = 16;
    if (OB_NOT_NULL(base_schema)) {
      for (int64_t i = 0; i < base_schema->get_column_count(); ++i) {
        const ObColumnSchemaV2 *col = base_schema->get_column_schema_by_idx(i);
        if (OB_NOT_NULL(col) && col->get_column_id() >= base) {
          base = col->get_column_id() + 1;
        }
      }
    }
    ObString names[3] = {found->state_name_, found->request_name_, found->spec_name_};
    for (int i = 0; OB_SUCC(ret) && i < 3; ++i) {
      AlterColumnSchema col;
      col.reset();
      col.alter_type_ = OB_DDL_ADD_COLUMN;
      col.set_column_id(base + i);
      col.set_nullable(true);
      col.set_is_hidden(true);
      if (1 == i) {
        col.set_data_type(ObVarcharType);
        col.set_data_length(256);
        col.set_charset_type(CHARSET_UTF8MB4);
        col.set_collation_type(CS_TYPE_UTF8MB4_BIN);
      } else {
        col.set_data_type(ObIntType);
      }
      if (OB_FAIL(col.set_column_name(names[i]))) {
      } else if (OB_FAIL(hidden_cols.push_back(col))) {
      }
    }
  }
  return ret;
}

int ObAiColumnService::flush_pending(ObExecContext &ctx,
                                     const ObString &database_name,
                                     const ObString &table_name,
                                     uint64_t table_id_hint) {
  int ret = OB_SUCCESS;
  UNUSED(ctx);
  ObSchemaGetterGuard guard;
  if (OB_NOT_NULL(GCTX.schema_service_)) {
    int refresh_ret = GCTX.schema_service_->refresh_and_add_schema();
    if (OB_SUCCESS != refresh_ret) {
      LOG_WARN("refresh schema before ai column flush failed", K(refresh_ret));
    }
  }
  if (OB_FAIL(load_guard(guard))) {
  } else {
    lib::ObMutexGuard glock(AiCatalog::instance().lock());
    for (int64_t i = 0; OB_SUCC(ret) && i < AiCatalog::instance().pending().count(); ++i) {
      AiDef &def = AiCatalog::instance().pending().at(i);
      if (def.flushed_) {
        continue;
      }
      if (!database_name.empty() && 0 != def.database_name_.case_compare(database_name)) {
        continue;
      }
      if (!table_name.empty() && 0 != def.table_name_.case_compare(table_name)) {
        continue;
      }
      const ObTableSchema *schema = nullptr;
      if (OB_FAIL(lookup_table(guard, def.database_name_, def.table_name_, table_id_hint, schema))) {
      } else if (OB_ISNULL(schema)) {
        ret = OB_ERR_UNEXPECTED;
      } else if (0 != schema->get_table_name_str().case_compare(def.table_name_)) {
        continue;
      } else if (OB_FAIL(resolve_ids_from_schema(def, *schema))) {
      } else if (OB_FAIL(persist_def(def))) {
      } else {
        def.flushed_ = true;
        AiCatalog::instance().defs().push_back(def);
      }
    }
  }
  return ret;
}

int ObAiColumnService::resolve_retry_alter(const ParseNode &action_list,
                                           const ObTableSchema *schema,
                                           uint64_t session_id,
                                           bool &handled) {
  int ret = OB_SUCCESS;
  handled = false;
  int retry_cnt = 0;
  int other_cnt = 0;
  ParseNode *retry = nullptr;
  for (int64_t i = 0; i < action_list.num_child_; ++i) {
    ParseNode *action = action_list.children_[i];
    if (OB_ISNULL(action)) {
    } else if (T_RETRY_AI_COLUMN == action->type_) {
      ++retry_cnt;
      retry = action;
    } else {
      ++other_cnt;
    }
  }
  if (0 == retry_cnt) {
  } else if (other_cnt > 0 || retry_cnt > 1) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "combining AI COLUMN RETRY with another ALTER action");
  } else if (OB_ISNULL(retry) || retry->num_child_ < 1 || OB_ISNULL(retry->children_[0]) ||
             OB_ISNULL(retry->children_[0]->str_value_)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(ensure_catalog())) {
  } else {
    ObString column(retry->children_[0]->str_len_, retry->children_[0]->str_value_);
    const AiDef *def = nullptr;
    if (OB_ISNULL(schema)) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      lib::ObMutexGuard guard(AiCatalog::instance().lock());
      for (int64_t i = 0; i < AiCatalog::instance().defs().count(); ++i) {
        const AiDef &d = AiCatalog::instance().defs().at(i);
        if (d.table_id_ == schema->get_table_id() && 0 == d.column_name_.case_compare(column)) {
          def = &AiCatalog::instance().defs().at(i);
        }
      }
      if (OB_ISNULL(def)) {
        // column exists but is not an AI column, or unknown name
        const ObColumnSchemaV2 *col = schema->get_column_schema(column);
        if (OB_ISNULL(col)) {
          ret = OB_ERR_BAD_FIELD_ERROR;
        } else {
          ret = OB_NOT_SUPPORTED;
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "retrying a column that is not an AI column");
        }
      } else {
        ObSqlString where;
        if (retry->num_child_ > 1 && OB_NOT_NULL(retry->children_[1])) {
          if (OB_FAIL(print_retry_expr(retry->children_[1], def->state_name_, where))) {
          }
        }
        if (OB_SUCC(ret)) {
          g_retry_pending = true;
          g_retry_session = session_id;
          snprintf(g_retry_db, sizeof(g_retry_db), "%.*s",
                   def->database_name_.length(), def->database_name_.ptr());
          snprintf(g_retry_table, sizeof(g_retry_table), "%.*s",
                   def->table_name_.length(), def->table_name_.ptr());
          snprintf(g_retry_column, sizeof(g_retry_column), "%.*s",
                   def->column_name_.length(), def->column_name_.ptr());
          snprintf(g_retry_where, sizeof(g_retry_where), "%.*s",
                   where.length(), where.ptr() ? where.ptr() : "");
          handled = true;
        }
      }
    }
  }
  return ret;
}

int ObAiColumnService::execute_pending_retry(ObExecContext &ctx, bool &handled) {
  int ret = OB_SUCCESS;
  handled = false;
  if (!g_retry_pending) {
  } else {
    handled = true;
    g_retry_pending = false;
    if (OB_FAIL(ensure_catalog())) {
    } else {
      const AiDef *def = nullptr;
      {
        lib::ObMutexGuard guard(AiCatalog::instance().lock());
        for (int64_t i = 0; i < AiCatalog::instance().defs().count(); ++i) {
          const AiDef &d = AiCatalog::instance().defs().at(i);
          if (0 == d.database_name_.case_compare(g_retry_db) &&
              0 == d.table_name_.case_compare(g_retry_table) &&
              0 == d.column_name_.case_compare(g_retry_column)) {
            def = &AiCatalog::instance().defs().at(i);
          }
        }
      }
      if (OB_ISNULL(def)) {
        ret = OB_ERR_UNEXPECTED;
      } else {
        char req_buf[64];
        int64_t req_len = 0;
        ObString request = make_request_id(req_buf, sizeof(req_buf), req_len);
        ObSqlString select_sql;
        ObMySQLProxy::MySQLResult res;
        if (OB_FAIL(select_sql.append_fmt("SELECT "))) {
        } else {
          ObSchemaGetterGuard guard;
          const ObTableSchema *schema = nullptr;
          if (OB_FAIL(load_guard(guard))) {
          } else if (OB_FAIL(lookup_table(guard, def->database_name_, def->table_name_, def->table_id_, schema))) {
          } else if (OB_ISNULL(schema)) {
            ret = OB_ERR_UNEXPECTED;
          } else {
            bool first = true;
            for (int64_t i = 0; OB_SUCC(ret) && i < schema->get_rowkey_column_num(); ++i) {
              const ObColumnSchemaV2 *pk = schema->get_rowkey_column(i);
              if (OB_ISNULL(pk) || pk->is_hidden()) {
              } else if (!first && OB_FAIL(select_sql.append(", "))) {
              } else if (OB_FAIL(select_sql.append_fmt("`%.*s`", pk->get_column_name_str().length(),
                                                       pk->get_column_name_str().ptr()))) {
              } else {
                first = false;
              }
            }
            if (OB_FAIL(ret)) {
            } else if (OB_FAIL(select_sql.append_fmt(" FROM `%.*s`.`%.*s`",
                                                     def->database_name_.length(), def->database_name_.ptr(),
                                                     def->table_name_.length(), def->table_name_.ptr()))) {
            } else if (g_retry_where[0] != '\0' && OB_FAIL(select_sql.append(" WHERE "))) {
            } else if (g_retry_where[0] != '\0' && OB_FAIL(select_sql.append(g_retry_where))) {
            } else if (OB_FAIL(sql_proxy_read(select_sql, res))) {
            } else if (OB_ISNULL(res.get_result())) {
              ret = OB_ERR_UNEXPECTED;
            } else {
              common::sqlclient::ObMySQLResult *result = res.get_result();
              while (OB_SUCC(ret) && OB_SUCC(result->next())) {
                ObSqlString predicate;
                bool pred_first = true;
                for (int64_t i = 0; OB_SUCC(ret) && i < schema->get_rowkey_column_num(); ++i) {
                  const ObColumnSchemaV2 *pk = schema->get_rowkey_column(i);
                  if (OB_ISNULL(pk) || pk->is_hidden()) {
                    continue;
                  }
                  char name_buf[128];
                  snprintf(name_buf, sizeof(name_buf), "%.*s",
                           pk->get_column_name_str().length(), pk->get_column_name_str().ptr());
                  int64_t iv = 0;
                  ObString sv;
                  if (!pred_first && OB_FAIL(predicate.append(" AND "))) {
                  } else if (OB_FAIL(predicate.append_fmt("`%.*s` = ",
                                                         pk->get_column_name_str().length(),
                                                         pk->get_column_name_str().ptr()))) {
                  } else if (OB_SUCC(result->get_int(name_buf, iv))) {
                    ret = predicate.append_fmt("%ld", iv);
                  } else {
                    ret = OB_SUCCESS;
                    if (OB_FAIL(result->get_varchar(name_buf, sv))) {
                    } else if (OB_FAIL(quote_literal(predicate, sv))) {
                    }
                  }
                  pred_first = false;
                }
                if (OB_FAIL(ret)) {
                } else {
                  ObSqlString upd;
                  int64_t affected = 0;
                  char req_local[64];
                  int64_t req_local_len = 0;
                  ObString row_request = make_request_id(req_local, sizeof(req_local), req_local_len);
                  if (OB_FAIL(upd.append_fmt(
                      "UPDATE `%.*s`.`%.*s` SET `%.*s` = NULL, `%.*s` = %d, `%.*s` = ",
                      def->database_name_.length(), def->database_name_.ptr(),
                      def->table_name_.length(), def->table_name_.ptr(),
                      def->column_name_.length(), def->column_name_.ptr(),
                      def->state_name_.length(), def->state_name_.ptr(), AI_COL_ROW_PENDING,
                      def->request_name_.length(), def->request_name_.ptr()))) {
                  } else if (OB_FAIL(quote_literal(upd, row_request))) {
                  } else if (OB_FAIL(upd.append_fmt(", `%.*s` = %ld WHERE ",
                                                    def->spec_name_.length(), def->spec_name_.ptr(),
                                                    def->def_version_))) {
                  } else if (OB_FAIL(upd.append(predicate.string()))) {
                  } else if (OB_FAIL(sql_proxy_write(upd, affected))) {
                  } else if (affected > 0) {
                    ObSqlString ins;
                    if (OB_FAIL(ins.append(
                        "INSERT INTO oceanbase.__all_ai_column_task(task_id, table_id, column_id, "
                        "request_id, rowkey_predicate, state, retry_count, max_retries, eligible_ts, "
                        "def_version, payload, database_name, table_name) VALUES ("))) {
                    } else if (OB_FAIL(ins.append_fmt("%ld, %lu, %lu, ",
                                                      next_task_id(), def->table_id_, def->column_id_))) {
                    } else if (OB_FAIL(quote_literal(ins, row_request))) {
                    } else if (OB_FAIL(ins.append(", "))) {
                    } else if (OB_FAIL(quote_literal(ins, predicate.string()))) {
                    } else if (OB_FAIL(ins.append_fmt(", %d, 0, %ld, now(6), %ld, NULL, ",
                                                      AI_COL_TASK_PENDING, def->max_retries_, def->def_version_))) {
                    } else if (OB_FAIL(quote_literal(ins, def->database_name_))) {
                    } else if (OB_FAIL(ins.append(", "))) {
                    } else if (OB_FAIL(quote_literal(ins, def->table_name_))) {
                    } else if (OB_FAIL(ins.append(")"))) {
                    } else if (OB_FAIL(sql_proxy_write(ins, affected))) {
                    }
                  }
                }
              }
              if (OB_ITER_END == ret) {
                ret = OB_SUCCESS;
              }
            }
          }
        }
      }
    }
  }
  UNUSED(ctx);
  return ret;
}

int ObAiColumnService::on_insert_row(ObTableModifyOp &op, const ObDMLBaseCtDef &ctdef) {
  int ret = OB_SUCCESS;
  if (g_ai_depth > 0 || !ctdef.is_primary_index_) {
    return ret;
  }
  if (OB_FAIL(ensure_catalog())) {
  } else {
    ObSchemaGetterGuard guard;
    const ObTableSchema *schema = nullptr;
    uint64_t table_id = ctdef.das_base_ctdef_.index_tid_;
    bool has = false;
    {
      lib::ObMutexGuard glock(AiCatalog::instance().lock());
      for (int64_t i = 0; i < AiCatalog::instance().defs().count(); ++i) {
        if (AiCatalog::instance().defs().at(i).table_id_ == table_id) {
          has = true;
        }
      }
    }
    if (!has) {
    } else if (OB_FAIL(load_guard(guard))) {
    } else if (OB_FAIL(guard.get_table_schema(table_id, schema))) {
    } else if (OB_ISNULL(schema)) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      ObSqlString predicate;
      char req_buf[64];
      int64_t req_len = 0;
      ObString request = make_request_id(req_buf, sizeof(req_buf), req_len);
      if (OB_FAIL(build_rowkey_predicate(op, ctdef, true, *schema, predicate))) {
      } else if (OB_FAIL(insert_tasks_for_row(op, *schema, predicate.string(), request, nullptr, 0))) {
      }
    }
  }
  return ret;
}

int read_assigned_request(ObTableModifyOp &op,
                          const ObDMLBaseCtDef &ctdef,
                          uint64_t request_col_id,
                          char *buf,
                          int64_t buf_len,
                          ObString &out) {
  int ret = OB_SUCCESS;
  out.reset();
  int64_t idx = -1;
  for (int64_t j = 0; j < ctdef.column_ids_.count(); ++j) {
    if (ctdef.column_ids_.at(j) == request_col_id) {
      idx = j;
      break;
    }
  }
  ObDatum *datum = nullptr;
  if (idx < 0 || idx >= ctdef.new_row_.count() || OB_ISNULL(ctdef.new_row_.at(idx))) {
  } else if (OB_FAIL(ctdef.new_row_.at(idx)->eval(op.get_eval_ctx(), datum))) {
  } else if (OB_ISNULL(datum) || datum->is_null() || datum->get_string().empty()) {
  } else {
    ObString raw = datum->get_string();
    int64_t n = raw.length();
    if (n >= buf_len) {
      n = buf_len - 1;
    }
    if (n > 0 && OB_NOT_NULL(raw.ptr())) {
      MEMCPY(buf, raw.ptr(), n);
      out.assign_ptr(buf, n);
    }
  }
  return ret;
}

int ObAiColumnService::on_update_row(ObTableModifyOp &op, const ObDMLBaseCtDef &ctdef) {
  int ret = OB_SUCCESS;
  if (g_ai_depth > 0 || !ctdef.is_primary_index_) {
    return ret;
  }
  if (OB_FAIL(ensure_catalog())) {
  } else {
    ObSchemaGetterGuard guard;
    const ObTableSchema *schema = nullptr;
    uint64_t table_id = ctdef.das_base_ctdef_.index_tid_;
    uint64_t changed_ids[16];
    int64_t changed_cnt = 0;
    uint64_t request_col = 0;
    if (OB_FAIL(load_guard(guard))) {
    } else if (OB_FAIL(guard.get_table_schema(table_id, schema))) {
    } else if (OB_ISNULL(schema)) {
    } else {
      lib::ObMutexGuard glock(AiCatalog::instance().lock());
      for (int64_t i = 0; i < AiCatalog::instance().defs().count(); ++i) {
        const AiDef &def = AiCatalog::instance().defs().at(i);
        if (def.table_id_ == table_id && depend_changed(op, ctdef, *schema, def)) {
          if (changed_cnt < 16) {
            changed_ids[changed_cnt++] = def.column_id_;
          }
          if (0 == request_col) {
            request_col = def.request_column_id_;
          }
        }
      }
    }
    if (OB_SUCC(ret) && changed_cnt > 0 && OB_NOT_NULL(schema)) {
      ObSqlString predicate;
      ObSqlString old_predicate;
      char req_buf[128];
      ObString request;
      if (OB_FAIL(read_assigned_request(op, ctdef, request_col, req_buf, sizeof(req_buf), request))) {
      } else if (request.empty()) {
        int64_t req_len = 0;
        request = make_request_id(req_buf, sizeof(req_buf), req_len);
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(build_rowkey_predicate(op, ctdef, false, *schema, old_predicate))) {
      } else if (OB_FAIL(build_rowkey_predicate(op, ctdef, true, *schema, predicate))) {
      } else if (0 != old_predicate.string().compare(predicate.string())) {
        ObSqlString del_old;
        if (OB_FAIL(del_old.append_fmt(
            "DELETE FROM oceanbase.__all_ai_column_task WHERE table_id = %lu AND rowkey_predicate = ",
            table_id))) {
        } else if (OB_FAIL(quote_literal(del_old, old_predicate.string()))) {
        } else if (OB_FAIL(nested_write(op, del_old))) {
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(insert_tasks_for_row(op, *schema, predicate.string(), request,
                                              changed_ids, changed_cnt))) {
      }
    }
  }
  return ret;
}

int ObAiColumnService::on_delete_row(ObTableModifyOp &op, const ObDMLBaseCtDef &ctdef) {
  int ret = OB_SUCCESS;
  if (g_ai_depth > 0 || !ctdef.is_primary_index_) {
    return ret;
  }
  uint64_t table_id = ctdef.das_base_ctdef_.index_tid_;
  bool has = false;
  if (OB_FAIL(ensure_catalog())) {
  } else {
    lib::ObMutexGuard glock(AiCatalog::instance().lock());
    for (int64_t i = 0; i < AiCatalog::instance().defs().count(); ++i) {
      if (AiCatalog::instance().defs().at(i).table_id_ == table_id) {
        has = true;
      }
    }
  }
  if (!has || OB_FAIL(ret)) {
  } else {
    ObSchemaGetterGuard guard;
    const ObTableSchema *schema = nullptr;
    ObSqlString predicate;
    ObSqlString sql;
    if (OB_FAIL(load_guard(guard))) {
    } else if (OB_FAIL(guard.get_table_schema(table_id, schema))) {
    } else if (OB_ISNULL(schema)) {
    } else if (OB_FAIL(build_rowkey_predicate(op, ctdef, false, *schema, predicate))) {
    } else if (OB_FAIL(sql.append_fmt(
        "DELETE FROM oceanbase.__all_ai_column_task WHERE table_id = %lu AND rowkey_predicate = ",
        table_id))) {
    } else if (OB_FAIL(quote_literal(sql, predicate.string()))) {
    } else if (OB_FAIL(nested_write(op, sql))) {
    }
  }
  return ret;
}

int ObAiColumnService::collect_update_assigns(const ObTableSchema &table_schema,
                                                const ObTableAssignment &table_assign,
                                                ObIArray<ObAiAssignSpec> &assigns,
                                                ObString &request_id) {
  int ret = OB_SUCCESS;
  request_id.reset();
  if (OB_FAIL(ensure_catalog())) {
  } else {
    char req_buf[64];
    int64_t req_len = 0;
    ObString request = make_request_id(req_buf, sizeof(req_buf), req_len);
    if (OB_FAIL(AiCatalog::instance().copy_str(request, request_id))) {
    }
    lib::ObMutexGuard glock(AiCatalog::instance().lock());
    for (int64_t i = 0; OB_SUCC(ret) && i < AiCatalog::instance().defs().count(); ++i) {
      const AiDef &def = AiCatalog::instance().defs().at(i);
      if (def.table_id_ != table_schema.get_table_id()) {
        continue;
      }
      bool hit = false;
      for (int64_t a = 0; OB_SUCC(ret) && a < table_assign.assignments_.count(); ++a) {
        const ObAssignment &as = table_assign.assignments_.at(a);
        if (OB_ISNULL(as.column_expr_) || as.is_implicit_) {
          continue;
        }
        uint64_t cid = as.column_expr_->get_column_id();
        if (cid == def.column_id_) {
          ret = OB_NOT_SUPPORTED;
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "assigning an AI column directly");
        }
        const ObColumnSchemaV2 *assigned = table_schema.get_column_schema(cid);
        if (OB_NOT_NULL(assigned)) {
          if (assigned->get_rowkey_position() > 0) {
            hit = true;
          }
          ObString name = assigned->get_column_name_str();
          const char *p = def.depend_names_.ptr();
          int64_t remain = def.depend_names_.length();
          while (!hit && remain > 0 && OB_NOT_NULL(p)) {
            const char *comma = static_cast<const char *>(memchr(p, ',', remain));
            int64_t len = comma ? static_cast<int64_t>(comma - p) : remain;
            if (0 == ObString(len, p).case_compare(name)) {
              hit = true;
            }
            if (OB_ISNULL(comma)) {
              break;
            }
            p = comma + 1;
            remain = def.depend_names_.length() - static_cast<int64_t>(p - def.depend_names_.ptr());
          }
        }
      }
      if (!hit || OB_FAIL(ret)) {
        continue;
      }
      uint64_t ids[4] = {def.column_id_, def.state_column_id_, def.request_column_id_, def.spec_column_id_};
      int kinds[4] = {0, 1, 2, 3};
      int64_t ints[4] = {0, AI_COL_ROW_PENDING, 0, def.def_version_};
      for (int k = 0; OB_SUCC(ret) && k < 4; ++k) {
        if (0 == ids[k] || OB_INVALID_ID == ids[k]) {
          continue;
        }
        ObAiAssignSpec spec;
        spec.column_id_ = ids[k];
        spec.kind_ = kinds[k];
        spec.int_value_ = ints[k];
        spec.str_value_ = (2 == kinds[k]) ? request_id : ObString();
        if (OB_FAIL(assigns.push_back(spec))) {
        }
      }
    }
  }
  return ret;
}

int ObAiColumnService::strict_check_sql(ObSQLSessionInfo &session, const ObString &sql) {
  int ret = OB_SUCCESS;
  int64_t strict = 0;
  if (OB_FAIL(session.get_sys_variable_by_name("ai_column_strict_read", strict))) {
    ret = OB_SUCCESS;
    strict = 0;
  }
  if (0 == strict || sql.empty()) {
    return OB_SUCCESS;
  }
  if (OB_FAIL(ensure_catalog())) {
    return OB_SUCCESS;
  }
  lib::ObMutexGuard glock(AiCatalog::instance().lock());
  for (int64_t i = 0; OB_SUCC(ret) && i < AiCatalog::instance().defs().count(); ++i) {
    const AiDef &def = AiCatalog::instance().defs().at(i);
    bool table_hit = ident_in_sql(sql, def.table_name_);
    bool col_hit = ident_in_sql(sql, def.column_name_);
    if (table_hit && col_hit) {
      ObSqlString q;
      ObMySQLProxy::MySQLResult res;
      if (OB_FAIL(q.append_fmt(
          "SELECT 1 FROM `%.*s`.`%.*s` WHERE `%.*s` IS NULL OR `%.*s` <> %d LIMIT 1",
          def.database_name_.length(), def.database_name_.ptr(),
          def.table_name_.length(), def.table_name_.ptr(),
          def.state_name_.length(), def.state_name_.ptr(),
          def.state_name_.length(), def.state_name_.ptr(),
          AI_COL_ROW_READY))) {
      } else if (OB_FAIL(sql_proxy_read(q, res))) {
      } else if (OB_ISNULL(res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
      } else if (OB_FAIL(res.get_result()->next())) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
        }
      } else {
        ret = OB_AI_COLUMN_NOT_READY;
        LOG_USER_ERROR(OB_AI_COLUMN_NOT_READY, def.table_id_, def.column_id_);
      }
    }
  }
  return ret;
}

void ObAiColumnService::start_worker() {
  static std::atomic<int> started(0);
  if (0 == started.exchange(1)) {
    pthread_t tid;
    pthread_create(&tid, nullptr, worker_main, nullptr);
    pthread_detach(tid);
  }
}

} // namespace sql
} // namespace oceanbase
