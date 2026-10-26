/*
 * Copyright (c) 2025 OceanBase.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#define USING_LOG_PREFIX SQL_ENG

#include "sql/engine/basic/ob_function_table_op.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"
#include "sql/engine/basic/file_sql_core.h"
#include "lib/time/ob_time_utility.h"
#include "common/timezone/ob_time_convert.h"
#include <limits.h>
#include <string.h>


namespace oceanbase
{
using namespace common;
namespace sql
{

OB_SERIALIZE_MEMBER((ObFunctionTableSpec, ObOpSpec), value_expr_, column_exprs_, has_correlated_expr_,
                    file_sql_kind_, file_path_, file_header_, file_delim_, file_quote_, file_escape_,
                    file_nullstr_, file_json_format_, file_size_, file_mtime_sec_, file_mtime_nsec_,
                    file_names_, file_ordinals_);

int ObFunctionTableOp::inner_open()
{
  int ret = OB_SUCCESS;
  node_idx_ = 0;
  already_calc_ = false;
  if (OB_ISNULL(MY_SPEC.value_expr_)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (ObExtendType == MY_SPEC.value_expr_->datum_meta_.type_) {
    next_row_func_ = &ObFunctionTableOp::inner_get_next_row_udf;
  } else {
    next_row_func_ = &ObFunctionTableOp::inner_get_next_row_sys_func;
  }
  return ret;
}

int ObFunctionTableOp::inner_rescan()
{
  int ret = OB_SUCCESS;
  file_sql_close();
  if (OB_FAIL(ObOperator::inner_rescan())) {
  } else {
    node_idx_ = 0;
    if (MY_SPEC.has_correlated_expr_) {
      row_count_ = 0;
      col_count_ = 0;
      value_table_ = NULL;
      already_calc_ = false;
    }
  }
  return ret;
}

int ObFunctionTableOp::inner_close()
{
  int ret = OB_SUCCESS;
  file_sql_close();
  node_idx_ = 0;
  row_count_ = 0;
  col_count_ = 0;
  value_table_ = NULL;
  return ret;
}

void ObFunctionTableOp::destroy()
{
  file_sql_close();
  ObOperator::destroy();
}


int ObFunctionTableOp::get_current_result(ObObj &result)
{
  int ret = OB_SUCCESS;
  void *data = NULL;
  CK (already_calc_);
  if (node_idx_ < 0 || node_idx_ >= row_count_ * col_count_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get current result in table function",
              K(node_idx_), K(row_count_), K(col_count_));
  }
  do {
    CK (node_idx_ >= 0);
    if (OB_SUCC(ret) && node_idx_ >= row_count_) {
      ret = OB_ITER_END;
    }
    CK (OB_NOT_NULL(value_table_));
    OX (data = value_table_->get_data());
    CK (OB_NOT_NULL(data));
    OX (result = (static_cast<ObObj*>(data))[node_idx_++]);
  } while (OB_SUCC(ret) && result.get_meta().get_type() == ObMaxType);
  return ret;
}

int ObFunctionTableOp::inner_get_next_row()
{
  return (this->*next_row_func_)();
}

int ObFunctionTableOp::inner_get_next_row_udf()
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx *plan_ctx = nullptr;
  clear_evaluated_flag();
  if (OB_ISNULL(plan_ctx = ctx_.get_physical_plan_ctx())) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_ISNULL(MY_SPEC.value_expr_)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (ObExtendType != MY_SPEC.value_expr_->datum_meta_.type_) {
    ret = OB_ERR_UNEXPECTED;
  } else if (FALSE_IT(plan_ctx->set_autoinc_id_tmp(0))) {
  } else if (OB_FAIL(ctx_.check_status())) {
  } else {
    ObDatum *value = nullptr;
    if (!already_calc_) {
      if (OB_FAIL(MY_SPEC.value_expr_->eval(eval_ctx_, value))) {
      } else if (value->is_null()) {
        //do nothing
      } else if (OB_ISNULL(value_table_ 
                 = reinterpret_cast<pl::ObPLCollection*>(value->get_ext()))) {
        ret = OB_ERR_UNEXPECTED;
      } else {
        row_count_ = value_table_->is_inited() ? value_table_->get_count() : 0;
        col_count_ = value_table_->get_column_count();
      }
      already_calc_ = true;
    }
    if (OB_SUCC(ret) && OB_UNLIKELY(node_idx_ >= row_count_)) {
      ret = OB_ITER_END;
    }
    CK (MY_SPEC.column_exprs_.count() >= col_count_);
    ObObj obj_stack[col_count_];
    if (OB_SUCC(ret)) {
      if (nullptr != value_table_ 
          && ObExtendType == value_table_->get_element_type().get_obj_type()) {
        pl::ObPLComposite *composite = NULL;
        pl::ObPLRecord *record = NULL;
        ObObj record_obj;
        OZ (get_current_result(record_obj));
        if (OB_FAIL(ret)) {
        } else if (ObUserDefinedSQLType == record_obj.get_type()) {
          obj_stack[0] = record_obj;   
        } else if (record_obj.is_pl_extend()) {
          CK (OB_NOT_NULL(composite = reinterpret_cast<pl::ObPLComposite*>(record_obj.get_ext())));
          if (OB_SUCC(ret)) {
            if (composite->is_record()) {
              OX (record = static_cast<pl::ObPLRecord*>(composite));
              CK (record->get_count() == col_count_);
              for (int64_t i = 0; OB_SUCC(ret) && i < col_count_; ++i) {
                OZ (record->get_element(i, obj_stack[i]));
              }
            } else if (composite->is_collection()) {
              OX (obj_stack[0] = record_obj);
            } else {
              ret = OB_ERR_UNEXPECTED;
            }
          }
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("unexpected here", K(ret), K(record_obj), K(record_obj.meta_));
        }
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < col_count_; ++i) {
          if (OB_FAIL(get_current_result(obj_stack[i]))) {
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      for (int64_t i = 0; OB_SUCC(ret) && i < col_count_; ++i) {
        if (obj_stack[i].is_null()) {
          MY_SPEC.column_exprs_.at(i)->locate_datum_for_write(eval_ctx_).set_null();
        } else {
          const ObObjDatumMapType &datum_map = MY_SPEC.column_exprs_.at(i)->obj_datum_map_;
          ObExpr * const &expr = MY_SPEC.column_exprs_.at(i);
          ObDatum &datum = expr->locate_datum_for_write(eval_ctx_);
          if (OB_FAIL(datum.from_obj(obj_stack[i], datum_map))) {
          } else if (is_lob_storage(obj_stack[i].get_type()) &&
                     OB_FAIL(ob_adjust_lob_datum(get_exec_ctx(), obj_stack[i],
                                                 expr->obj_meta_, datum_map,
                                                 get_exec_ctx().get_allocator(), datum))) {
          }
        }
        if (OB_SUCC(ret)) {
          MY_SPEC.column_exprs_.at(i)->set_evaluated_projected(eval_ctx_);
        }
      }
    }
  }
  return ret;
}

int ObFunctionTableOp::inner_get_next_row_sys_func()
{
  int ret = OB_SUCCESS;
  if (MY_SPEC.file_sql_kind_ != 0) {
    return file_sql_next_row();
  }
  ObPhysicalPlanCtx *plan_ctx = nullptr;
  ObDatum *value = nullptr;
  clear_evaluated_flag();
  if (OB_ISNULL(plan_ctx = ctx_.get_physical_plan_ctx())) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(ctx_.check_status())) {
  } else if (OB_FAIL(MY_SPEC.value_expr_->eval(eval_ctx_, value))) {
    if (OB_ITER_END != ret) {
    }
  } else {
    MY_SPEC.column_exprs_.at(0)->locate_datum_for_write(eval_ctx_).set_datum(*value);
    MY_SPEC.column_exprs_.at(0)->set_evaluated_projected(eval_ctx_);
  }
  return ret;
}




void ObFunctionTableOp::file_sql_close()
{
  if (NULL != file_reader_) {
    fs_close(file_reader_);
    file_reader_ = NULL;
  }
}

int ObFunctionTableOp::file_sql_next_row()
{
  int ret = OB_SUCCESS;
  clear_evaluated_flag();
  if (MY_SPEC.file_sql_kind_ == 0) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(ctx_.check_status())) {
  } else {
    bool enabled = false;
    if (OB_ISNULL(ctx_.get_my_session())) {
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_FAIL(ctx_.get_my_session()->get_enable_file_sql(enabled))) {
    } else if (!enabled) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "file sql is disabled");
    }
  }
  if (OB_SUCC(ret) && NULL == file_reader_) {
    char path[PATH_MAX];
    char err[256];
    err[0] = 0;
    if (MY_SPEC.file_path_.length() <= 0 || MY_SPEC.file_path_.length() >= PATH_MAX) {
      ret = OB_INVALID_ARGUMENT;
    } else {
      MEMCPY(path, MY_SPEC.file_path_.ptr(), MY_SPEC.file_path_.length());
      path[MY_SPEC.file_path_.length()] = '\0';
      FsOptions opt;
      fs_options_init(&opt);
      opt.header = MY_SPEC.file_header_ ? 1 : 0;
      opt.delim = static_cast<char>(MY_SPEC.file_delim_);
      opt.quote = static_cast<char>(MY_SPEC.file_quote_);
      opt.escape = static_cast<char>(MY_SPEC.file_escape_);
      opt.sample_size = -1;
      opt.expect_size = MY_SPEC.file_size_;
      opt.expect_mtime_sec = MY_SPEC.file_mtime_sec_;
      opt.expect_mtime_nsec = MY_SPEC.file_mtime_nsec_;
      if (MY_SPEC.file_nullstr_.length() > 0) {
        opt.nullstr = MY_SPEC.file_nullstr_.ptr();
        opt.nullstr_len = MY_SPEC.file_nullstr_.length();
      }
      if (MY_SPEC.file_json_format_.length() > 0) {
        opt.json_format = MY_SPEC.file_json_format_.ptr();
        opt.json_format_len = MY_SPEC.file_json_format_.length();
      }
      const int ncol = MY_SPEC.column_exprs_.count();
      FsCol cols[512];
      memset(cols, 0, sizeof(cols));
      const char *np = MY_SPEC.file_names_.ptr();
      int nleft = MY_SPEC.file_names_.length();
      const char *optr = MY_SPEC.file_ordinals_.ptr();
      int oleft = MY_SPEC.file_ordinals_.length();
      for (int i = 0; OB_SUCC(ret) && i < ncol; ++i) {
        if (i >= 512 || oleft < (int)sizeof(int32_t) || nleft <= 0) {
          ret = OB_ERR_UNEXPECTED;
          break;
        }
        int32_t ord = 0;
        MEMCPY(&ord, optr, sizeof(ord));
        optr += sizeof(ord);
        oleft -= sizeof(ord);
        const char *sep = (const char *)memchr(np, '\x1f', nleft);
        int nlen = sep ? (int)(sep - np) : nleft;
        if (nlen >= (int)sizeof(cols[i].name)) nlen = (int)sizeof(cols[i].name) - 1;
        MEMCPY(cols[i].name, np, nlen);
        cols[i].name_len = nlen;
        cols[i].ordinal = ord;
        ObObjType tp = MY_SPEC.column_exprs_.at(i)->obj_meta_.get_type();
        if (tp == ObTinyIntType) cols[i].type = FS_BOOL;
        else if (tp == ObIntType) cols[i].type = FS_BIGINT;
        else if (tp == ObDoubleType) cols[i].type = FS_DOUBLE;
        else if (tp == ObDateType) cols[i].type = FS_DATE;
        else if (tp == ObDateTimeType) cols[i].type = FS_DATETIME;
        else if (tp == ObNullType) cols[i].type = FS_NULL;
        else cols[i].type = FS_VARCHAR;
        if (sep) { nleft -= nlen + 1; np = sep + 1; }
        else nleft = 0;
      }
      if (OB_SUCC(ret)) {
        int irc = fs_open(MY_SPEC.file_sql_kind_, path, &opt, cols, ncol, &file_reader_, err, sizeof(err));
        if (irc != FS_OK) {
          ret = OB_INVALID_ARGUMENT;
          LOG_USER_ERROR(OB_NOT_SUPPORTED, err[0] ? err : "open file");
        }
      }
    }
  }
  FsValue vals[512];
  memset(vals, 0, sizeof(vals));
  if (OB_SUCC(ret)) {
    int irc = fs_next(file_reader_, vals, MY_SPEC.column_exprs_.count());
    if (irc == FS_END) {
      ret = OB_ITER_END;
    } else if (irc != FS_OK) {
      ret = (irc == FS_CHANGED) ? OB_ERR_UNEXPECTED : OB_INVALID_ARGUMENT;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, fs_error(file_reader_));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < MY_SPEC.column_exprs_.count(); ++i) {
    ObExpr *expr = MY_SPEC.column_exprs_.at(i);
    ObDatum &datum = expr->locate_datum_for_write(eval_ctx_);
    if (vals[i].is_null || expr->obj_meta_.get_type() == ObNullType) {
      datum.set_null();
    } else if (expr->obj_meta_.get_type() == ObIntType || expr->obj_meta_.get_type() == ObTinyIntType) {
      datum.set_int(vals[i].i64);
    } else if (expr->obj_meta_.get_type() == ObDoubleType) {
      datum.set_double(vals[i].f64);
    } else if (expr->obj_meta_.get_type() == ObDateType) {
      int32_t dv = 0;
      if (OB_FAIL(ObTimeConverter::str_to_date(ObString(vals[i].str_len, vals[i].str), dv))) {
      } else {
        datum.set_date(dv);
      }
    } else if (expr->obj_meta_.get_type() == ObDateTimeType) {
      char tmp[64];
      int n = vals[i].str_len;
      if (n >= (int)sizeof(tmp)) n = (int)sizeof(tmp) - 1;
      if (n > 0 && vals[i].str) MEMCPY(tmp, vals[i].str, n);
      tmp[n] = 0;
      for (int k = 0; k < n; ++k) if (tmp[k] == 'T') tmp[k] = ' ';
      int64_t us = 0;
      ObTimeConvertCtx cv(ctx_.get_my_session()->get_timezone_info(), false);
      if (OB_FAIL(ObTimeConverter::str_to_datetime(ObString(n, tmp), cv, us))) {
      } else {
        datum.set_datetime(us);
      }
    } else {
      int n = vals[i].str_len;
      char *buf = expr->get_str_res_mem(eval_ctx_, n > 0 ? n : 1);
      if (OB_ISNULL(buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else {
        if (n > 0 && vals[i].str) MEMCPY(buf, vals[i].str, n);
        datum.set_string(buf, n);
      }
    }
    if (OB_SUCC(ret)) {
      expr->set_evaluated_projected(eval_ctx_);
    }
  }
  return ret;
}

} // end namespace sql
} // end namespace oceanbase
