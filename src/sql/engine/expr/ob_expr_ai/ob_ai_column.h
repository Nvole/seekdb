/*
 * Copyright (c) 2025 OceanBase.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

#ifndef OCEANBASE_SQL_ENGINE_EXPR_OB_AI_COLUMN_H_
#define OCEANBASE_SQL_ENGINE_EXPR_OB_AI_COLUMN_H_

#include "lib/container/ob_array.h"
#include "lib/string/ob_string.h"
#include "sql/parser/parse_node.h"

namespace oceanbase {
namespace share {
namespace schema {
class ObColumnSchemaV2;
class ObTableSchema;
class AlterColumnSchema;
}
}
namespace sql {
class ObDDLResolver;
class ObAlterTableResolver;
class ObDelUpdResolver;
class ObTableModifyOp;
class ObDMLBaseCtDef;
class ObExecContext;
class ObSQLSessionInfo;
struct ObTableAssignment;
struct ObAssignment;
struct TableItem;

enum ObAiColumnFuncType {
  AI_COL_FUNC_NONE = 0,
  AI_COL_FUNC_COMPLETE = 1,
  AI_COL_FUNC_EMBED = 2,
};

enum ObAiColumnRowState {
  AI_COL_ROW_PENDING = 0,
  AI_COL_ROW_READY = 1,
  AI_COL_ROW_FAILED = 2,
};

enum ObAiColumnTaskState {
  AI_COL_TASK_PENDING = 0,
  AI_COL_TASK_RUNNING = 1,
  AI_COL_TASK_RETRY = 3,
  AI_COL_TASK_BUFFERED = 4,
};

struct ObAiAssignSpec {
  uint64_t column_id_;
  int kind_; // 0 null result, 1 pending state, 2 request id, 3 def version
  int64_t int_value_;
  common::ObString str_value_;
};

class ObAiColumnService {
public:
  static int define_column(const common::ObString &database_name,
                           const common::ObString &table_name,
                           share::schema::ObColumnSchemaV2 &column,
                           ParseNode *def_node,
                           common::ObIAllocator &allocator);
  static int append_hidden_columns(share::schema::ObColumnSchemaV2 &result_column,
                                   const uint64_t hidden_ids[3],
                                   common::ObIArray<share::schema::ObColumnSchemaV2> &resolved_cols);
  static int build_hidden_alter_columns(const share::schema::ObColumnSchemaV2 &result_column,
                                         const share::schema::ObTableSchema *base_schema,
                                         common::ObIArray<share::schema::AlterColumnSchema> &hidden_cols);
  static int flush_pending(ObExecContext &ctx,
                           const common::ObString &database_name,
                           const common::ObString &table_name,
                           uint64_t table_id_hint);
  static int resolve_retry_alter(const ParseNode &action_list,
                                 const share::schema::ObTableSchema *schema,
                                 uint64_t session_id,
                                 bool &handled);
  static int collect_update_assigns(const share::schema::ObTableSchema &table_schema,
                                    const ObTableAssignment &table_assign,
                                    common::ObIArray<ObAiAssignSpec> &assigns,
                                    common::ObString &request_id);
  static int execute_pending_retry(ObExecContext &ctx, bool &handled);
  static int on_insert_row(ObTableModifyOp &op, const ObDMLBaseCtDef &ctdef);
  static int on_update_row(ObTableModifyOp &op, const ObDMLBaseCtDef &ctdef);
  static int on_delete_row(ObTableModifyOp &op, const ObDMLBaseCtDef &ctdef);
  static int strict_check_sql(ObSQLSessionInfo &session, const common::ObString &sql);
  static void start_worker();
};

} // namespace sql
} // namespace oceanbase

#endif
