# engine/executor.py
from typing import List, Dict, Any, Optional, Tuple

from storage.file_manager import FileManager
from sql_compiler.planner import ExecutionPlan
from sql_compiler.catalog import Catalog
from engine.storage_engine import StorageEngine


# ---------------- 公用：条件求值 ----------------
def _evaluate_condition(row: Dict[str, Any], condition: Dict[str, Any]) -> bool:
    """
    支持：
      - 'a.b' 与裸列名解析（JOIN 后的前缀键）
      - 常量类型感知（int/float/str），字符串数字自动转数值参与 >/< 比较
      - 操作符：=, >, <, >=, <=, !=, <>
    """
    def _looks_int(s: str) -> bool:
        try:
            int(s); return True
        except Exception:
            return False

    def _looks_float(s: str) -> bool:
        try:
            float(s); return True
        except Exception:
            return False

    def _strip_quotes_if_needed(s: Any) -> Any:
        if isinstance(s, str) and len(s) >= 2 and s[0] == "'" and s[-1] == "'":
            return s[1:-1]
        return s

    def _resolve_column_value(col_name: str, r: Dict[str, Any]):
        # 1) 精确命中
        if col_name in r:
            return r[col_name]
        # 2) 若带点，尝试右半段
        if "." in col_name:
            _, base = col_name.split(".", 1)
            if base in r:
                return r[base]
        # 3) 裸列：后缀唯一匹配
        candidates = [v for k, v in r.items() if k.endswith("." + col_name)]
        if len(candidates) == 1:
            return candidates[0]
        return None

    def extract_value(expr: Dict[str, Any], r: Dict[str, Any]):
        et = expr.get("type")
        if et == "column":
            return _resolve_column_value(expr.get("value"), r)
        elif et == "constant":
            val = _strip_quotes_if_needed(expr.get("value"))
            vt = (expr.get("value_type") or "").lower()
            if vt == "int":
                try: return int(val)
                except Exception: return val
            if vt == "float":
                try: return float(val)
                except Exception: return val
            if vt == "string":
                return str(val)
            # 未声明类型：猜测
            if isinstance(val, str):
                if _looks_int(val): return int(val)
                if _looks_float(val): return float(val)
            return val
        return expr.get("value")

    if not condition:
        return True

    left = condition.get("left", {})
    op = condition.get("operator")
    right = condition.get("right", {})
    lv = extract_value(left, row)
    rv = extract_value(right, row)

    if lv is None or rv is None:
        return False

    # 尽量数值化，便于 >/<
    if not isinstance(lv, (int, float)) and isinstance(lv, str):
        if _looks_int(lv): lv = int(lv)
        elif _looks_float(lv): lv = float(lv)
    if not isinstance(rv, (int, float)) and isinstance(rv, str):
        if _looks_int(rv): rv = int(rv)
        elif _looks_float(rv): rv = float(rv)

    try:
        if     op == "=":   return lv == rv
        elif   op == ">":   return lv > rv if isinstance(lv,(int,float)) and isinstance(rv,(int,float)) else str(lv) > str(rv)
        elif   op == "<":   return lv < rv if isinstance(lv,(int,float)) and isinstance(rv,(int,float)) else str(lv) < str(rv)
        elif   op == ">=":  return lv >= rv if isinstance(lv,(int,float)) and isinstance(rv,(int,float)) else str(lv) >= str(rv)
        elif   op == "<=":  return lv <= rv if isinstance(lv,(int,float)) and isinstance(rv,(int,float)) else str(lv) <= str(rv)
        elif   op in ("!=", "<>"): return lv != rv
        else: return False
    except Exception:
        return False


# ---------------- 执行器 ----------------
class Executor:
    def __init__(self, file_manager: FileManager, catalog: Catalog):
        self.storage_engine = StorageEngine(file_manager)
        self.file_manager = file_manager
        self.catalog = catalog

    def execute(self, plan: ExecutionPlan) -> Any:
        if plan.plan_type == "Explain":
            inner = plan.details.get("inner_plan")
            return inner.explain() if isinstance(inner, ExecutionPlan) else "Explain: <empty>"
        if plan.plan_type == "CreateTable":
            return self.execute_create_table(plan)
        if plan.plan_type == "Insert":
            return self.execute_insert(plan)
        if plan.plan_type == "Select":
            return self.execute_select(plan)
        if plan.plan_type == "Delete":
            return self.execute_delete(plan)
        if plan.plan_type == "Update":
            return self.execute_update(plan)
        raise Exception(f"Unsupported execution plan: {plan.plan_type}")

    # ---------- DDL/DML ----------
    def execute_create_table(self, plan: ExecutionPlan) -> str:
        table_name = plan.details["table_name"]
        columns = plan.details["columns"]
        constraints = plan.details.get("constraints", [])
        # 创建物理文件
        self.file_manager.create_table_file(table_name, columns)
        # 写 catalog（兼容老签名）
        if hasattr(self.catalog, "create_table") and len(self.catalog.create_table.__code__.co_varnames) > 2:
            self.catalog.create_table(table_name, columns, constraints)
        else:
            self.catalog.create_table(table_name, columns)
            if constraints:
                table_info = self.catalog.get_table_info(table_name)
                table_info["constraints"] = constraints
                self.catalog._save_catalog()
        return f"Table '{table_name}' created successfully"

    def execute_insert(self, plan: ExecutionPlan) -> str:
        table_name = plan.details["table_name"]
        column_names = plan.details["column_names"]
        values = plan.details["values"]

        table_info = self.catalog.get_table_info(table_name)
        if not table_info:
            raise Exception(f"Table '{table_name}' does not exist")
        if not column_names:
            column_names = [col["name"] for col in table_info["columns"]]

        # 构建记录
        record = {}
        for i, col_name in enumerate(column_names):
            _, value = values[i]
            record[col_name] = value

        # 外键检查（带智能提示）
        for col_name, value in record.items():
            for constraint in table_info.get("constraints", []):
                if constraint and constraint[0] == "FOREIGN_KEY" and constraint[1] == col_name:
                    _, _, ref_table, ref_col = constraint
                    if not self._check_reference_exists(ref_table, ref_col, value):
                        # —— 智能提示：列出现有候选值 & 修复示例 ——
                        try:
                            existing_rows = self.file_manager.read_records(ref_table)
                            vals = []
                            for r in existing_rows:
                                if ref_col in r:
                                    vals.append(r[ref_col])
                            uniq_vals = sorted(set(vals))[:10]
                            candidates = ", ".join(map(lambda x: repr(x), uniq_vals)) if uniq_vals else "(无现有记录)"
                        except Exception:
                            candidates = "(无法读取引用表候选值)"

                        full_cols = column_names or [c["name"] for c in table_info["columns"]]
                        full_vals = [repr(record[c]) for c in full_cols]

                        # 用现有候选里第一个给出“改用现有键”的示例（若没有候选就保留原值）
                        fallback = repr(uniq_vals[0]) if uniq_vals else repr(value)
                        patched_vals = [
                            (fallback if c == col_name else v)
                            for c, v in zip(full_cols, full_vals)
                        ]

                        msg = (
                            f"智能提示：外键约束失败 —— {table_name}.{col_name}={repr(value)} "
                            f"在 {ref_table}({ref_col}) 中不存在。\n"
                            f"可选修复：\n"
                            f"  方案 A：先向父表插入该键值，再插入当前记录：\n"
                            f"    INSERT INTO {ref_table}({ref_col}/*, 其他列 */) VALUES ({repr(value)}/*, ... */);\n"
                            f"    INSERT INTO {table_name}({', '.join(full_cols)}) VALUES ({', '.join(full_vals)});\n"
                            f"  方案 B：改用父表中已存在的键（候选前若干：{candidates}）：\n"
                            f"    INSERT INTO {table_name}({', '.join(full_cols)}) VALUES ({', '.join(patched_vals)});"
                        )
                        raise Exception(msg)

        ok = self.file_manager.insert_record(table_name, record)
        if not ok:
            raise Exception("Failed to insert record")
        self.catalog.update_row_count(table_name, self.catalog.get_table_info(table_name)["row_count"] + 1)
        self.file_manager.flush_all()
        return f"1 row inserted into '{table_name}'"

    def execute_delete(self, plan: ExecutionPlan) -> str:
        table_name = plan.details["table_name"]
        condition = plan.details["condition"]
        table_info = self.catalog.get_table_info(table_name)
        if not table_info:
            raise Exception(f"Table '{table_name}' does not exist")
        deleted = self.file_manager.delete_records(table_name, condition)
        self.catalog.update_row_count(table_name, max(0, table_info["row_count"] - deleted))
        return f"{deleted} row(s) deleted from '{table_name}'"

    def execute_update(self, plan: ExecutionPlan) -> str:
        table_name = plan.details["table_name"]
        set_clause = plan.details["set_clause"]      # [(col, {'type':'constant','value_type':...,'value':...}), ...]
        condition = plan.details["condition"]

        table_info = self.catalog.get_table_info(table_name)
        if not table_info:
            raise Exception(f"Table '{table_name}' does not exist")

        # 列类型驱动的转换
        typed_set_clause: List[Tuple[str, Any]] = []
        name2type = {c["name"]: c["type"] for c in table_info["columns"]}
        for col_name, value_dict in set_clause:
            if col_name not in name2type:
                raise Exception(f"Column '{col_name}' does not exist in table '{table_name}'")
            typ = name2type[col_name]
            val = value_dict["value"]
            if typ == "INT":
                val = int(val)
            elif typ == "FLOAT":
                val = float(val)
            else:
                val = str(val)
            typed_set_clause.append((col_name, val))

        typed_condition = condition
        if condition:
            left_col = condition["left"]["value"]
            r_val = condition["right"]["value"]
            col_typ = name2type.get(left_col)
            if col_typ == "INT":
                typed_condition["right"]["value"] = int(r_val)
            elif col_typ == "FLOAT":
                typed_condition["right"]["value"] = float(r_val)

        updated = self.file_manager.update_records(table_name, typed_set_clause, typed_condition)

        # 简单级联（只在 WHERE 为等值时）
        if updated > 0 and condition and condition.get("operator") == "=":
            where_col = condition["left"]["value"]
            old_value = condition["right"]["value"]
            for set_col, new_value_dict in set_clause:
                if set_col == where_col:
                    refs = self.catalog.find_referencing_tables(table_name, set_col)
                    for ref_table, ref_col in refs:
                        cascade_plan = ExecutionPlan("Update", {
                            "table_name": ref_table,
                            "set_clause": [(ref_col, new_value_dict)],
                            "condition": {
                                "left": {"type": "column", "value": ref_col},
                                "operator": "=",
                                "right": {"type": "constant", "value_type": "string", "value": old_value},
                            }
                        })
                        self.execute_update(cascade_plan)

        return f"Updated {updated} row(s)"

    # ---------- SELECT ----------
    def execute_select(self, plan: ExecutionPlan) -> List[Dict[str, Any]]:
        ts_plan = plan.details["table_source"]
        columns: List[str] = plan.details.get("columns") or []
        aggregates: List[Dict[str, Any]] = plan.details.get("aggregates") or []
        group_by: Optional[str] = plan.details.get("group_by")
        order_by: Optional[str] = plan.details.get("order_by")
        order_dir: Optional[str] = plan.details.get("order_direction")

        # 1) 执行表源（包含谓词下推）
        raw = self._execute_table_source(ts_plan)

        # 2) 残余 WHERE 过滤（非常重要）
        residual = plan.details.get("condition")
        if residual:
            raw = [r for r in raw if _evaluate_condition(r, residual)]

        # 3) 聚合 or 普通投影/排序
        if aggregates:
            return self._execute_aggregates(raw, aggregates, group_by, order_by, order_dir)

        rows = [self._project_row(row, columns) for row in raw]
        if order_by:
            rows = self._order_rows(rows, order_by, order_dir)
        return rows

    # ---------- 内部：表源执行 with 谓词下推 ----------
    def _execute_table_source(self, ts_plan: Dict) -> List[Dict[str, Any]]:
        t = ts_plan["type"]
        if t == "TableScan":
            table = ts_plan["table_name"]
            alias = ts_plan.get("alias")
            cond = ts_plan.get("condition")  # 谓词下推
            base_rows = self.file_manager.read_records(table, cond) if cond else self.file_manager.read_records(table)

            # 无论是否有别名，都生成“带前缀键”（别名或表名）+ “裸键”
            prefix = (alias or table)
            out = []
            for r in base_rows:
                nr = {}
                for k, v in r.items():
                    nr[k] = v                       # 裸列
                    nr[f"{prefix}.{k}"] = v         # 前缀列
                out.append(nr)
            return out

        if t == "Join":
            return self._execute_join(ts_plan)

        raise Exception(f"Unsupported table source type: {t}")

    def _execute_join(self, join_plan: Dict) -> List[Dict[str, Any]]:
        join_type = join_plan["join_type"]
        left_plan = join_plan["left"]
        right_plan = join_plan["right"]
        join_condition = join_plan["condition"]

        left_rows = self._execute_table_source(left_plan)
        right_rows = self._execute_table_source(right_plan)

        results: List[Dict[str, Any]] = []

        if join_type == "INNER":
            for l in left_rows:
                for r in right_rows:
                    combined = {**l, **r}
                    if _evaluate_condition(combined, join_condition):
                        results.append(combined)
            return results

        if join_type == "LEFT":
            # 尝试确定需要补全的右表列（考虑右表为空的情况）
            right_cols = set(right_rows[0].keys()) if right_rows else set()
            if not right_cols and right_plan["type"] == "TableScan":
                rt = right_plan["table_name"]
                rp = right_plan.get("alias") or rt
                meta = self.catalog.get_table_info(rt) or {"columns": []}
                right_cols = {f"{rp}.{c['name']}" for c in meta["columns"]} | {c['name'] for c in meta["columns"]}

            for l in left_rows:
                matched = False
                for r in right_rows:
                    combined = {**l, **r}
                    if _evaluate_condition(combined, join_condition):
                        results.append(combined)
                        matched = True
                if not matched:
                    combined = dict(l)
                    for c in right_cols:
                        combined.setdefault(c, None)
                    results.append(combined)
            return results

        raise Exception(f"Unsupported join type: {join_type}")

    # ---------- 内部：聚合/分组/排序/投影 ----------
    def _resolve_col_from_row(self, row: Dict[str, Any], col: str):
        if col in row:
            return row[col]
        if "." in col:
            _, base = col.split(".", 1)
            if base in row:
                return row[base]
        hits = [v for k, v in row.items() if k.endswith("." + col)]
        if len(hits) == 1:
            return hits[0]
        return None

    def _execute_aggregates(self,
                            rows: List[Dict[str, Any]],
                            aggregates: List[Dict[str, Any]],
                            group_by: Optional[str],
                            order_by: Optional[str],
                            order_dir: Optional[str]) -> List[Dict[str, Any]]:
        # 规范化聚合项：{'func','arg','alias'}
        aggs = [{"func": a.get("func").upper(),
                 "arg": a.get("arg"),
                 "alias": a.get("alias")} for a in aggregates]

        if group_by:
            # 分组键（单列）
            groups: Dict[Any, List[Dict[str, Any]]] = {}
            for row in rows:
                key = self._resolve_col_from_row(row, group_by)
                groups.setdefault(key, []).append(row)

            out: List[Dict[str, Any]] = []
            for gkey, bucket in groups.items():
                one = {}
                one[group_by] = gkey  # 输出分组列，以 group_by 原样为键
                for a in aggs:
                    val = self._agg_bucket(bucket, a["func"], a["arg"])
                    out_key = a["alias"] or f"{a['func']}({a['arg']})"
                    one[out_key] = val
                out.append(one)

            if order_by:
                out = self._order_rows(out, order_by, order_dir)
            return out

        # 无分组：全表聚合 -> 单行
        res: Dict[str, Any] = {}
        for a in aggs:
            val = self._agg_bucket(rows, a["func"], a["arg"])
            out_key = a["alias"] or f"{a['func']}({a['arg']})"
            res[out_key] = val
        return [res]

    def _agg_bucket(self, bucket: List[Dict[str, Any]], func: str, arg: str) -> Any:
        if func == "COUNT" and arg == "*":
            return len(bucket)
        vals: List[float] = []
        for row in bucket:
            v = self._resolve_col_from_row(row, arg)
            if v is None:
                continue
            if func in ("SUM", "AVG"):
                try:
                    v = float(v)
                except Exception:
                    continue
            vals.append(v)
        if func == "COUNT":
            return len(vals)
        if func == "SUM":
            return sum(vals) if vals else 0
        if func == "AVG":
            return (sum(vals) / len(vals)) if vals else 0
        raise Exception(f"Unsupported aggregate function: {func}")

    def _parse_select_column_item(self, item: str) -> Tuple[str, Optional[str]]:
        """
        解析 SELECT 列形态为 (expr, alias)：
          "col"
          "t.col"
          "expr AS alias"（AS 不区分大小写）
        """
        s = item.strip()
        parts = s.split()
        if len(parts) >= 3 and parts[-2].upper() == "AS":
            alias = parts[-1]
            expr = " ".join(parts[:-2])
            return expr, alias
        return s, None

    def _project_row(self, row: Dict[str, Any], columns: List[str]) -> Dict[str, Any]:
        if not columns or columns == ["*"]:
            return dict(row)
        out: Dict[str, Any] = {}
        for col_item in columns:
            if col_item == "*":
                out.update(row)
                continue
            expr, alias = self._parse_select_column_item(col_item)
            val = self._resolve_col_from_row(row, expr)
            out[alias or expr] = val
        return out

    def _order_rows(self, rows: List[Dict[str, Any]], key_name: str, direction: Optional[str]) -> List[Dict[str, Any]]:
        rev = (isinstance(direction, str) and direction.upper() == "DESC")
        # 缺值放后
        def _key(r):
            v = r.get(key_name)
            return (1, None) if v is None else (0, v)
        return sorted(rows, key=_key, reverse=rev)

    # ---------- 其他 ----------
    def _check_reference_exists(self, table_name, column_name, value):
        table_info = self.catalog.get_table_info(table_name)
        if not table_info:
            return False
        cond = {
            "left":  {"type": "column", "value": column_name},
            "operator": "=",
            "right": {"type": "constant", "value_type": "string", "value": str(value)}
        }
        records = self.file_manager.read_records(table_name, cond)
        return len(records) > 0
