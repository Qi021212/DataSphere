# sql_compiler/planner.py

from __future__ import annotations
from typing import List, Any, Dict, Tuple, Optional
import re

# =========================================================
# ExecutionPlan
# =========================================================

class ExecutionPlan:
    def __init__(self, plan_type: str, details: Dict[str, Any]):
        self.plan_type = plan_type
        self.details = details

    def __repr__(self) -> str:
        return f"ExecutionPlan(type={self.plan_type}, details_keys={list(self.details.keys())})"

    def explain(self, indent: int = 0) -> str:
        pad = "  " * indent

        if self.plan_type == "Explain":
            inner = self.details.get("inner_plan")
            head = f"{pad}Explain:"
            if isinstance(inner, ExecutionPlan):
                return head + "\n" + inner.explain(indent + 1)
            return head + " <empty>"

        cols = self.details.get('columns') or []
        aggs = self.details.get('aggregates') or []
        cols_str = ", ".join(cols) if isinstance(cols, list) else str(cols)
        aggs_str = ", ".join(
            f"{a.get('func')}({a.get('arg')})" + (f" AS {a.get('alias')}" if a.get('alias') else "")
            for a in aggs
        )

        suffix_parts = []
        if cols_str:
            suffix_parts.append(cols_str)
        if aggs_str:
            suffix_parts.append(f"[Agg: {aggs_str}]")
        suffix = "  ".join(suffix_parts)

        out = f"{pad}{self.plan_type}: {suffix}"

        if self.plan_type == "Select":
            cond = self.details.get("condition")
            if cond:
                out += f"  [Filter: {cond}]"

            gb = self.details.get("group_by")
            if gb:
                out += f"  [GroupBy: {gb}]"

            ob = self.details.get("order_by")
            if ob:
                dir_ = self.details.get("order_direction")
                out += f"  [OrderBy: {ob}{(' ' + dir_) if dir_ else ''}]"

            out += "\n" + self._explain_table(self.details["table_source"], indent + 1)
        return out

    def _explain_table(self, node: Dict[str, Any], indent: int) -> str:
        pad = "  " * indent
        t = node["type"]
        if t == "TableScan":
            cond = node.get("condition")
            s = f"{pad}SeqScan({node['table_name']}"
            if cond:
                s += f", cond={cond}"
            s += ")"
            return s
        elif t == "Join":
            s = f"{pad}Join(cond={node['condition']})"
            left = self._explain_table(node["left"], indent + 1)
            right = self._explain_table(node["right"], indent + 1)
            return s + "\n" + left + "\n" + right
        return f"{pad}{t}"

# =========================================================
# 工具函数
# =========================================================

_COMPARISON_OPS = ["<>", ">=", "<=", "!=", "=", ">", "<"]

def _is_quoted_string(s: str) -> bool:
    s = s.strip()
    return len(s) >= 2 and s[0] == "'" and s[-1] == "'"

def _strip_quotes(s: str) -> str:
    return s[1:-1] if _is_quoted_string(s) else s

def _as_constant(v: Any) -> Dict[str, Any]:
    if isinstance(v, int):
        return {'type': 'constant', 'value_type': 'int', 'value': v}
    if isinstance(v, float):
        return {'type': 'constant', 'value_type': 'float', 'value': v}
    s = str(v).strip()
    if _is_quoted_string(s):
        return {'type': 'constant', 'value_type': 'string', 'value': _strip_quotes(s)}
    try:
        if '.' in s:
            return {'type': 'constant', 'value_type': 'float', 'value': float(s)}
        return {'type': 'constant', 'value_type': 'int', 'value': int(s)}
    except Exception:
        return {'type': 'constant', 'value_type': 'string', 'value': s}

def _as_column(name: str) -> Dict[str, Any]:
    return {'type': 'column', 'value': name}

_pred_re = re.compile(r"""^\s*(?P<left>[A-Za-z_][\w\.]*)\s*(?P<op><>|>=|<=|!=|=|>|<)\s*(?P<right>.+?)\s*$""", re.X)

def _parse_simple_condition(expr: Optional[str]) -> Optional[Dict[str, Any]]:
    if not expr or not str(expr).strip():
        return None
    m = _pred_re.match(expr.strip("() "))
    if not m:
        return None
    left, op, right_raw = m.group("left").strip(), m.group("op").strip(), m.group("right").strip()
    right = _as_column(right_raw) if re.match(r"^[A-Za-z_][\w\.]*$", right_raw) and not _is_quoted_string(right_raw) else _as_constant(right_raw)
    return {'left': _as_column(left), 'operator': op, 'right': right}

def _values_to_executor_format(row: List[Any]) -> List[Tuple[str, Any]]:
    out: List[Tuple[str, Any]] = []
    for v in row:
        if isinstance(v, int):
            out.append(("int", v))
        elif isinstance(v, float):
            out.append(("float", v))
        else:
            out.append(("string", str(v)))
    return out

# ---- 解析聚合表达式（来自 parser 的 select_items 里是字符串 SQL 片段）----
_agg_re = re.compile(r"^\s*(COUNT|SUM|AVG)\s*\(\s*(\*|[A-Za-z_][\w\.]*)\s*\)\s*$", re.I)

def _parse_aggregate(expr: str, alias: Optional[str]) -> Optional[Dict[str, Any]]:
    m = _agg_re.match(expr or "")
    if not m:
        return None
    func = m.group(1).upper()
    arg = m.group(2)
    return {'func': func, 'arg': arg, 'alias': alias}

# =========================================================
# Planner
# =========================================================

class Planner:
    def _build_table_source(self, base_table: str, base_alias: Optional[str], joins: List[Tuple[str, Optional[str], str]]) -> Dict[str, Any]:
        left_ts = {'type': 'TableScan', 'table_name': base_table}
        if base_alias: left_ts['alias'] = base_alias
        current = left_ts
        for right_tbl, right_alias, on_sql in (joins or []):
            right_ts = {'type': 'TableScan', 'table_name': right_tbl}
            if right_alias: right_ts['alias'] = right_alias
            cond = _parse_simple_condition(on_sql) or {'left': _as_column('0'),'operator': '=','right': _as_constant(1)}
            current = {'type': 'Join','join_type': 'INNER','left': current,'right': right_ts,'condition': cond}
        return current

    def _split_columns_and_aggregates(self, items) -> Tuple[List[str], List[Dict[str, Any]]]:
        columns: List[str] = []
        aggregates: List[Dict[str, Any]] = []
        for expr, alias in (items or []):
            expr_s = str(expr).strip()
            agg = _parse_aggregate(expr_s, alias)
            if agg:
                aggregates.append(agg)
            else:
                if alias:
                    columns.append(f"{expr_s} AS {alias}")
                else:
                    columns.append(expr_s)
        return columns, aggregates

    # --- 谓词下推 ---
    def _predicate_pushdown(self, table_source: Dict[str, Any], condition: Optional[str]) -> Dict[str, Any]:
        if not condition:
            return table_source
        parts = [c.strip() for c in re.split(r"\bAND\b", condition, flags=re.I)]
        for cond_sql in parts:
            cond = _parse_simple_condition(cond_sql)
            if not cond:
                continue
            left_col = cond['left']['value']
            target = table_source
            while target['type'] == 'Join':
                left_alias = target['left'].get('alias') or target['left'].get('table_name', '')
                right_alias = target['right'].get('alias') or target['right'].get('table_name', '')
                if left_col.startswith(left_alias + "."):
                    target = target['left']
                elif left_col.startswith(right_alias + "."):
                    target = target['right']
                else:
                    break
            if target['type'] == 'TableScan':
                target['condition'] = cond
        return table_source

    # --- 生成执行计划 ---
    def generate_plan(self, ast_node) -> ExecutionPlan:
        node_t = type(ast_node).__name__

        # EXPLAIN
        if node_t == "ExplainNode":
            inner_plan = self.generate_plan(ast_node.inner)
            return ExecutionPlan('Explain', {'inner_plan': inner_plan})

        # CREATE TABLE
        if node_t == "CreateTableNode":
            cols = [{'name': n, 'type': t} for (n, t) in ast_node.columns]
            details = {
                'table_name': ast_node.table_name,
                'columns': cols,
                'primary_key': getattr(ast_node, 'primary_key', None),
                'constraints': getattr(ast_node, 'constraints', []) or []
            }
            return ExecutionPlan('CreateTable', details)

        # INSERT（支持多行）
        if node_t == "InsertNode":
            all_rows = ast_node.values or []
            values_for_exec: List[List[Tuple[str, Any]]] = [
                _values_to_executor_format(row) for row in all_rows
            ]
            details = {
                'table_name': ast_node.table_name,
                'column_names': ast_node.column_names or [],
                'values': values_for_exec  # 二维：每行若干 (type, value)
            }
            return ExecutionPlan('Insert', details)

        # DELETE
        if node_t == "DeleteNode":
            cond = None
            if getattr(ast_node, 'where_condition', None):
                cond = _parse_simple_condition(ast_node.where_condition)
            details = {'table_name': ast_node.table_name,'condition': cond}
            return ExecutionPlan('Delete', details)

        # UPDATE
        if node_t == "UpdateNode":
            set_clause = []
            for col, v in ast_node.assignments:
                c = _as_constant(v)
                val = {'type': 'constant', 'value_type': c['value_type'], 'value': c['value']}
                set_clause.append((col, val))
            cond = None
            if getattr(ast_node, 'where_condition', None):
                cond = _parse_simple_condition(ast_node.where_condition)
            details = {'table_name': ast_node.table_name,'set_clause': set_clause,'condition': cond}
            return ExecutionPlan('Update', details)

        # SELECT
        if node_t == "SelectNode":
            table_source = self._build_table_source(
                ast_node.from_table,
                getattr(ast_node,'from_alias',None),
                getattr(ast_node,'joins',[]) or []
            )
            columns, aggregates = self._split_columns_and_aggregates(ast_node.select_items)

            # 优化前
            raw = ExecutionPlan('Select', {
                'table_source': table_source,
                'columns': columns,
                'aggregates': aggregates,
                'condition': ast_node.where_condition,
                'group_by': getattr(ast_node, 'group_by', None),
                'order_by': getattr(ast_node, 'order_by', None),
                'order_direction': getattr(ast_node, 'order_direction', None),
            })
            print("逻辑执行计划（优化前）:")
            print(raw.explain())

            # 谓词下推（仅 WHERE）
            optimized_ts = self._predicate_pushdown(table_source, ast_node.where_condition)
            opt = ExecutionPlan('Select', {
                'table_source': optimized_ts,
                'columns': columns,
                'aggregates': aggregates,
                'condition': None,  # 已尽可能下推
                'group_by': getattr(ast_node, 'group_by', None),
                'order_by': getattr(ast_node, 'order_by', None),
                'order_direction': getattr(ast_node, 'order_direction', None),
            })
            print("\n逻辑执行计划（谓词下推后）:")
            print(opt.explain())
            return opt

        raise Exception(f"[执行计划错误] 不支持的 AST 节点类型: {node_t}")
