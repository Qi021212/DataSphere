# sql_compiler/semantic_analyzer.py
# SQL 语义分析器：检查 AST 的语义正确性（表存在、列存在、类型匹配等）
from typing import List, Tuple, Optional, Dict
from sql_compiler.catalog import Catalog  # ✅ 复用统一的目录类（持久化）

class SemanticError(Exception):
    """语义错误异常"""
    pass

# ---------------- 内部工具：安全解析条件字符串 ----------------
_COMPARISON_OPS = ["<>", ">=", "<=", "=", ">", "<"]  # 注意顺序：长的在前，避免 >= 被先匹配成 >
_LOGICAL_AND = "AND"
_LOGICAL_OR  = "OR"
_LOGICAL_NOT = "NOT"

def _strip_outer_parens(s: str) -> str:
    s = s.strip()
    while s.startswith("(") and s.endswith(")"):
        depth = 0
        ok = True
        for i, ch in enumerate(s):
            if ch == "(":
                depth += 1
            elif ch == ")":
                depth -= 1
                if depth == 0 and i != len(s) - 1:
                    ok = False
                    break
        if ok:
            s = s[1:-1].strip()
        else:
            break
    return s

def _unquote_ident(x: str) -> str:
    x = x.strip()
    if (x.startswith("`") and x.endswith("`")) or (x.startswith('"') and x.endswith('"')):
        return x[1:-1].strip()
    return x

def _normalize_ident(x: str) -> str:
    x = _strip_outer_parens(x)
    x = _unquote_ident(x)
    if "." in x:
        a, b = x.split(".", 1)
        a = _unquote_ident(_strip_outer_parens(a))
        b = _unquote_ident(_strip_outer_parens(b))
        return f"{a.strip()}.{b.strip()}"
    return x.strip()

def _is_quoted_string(s: str) -> bool:
    s = s.strip()
    return (len(s) >= 2 and s[0] == "'" and s[-1] == "'")

def _top_level_split_bool(expr: str) -> List[str]:
    expr = expr.strip()
    items: List[str] = []
    buf: List[str] = []
    depth = 0
    in_str = False

    def push_buf():
        if buf:
            items.append("".join(buf).strip())
            buf.clear()

    i = 0
    n = len(expr)
    while i < n:
        ch = expr[i]
        if in_str:
            buf.append(ch)
            if ch == "'":
                in_str = False
            i += 1
            continue

        if ch == "'":
            in_str = True
            buf.append(ch)
            i += 1
            continue

        if ch == "(":
            depth += 1; buf.append(ch); i += 1; continue
        if ch == ")":
            depth -= 1; buf.append(ch); i += 1; continue

        if depth == 0:
            if expr[i:i+3].upper() == "AND" and (i+3 == n or not expr[i+3].isalpha()):
                push_buf(); items.append(_LOGICAL_AND); i += 3; continue
            if expr[i:i+2].upper() == "OR" and (i+2 == n or not expr[i+2].isalpha()):
                push_buf(); items.append(_LOGICAL_OR); i += 2; continue

        buf.append(ch); i += 1

    push_buf()
    return [x for x in items if x != ""]

def _remove_top_level_not(s: str):
    t = s.lstrip()
    if t.upper().startswith(_LOGICAL_NOT + " "):
        return True, t[len(_LOGICAL_NOT):].lstrip()
    if t.upper() == _LOGICAL_NOT:
        return True, ""
    return False, s

def _split_predicate(s: str):
    s = _strip_outer_parens(s)
    depth = 0
    in_str = False
    pos = -1
    op = None
    i = 0
    n = len(s)
    while i < n:
        ch = s[i]
        if in_str:
            if ch == "'": in_str = False
            i += 1; continue
        if ch == "'":
            in_str = True; i += 1; continue
        if ch == "(":
            depth += 1; i += 1; continue
        if ch == ")":
            depth -= 1; i += 1; continue

        if depth == 0:
            for token in _COMPARISON_OPS:
                L = len(token)
                if s[i:i+L] == token:
                    pos = i; op = token; break
            if op: break
        i += 1

    if op is None or pos < 0:
        raise Exception(f"[语义错误] 无法解析谓词: {s}")

    left = s[:pos].strip()
    right = s[pos+len(op):].strip()
    return left, op, right

class SemanticAnalyzer:
    """语义分析器：检查抽象语法树的正确性（不做持久化！）"""

    def __init__(self, catalog: Catalog):
        self.catalog = catalog  # ✅ 统一目录对象

    def analyze(self, ast_node):
        node_type = type(ast_node).__name__
        if node_type == "CreateTableNode":
            return self._analyze_create_table(ast_node)
        elif node_type == "InsertNode":
            return self._analyze_insert(ast_node)
        elif node_type == "SelectNode":
            return self._analyze_select(ast_node)
        elif node_type == "DeleteNode":
            return self._analyze_delete(ast_node)
        elif node_type == "UpdateNode":
            return self._analyze_update(ast_node)
        else:
            raise Exception(f"[语义错误] 不支持的 AST 节点: {node_type}")

    # ---------- CREATE ----------
    def _analyze_create_table(self, node):
        # 只校验，不写入目录；真正创建由执行器完成
        if self.catalog.table_exists(node.table_name):
            raise Exception(f"[语义错误] 表 '{node.table_name}' 已存在")
        seen = set()
        for name, typ in node.columns:
            if name in seen:
                raise Exception(f"[语义错误] 列 '{name}' 重复定义")
            seen.add(name)
            if typ not in ("INT", "VARCHAR", "FLOAT", "BOOL"):
                raise Exception(f"[语义错误] 不支持的数据类型 '{typ}'")
        return f"[语义正确] 创建表 {node.table_name} 可行"

    # ---------- INSERT ----------
    def _analyze_insert(self, node):
        if not self.catalog.table_exists(node.table_name):
            raise Exception(f"[语义错误] 表 '{node.table_name}' 不存在")
        table_cols = self.catalog.get_table_info(node.table_name)['columns']
        table_names = [c['name'] for c in table_cols]
        table_types = [c['type'] for c in table_cols]

        if node.column_names:
            for col in node.column_names:
                if col not in table_names:
                    raise Exception(f"[语义错误] 列 '{col}' 不存在于表 {node.table_name}")
            ins_types = [table_types[table_names.index(c)] for c in node.column_names]
        else:
            ins_types = table_types

        for row in node.values:
            if len(row) != len(ins_types):
                raise Exception(f"[语义错误] 插入值数量与列数不符")
        return f"[语义正确] 插入 {len(node.values)} 行到 {node.table_name}"

    # ---------- 辅助 ----------
    def _col_exists_in_table(self, table_name: str, col_name: str) -> bool:
        cols = [c['name'] for c in self.catalog.get_table_info(table_name)['columns']]
        return col_name in cols

    def _check_qualified_or_unqualified_col(self, alias_map, token: str):
        token = _normalize_ident(token)
        if "." in token:
            a, c = token.split(".", 1)
            if a not in alias_map:
                raise Exception(f"[语义错误] 表别名 '{a}' 未定义")
            tbl = alias_map[a]
            if not self._col_exists_in_table(tbl, c):
                raise Exception(f"[语义错误] 列 '{c}' 不存在于表 {tbl}")
        else:
            hits = []
            for t in set(alias_map.values()):
                if self._col_exists_in_table(t, token):
                    hits.append(t)
            if len(hits) == 0:
                raise Exception(f"[语义错误] 列 '{token}' 不存在于任何表")
            if len(hits) > 1:
                raise Exception(f"[语义错误] 列 '{token}' 在多个表中存在，需限定表名或别名（歧义）")

    def _check_condition_string(self, alias_map, cond_str: str):
        if not cond_str or not cond_str.strip():
            return
        parts = _top_level_split_bool(cond_str)
        i = 0
        while i < len(parts):
            item = parts[i]
            if item.upper() in (_LOGICAL_AND, _LOGICAL_OR):
                i += 1; continue
            _has_not, body = _remove_top_level_not(item)
            body = _strip_outer_parens(body)
            sub = _top_level_split_bool(body)
            if len(sub) > 1:
                self._check_condition_string(alias_map, body)
            else:
                left, _op, right = _split_predicate(body)
                self._check_qualified_or_unqualified_col(alias_map, left)
                r = right.strip()
                if _is_quoted_string(r):
                    pass
                else:
                    try:
                        float(r)
                    except Exception:
                        if "." in r:
                            self._check_qualified_or_unqualified_col(alias_map, r)
                        else:
                            hits = [t for t in set(alias_map.values()) if self._col_exists_in_table(t, r)]
                            if len(hits) == 0:
                                raise Exception(f"[语义错误] 列或标识符 '{r}' 在条件中未定义")
            i += 1

    # ---------- SELECT ----------
    def _analyze_select(self, node):
        alias_map: Dict[str, str] = {}
        if getattr(node, "from_alias", None):
            alias_map[_normalize_ident(node.from_alias.strip().strip("()"))] = node.from_table
        alias_map[_normalize_ident(node.from_table)] = node.from_table

        for right_table, alias, cond in getattr(node, "joins", []):
            if alias:
                alias_map[_normalize_ident(str(alias).strip().strip("()"))] = right_table
            alias_map[_normalize_ident(right_table)] = right_table

        for tbl in set(alias_map.values()):
            if not self.catalog.table_exists(tbl):
                raise Exception(f"[语义错误] 表 '{tbl}' 不存在")

        for col, _alias in getattr(node, "select_items", []):
            if col == "*": continue
            self._check_qualified_or_unqualified_col(alias_map, col)

        for _, _, cond in getattr(node, "joins", []):
            self._check_condition_string(alias_map, cond)
        if getattr(node, "where_condition", None):
            self._check_condition_string(alias_map, node.where_condition)

        return f"[语义正确] 查询表 {node.from_table}"

    # ---------- DELETE ----------
    def _analyze_delete(self, node):
        if not self.catalog.table_exists(node.table_name):
            raise Exception(f"[语义错误] 表 '{node.table_name}' 不存在")
        return f"[语义正确] 删除表 {node.table_name} 的数据"

    # ---------- UPDATE ----------
    def _analyze_update(self, node):
        if not self.catalog.table_exists(node.table_name):
            raise Exception(f"[语义错误] 表 '{node.table_name}' 不存在")
        names = {c['name']: c['type'] for c in self.catalog.get_table_info(node.table_name)['columns']}
        for col, _val in node.assignments:
            if col not in names:
                raise Exception(f"[语义错误] 列 '{col}' 不存在于表 {node.table_name}")
        return f"[语义正确] 更新表 {node.table_name}"
