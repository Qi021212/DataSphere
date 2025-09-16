# SQL 语义分析器：检查 AST 的语义正确性（表存在、列存在、类型匹配等）+ 智能提示
from typing import List, Tuple, Optional, Dict
from sql_compiler.catalog import Catalog  # ✅ 复用统一的目录类（持久化）
import re

class SemanticError(Exception):
    """语义错误异常"""
    pass

# ---------------- 内部工具：安全解析条件字符串 ----------------
_COMPARISON_OPS = ["<>", ">=", "<=", "=", "!=", ">", "<"]  # 注意顺序：长的在前
_LOGICAL_AND = "AND"
_LOGICAL_OR  = "OR"
_LOGICAL_NOT = "NOT"

# 识别聚合：COUNT/SUM/AVG（COUNT(*) 支持）
_AGG_RE = re.compile(r"^\s*(COUNT|SUM|AVG)\s*\(\s*(\*|[A-Za-z_][\w\.]*)\s*\)\s*$", re.I)

def _is_aggregate_expr(expr: str) -> bool:
    return bool(_AGG_RE.match(expr or ""))

def _parse_aggregate(expr: str) -> Tuple[str, str]:
    """
    返回 (FUNC, ARG)；如 ('COUNT', '*') / ('SUM', 'age') / ('AVG', 't.col')
    调用方保证 expr 已匹配聚合。
    """
    m = _AGG_RE.match(expr or "")
    func = m.group(1).upper()
    arg = m.group(2)
    return func, arg

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

# ---------------- 新增：类型解析与检查 ----------------
_TYPE_VARCHAR_PARAM_RE = re.compile(r"^VARCHAR\s*\(\s*(\d+)\s*\)$", re.I)

def _parse_type(typ_str: str) -> Tuple[str, Optional[int]]:
    """
    将类型字符串解析为 (base_type, length)
      - INT        -> ("INT", None)
      - FLOAT      -> ("FLOAT", None)
      - BOOL       -> ("BOOL", None)
      - VARCHAR    -> ("VARCHAR", None)
      - VARCHAR(n) -> ("VARCHAR", n)
    """
    t = (typ_str or "").strip().upper()
    if t in ("INT", "FLOAT", "BOOL", "VARCHAR"):
        return t, None
    m = _TYPE_VARCHAR_PARAM_RE.match(t)
    if m:
        n = int(m.group(1))
        return "VARCHAR", n
    # 兼容一些空白情况（如 "VARCHAR ( 20 )"）
    if t.startswith("VARCHAR"):
        m2 = _TYPE_VARCHAR_PARAM_RE.match(t.replace(" ", ""))
        if m2:
            return "VARCHAR", int(m2.group(1))
    return t, None  # 交给上层报错

def _is_int_like(v) -> bool:
    return isinstance(v, int) and not isinstance(v, bool)

def _is_float_like(v) -> bool:
    return isinstance(v, float) or _is_int_like(v)

def _type_check_value(base: str, length: Optional[int], value) -> Optional[str]:
    """
    返回 None 表示通过；否则返回错误消息字符串
    INSERT 的值已经是 lexer 转出来的 python 值：
      - NUMBER -> int/float
      - STRING -> str（不含引号）
    """
    if base == "INT":
        if not _is_int_like(value):
            return f"类型不匹配：期望 INT，实际 {type(value).__name__}({value})"
        return None
    if base == "FLOAT":
        if not _is_float_like(value):
            return f"类型不匹配：期望 FLOAT，实际 {type(value).__name__}({value})"
        return None
    if base == "BOOL":
        # 你也可以允许 0/1；这里严格要求 bool
        if not isinstance(value, bool):
            return f"类型不匹配：期望 BOOL，实际 {type(value).__name__}({value})"
        return None
    if base == "VARCHAR":
        if not isinstance(value, str):
            return f"类型不匹配：期望 VARCHAR，实际 {type(value).__name__}({value})"
        if length is not None and len(value) > length:
            return f"长度错误：VARCHAR({length})，实际长度 {len(value)} 的值 '{value}' 超出限制"
        return None
    return f"不支持的数据类型 '{base}'"

class SemanticAnalyzer:
    """语义分析器：检查抽象语法树的正确性（不做持久化！）"""

    def __init__(self, catalog: Catalog):
        self.catalog = catalog  # ✅ 统一目录对象

    def analyze(self, ast_node):
        node_type = type(ast_node).__name__
        if node_type == "ExplainNode":
            # 只分析内部语句；不产生副作用
            return self.analyze(ast_node.inner)
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
        col_bases: Dict[str, Tuple[str, Optional[int]]] = {}

        for name, typ in node.columns:
            if name in seen:
                raise Exception(f"[语义错误] 列 '{name}' 重复定义")
            seen.add(name)

            base, n = _parse_type(typ)
            if base not in ("INT", "VARCHAR", "FLOAT", "BOOL"):
                raise Exception(f"[语义错误] 不支持的数据类型 '{typ}'")
            if base == "VARCHAR" and n is not None:
                if not isinstance(n, int) or n <= 0:
                    raise Exception(f"[语义错误] VARCHAR 长度非法：{typ}（应为正整数）")
            col_bases[name] = (base, n)

        # 表级/列级 PRIMARY KEY：只能定义一次且需引用存在的列
        pk_col = getattr(node, "primary_key", None)
        if pk_col is not None:
            if pk_col not in seen:
                raise Exception(f"[语义错误] 主键列 '{pk_col}' 未在列定义中出现")
        # 如果 AST 支持列级 PRIMARY KEY 多次书写，这里已在 parser 层防重复；
        # 这里再兜底：统计多少列携带 PK（如果你在 AST 中以其他方式存储，可扩展）

        # 外键（如果 AST 中携带）
        for kind, local_col, ref_table, ref_col in getattr(node, "constraints", []):
            if kind != "FOREIGN_KEY":
                continue
            if local_col not in seen:
                raise Exception(f"[语义错误] 外键本地列 '{local_col}' 未定义")
            if not self.catalog.table_exists(ref_table):
                raise Exception(f"[语义错误] 外键引用的表 '{ref_table}' 不存在")
            ref_cols = [c['name'] for c in self.catalog.get_table_info(ref_table)['columns']]
            if ref_col not in ref_cols:
                raise Exception(f"[语义错误] 外键引用的列 '{ref_col}' 不存在于表 {ref_table}")

        return f"[语义正确] 创建表 {node.table_name} 可行"

    # ---------- INSERT ----------
    def _analyze_insert(self, node):
        if not self.catalog.table_exists(node.table_name):
            raise Exception(f"[语义错误] 表 '{node.table_name}' 不存在")

        tbl = self.catalog.get_table_info(node.table_name)
        table_cols = tbl['columns']  # 预计: [{'name':..., 'type':...}, ...]
        table_names = [c['name'] for c in table_cols]
        table_types = [c['type'] for c in table_cols]

        # 将类型解析为 (base, len)
        parsed_types: List[Tuple[str, Optional[int]]] = [_parse_type(t) for t in table_types]

        # 对应列选择
        if node.column_names:
            for col in node.column_names:
                if col not in table_names:
                    raise Exception(f"[语义错误] 列 '{col}' 不存在于表 {node.table_name}")
            ins_types = [parsed_types[table_names.index(c)] for c in node.column_names]
            target_cols = node.column_names
        else:
            ins_types = parsed_types
            target_cols = table_names

        # 行级检查：数量、类型、VARCHAR 长度
        for row in node.values:
            if len(row) != len(ins_types):
                expected = len(target_cols)
                got = len(row)
                # 构造示例：缺省值用 <value>
                fixed_vals = []
                for i in range(len(target_cols)):
                    if i < len(row):
                        v = row[i]
                    else:
                        v = "<value>"
                    base, n = ins_types[i]
                    # 仅示例上加引号（真实执行时由执行器处理）
                    if isinstance(v, str) and base == "VARCHAR":
                        v = f"'{v}'"
                    fixed_vals.append(str(v))
                hint = (
                    "智能提示：INSERT 提供的值数量与列数不一致。\n"
                    f"  期望列({expected}): ({', '.join(target_cols)})\n"
                    f"  实际提供({got}): ({', '.join(map(str, row))})\n"
                    "  示例修复：\n"
                    f"    INSERT INTO {node.table_name}({', '.join(target_cols)}) VALUES ({', '.join(fixed_vals)});"
                )
                raise Exception(hint)

            # 类型与长度检查
            for (base, n), v, colname in zip(ins_types, row, target_cols):
                err = _type_check_value(base, n, v)
                if err:
                    raise Exception(f"[语义错误] 列 '{colname}': {err}")

        return f"[语义正确] 插入 {len(node.values)} 行到 {node.table_name}"

    # ---------- 辅助 ----------
    def _col_exists_in_table(self, table_name: str, col_name: str) -> bool:
        cols = [c['name'] for c in self.catalog.get_table_info(table_name)['columns']]
        return col_name in cols

    def _resolve_column(self, alias_map: Dict[str, str], token: str) -> Tuple[str, str]:
        """
        解析列引用为 (表名, 列名)。会处理歧义与不存在。
        """
        token = _normalize_ident(token)
        if "." in token:
            a, c = token.split(".", 1)
            if a not in alias_map:
                raise Exception(f"[语义错误] 表别名 '{a}' 未定义")
            tbl = alias_map[a]
            if not self._col_exists_in_table(tbl, c):
                raise Exception(f"[语义错误] 列 '{c}' 不存在于表 {tbl}")
            return tbl, c
        else:
            hits = []
            for t in set(alias_map.values()):
                if self._col_exists_in_table(t, token):
                    hits.append(t)
            if len(hits) == 0:
                raise Exception(f"[语义错误] 列 '{token}' 不存在于任何表")
            if len(hits) > 1:
                raise Exception(f"[语义错误] 列 '{token}' 在多个表中存在，需限定表名或别名（歧义）")
            return hits[0], token

    def _check_qualified_or_unqualified_col(self, alias_map, token: str):
        # 仅做存在性/歧义检查
        self._resolve_column(alias_map, token)

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
        # 1) 构建别名映射与表存在性检查
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

        # 2) 拆分选择项：普通列 vs 聚合
        plain_cols: List[str] = []
        aggregates: List[Tuple[str, str]] = []  # (FUNC, ARG)

        for col, _alias in getattr(node, "select_items", []):
            if col == "*":
                plain_cols.append(col)
                continue
            if _is_aggregate_expr(col):
                func, arg = _parse_aggregate(col)
                if not (func == "COUNT" and arg == "*"):
                    # COUNT(col)/SUM(col)/AVG(col) 需要检查列存在
                    self._check_qualified_or_unqualified_col(alias_map, arg)
                    # 数值性检查（仅 SUM/AVG）
                    if func in ("SUM", "AVG"):
                        tbl, c = self._resolve_column(alias_map, arg)
                        cols = {x['name']: x['type'] for x in self.catalog.get_table_info(tbl)['columns']}
                        base, _n = _parse_type(cols[c])
                        if base not in ("INT", "FLOAT"):
                            raise Exception(f"[语义错误] {func} 仅支持数值列，列 '{arg}' 的类型为 {cols[c]}")
                aggregates.append((func, arg))
            else:
                # 普通列：校验存在性/歧义
                self._check_qualified_or_unqualified_col(alias_map, col)
                plain_cols.append(col)

        # 3) 分组规则
        gb = getattr(node, "group_by", None)
        if aggregates:
            if gb:
                # 仅支持单列分组：该列必须存在；所有普通列都必须等于该列（或等价引用）
                self._check_qualified_or_unqualified_col(alias_map, gb)
                gb_norm = _normalize_ident(gb)
                for c in plain_cols:
                    if c == "*":
                        raise Exception("[语义错误] 聚合查询中不支持 SELECT *（请改为明确列或仅使用聚合函数）")
                    if _normalize_ident(c) != gb_norm:
                        raise Exception(f"[语义错误] 非分组列 '{c}' 必须出现在 GROUP BY 中（当前仅支持单列分组 '{gb}'）")
            else:
                # 无分组时，不能混用普通列
                non_star_plain = [c for c in plain_cols if c != "*"]
                if non_star_plain:
                    raise Exception("[语义错误] 存在聚合函数但缺少 GROUP BY；非分组列必须出现在 GROUP BY 中")
                # 同时禁止 '*' 与聚合混用（否则语义不清）
                if "*" in plain_cols:
                    raise Exception("[语义错误] 聚合查询中不支持 SELECT *（请改为明确列或仅使用聚合函数）")
        else:
            # 没有聚合：允许 SELECT * 或普通列
            pass

        # 4) 连接与 WHERE 条件检查
        for _, _, cond in getattr(node, "joins", []):
            self._check_condition_string(alias_map, cond)
        if getattr(node, "where_condition", None):
            self._check_condition_string(alias_map, node.where_condition)

        # 5) ORDER BY（如有）：目前仅保证列存在
        if getattr(node, "order_by", None):
            ob = node.order_by
            if ob != "*":
                self._check_qualified_or_unqualified_col(alias_map, ob)

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
        # 说明：由于 UPDATE 的值可能是列引用或常量，且在 AST 中字符串/标识符都是 str，
        # 这里不强制做类型/长度检查；如需严格，可在 Parser 给值打标签再在此检查。
        return f"[语义正确] 更新表 {node.table_name}"
