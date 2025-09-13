# sql_compiler/parser.py
# 严格 LL(1) 辅助的解析器：先用 FIRST/FOLLOW+预测表做推导可视化（教学），再用递归下降构建 AST
# 调试仿真不会消费实际 tokens

from typing import List, Dict, Set, Optional, Tuple, Any, Callable
from sql_compiler.lexer import Token
from sql_compiler.diag import caret_line, suggest_expected_vs_got, nearest


# ---------------- AST 节点 ----------------
class ASTNode:
    pass


class CreateTableNode(ASTNode):
    def __init__(self, table_name: str, columns: List[Tuple[str, str]], pos: Optional[int] = None):
        self.table_name = table_name
        self.columns = columns
        self.pos = pos

    def __repr__(self):
        cols = ", ".join(f"{n} {t}" for n, t in self.columns)
        return f"CreateTableNode({self.table_name}, [{cols}])"


class InsertNode(ASTNode):
    def __init__(self, table_name: str, column_names: List[str], values: List[List[Any]], pos: Optional[int] = None):
        self.table_name = table_name
        self.column_names = column_names
        self.values = values
        self.pos = pos

    def __repr__(self):
        vals = "; ".join([f"({', '.join(map(str, row))})" for row in self.values])
        cols = f"({', '.join(self.column_names)})" if self.column_names else ""
        return f"InsertNode({self.table_name}{cols}, VALUES {vals})"


class SelectNode(ASTNode):
    def __init__(self, select_items: List[Tuple[str, Optional[str]]], from_table: str,
                 from_alias: Optional[str] = None, pos: Optional[int] = None):
        self.select_items = select_items
        self.from_table = from_table
        self.from_alias = from_alias
        # (right_table, alias, condition_sql)
        self.joins: List[Tuple[str, Optional[str], str]] = []
        self.where_condition: Optional[str] = None
        self.group_by: Optional[str] = None
        self.order_by: Optional[str] = None
        self.order_direction: Optional[str] = None
        self.pos = pos

    def __repr__(self):
        items = ", ".join(c for c, _ in self.select_items)
        j = ""
        if self.joins:
            j = " " + " ".join([f"JOIN {t}{(' ' + a) if a else ''} ON {cond}" for t, a, cond in self.joins])
        w = f" WHERE {self.where_condition}" if self.where_condition else ""
        g = f" GROUP BY {self.group_by}" if self.group_by else ""
        o = f" ORDER BY {self.order_by}{(' ' + self.order_direction) if self.order_direction else ''}" if self.order_by else ""
        return f"SelectNode(SELECT {items} FROM {self.from_table}{(' ' + self.from_alias) if self.from_alias else ''}{j}{w}{g}{o})"


class DeleteNode(ASTNode):
    def __init__(self, table_name: str, pos: Optional[int] = None):
        self.table_name = table_name
        self.where_condition = None
        self.pos = pos

    def __repr__(self):
        w = f" WHERE {self.where_condition}" if self.where_condition else ""
        return f"DeleteNode(DELETE FROM {self.table_name}{w})"


class UpdateNode(ASTNode):
    def __init__(self, table_name: str, assignments: List[Tuple[str, Any]], pos: Optional[int] = None):
        self.table_name = table_name
        self.assignments = assignments
        self.where_condition = None
        self.pos = pos

    def __repr__(self):
        def fmt(v):
            if isinstance(v, str) and not (v.startswith("'") and v.endswith("'")) and "." not in v:
                return f"'{v}'"
            return v
        assigns = ", ".join([f"{c}={fmt(v)}" for c, v in self.assignments])
        w = f" WHERE {self.where_condition}" if self.where_condition else ""
        return f"UpdateNode(UPDATE {self.table_name} SET {assigns}{w})"


# ---------------- token -> 文法符号 映射（用于 LL(1) 仿真） ----------------
def token_to_symbol(tok: Optional[Token]) -> str:
    """把 lexer 的 Token 映射成文法里的终结符（用于 LL1 推导仿真）"""
    if tok is None:
        return "#"
    ttype = getattr(tok, "type", "")
    tval = str(tok.value)

    if ttype == "KEYWORD":
        kw = tval.upper()
        if kw in ("AND", "OR", "NOT", "ASC", "DESC", "SELECT", "FROM", "JOIN", "ON",
                  "WHERE", "GROUP", "BY", "ORDER", "INSERT", "INTO", "VALUES",
                  "CREATE", "TABLE", "DELETE", "UPDATE", "SET", "INT", "VARCHAR"):
            return kw
        return kw
    if ttype == "IDENTIFIER":
        return "IDENTIFIER"
    if ttype == "NUMBER":
        return "NUMBER"
    if ttype == "STRING":
        return "STRING"
    if ttype == "OPERATOR":
        return "OPERATOR"
    if ttype == "DELIMITER":
        # 常见字面量
        if tval in ("(", ")", ",", ";", ".", "*"):
            return tval
        if tval in ("=",):
            return "OPERATOR"
        return tval
    if isinstance(tok.value, str):
        return tok.value.upper()
    return str(tok.value)


# ---------------- FIRST / FOLLOW / 预测表 与 LL(1) 仿真 ----------------
def compute_first_sets(grammar: Dict[str, List[List[str]]], terminals: Set[str]) -> Dict[str, Set[str]]:
    FIRST: Dict[str, Set[str]] = {nt: set() for nt in grammar}
    changed = True
    while changed:
        changed = False
        for A, prods in grammar.items():
            for prod in prods:
                if prod == ["ε"] or len(prod) == 0:
                    if "ε" not in FIRST[A]:
                        FIRST[A].add("ε"); changed = True
                    continue
                add_eps = True
                for X in prod:
                    if X in terminals:
                        if X not in FIRST[A]:
                            FIRST[A].add(X); changed = True
                        add_eps = False
                        break
                    else:
                        for s in FIRST.get(X, set()):
                            if s != "ε" and s not in FIRST[A]:
                                FIRST[A].add(s); changed = True
                        if "ε" in FIRST.get(X, set()):
                            add_eps = True
                        else:
                            add_eps = False
                        if not add_eps:
                            break
                if add_eps:
                    if "ε" not in FIRST[A]:
                        FIRST[A].add("ε"); changed = True
    return FIRST


def compute_follow_sets(grammar: Dict[str, List[List[str]]], start_symbol: str, terminals: Set[str],
                        FIRST: Dict[str, Set[str]]) -> Dict[str, Set[str]]:
    FOLLOW: Dict[str, Set[str]] = {nt: set() for nt in grammar}
    FOLLOW[start_symbol].add("#")
    changed = True
    while changed:
        changed = False
        for A, prods in grammar.items():
            for prod in prods:
                for i, B in enumerate(prod):
                    if B not in grammar:
                        continue
                    beta = prod[i+1:]
                    if not beta:
                        before = len(FOLLOW[B])
                        FOLLOW[B].update(FOLLOW[A])
                        if len(FOLLOW[B]) != before:
                            changed = True
                    else:
                        first_beta = set()
                        contains_eps = True
                        for sym in beta:
                            if sym in terminals:
                                first_beta.add(sym); contains_eps = False; break
                            else:
                                first_beta.update(x for x in FIRST.get(sym, set()) if x != "ε")
                                if "ε" in FIRST.get(sym, set()):
                                    contains_eps = True
                                else:
                                    contains_eps = False; break
                        before = len(FOLLOW[B])
                        FOLLOW[B].update(first_beta)
                        if contains_eps:
                            FOLLOW[B].update(FOLLOW[A])
                        if len(FOLLOW[B]) != before:
                            changed = True
    return FOLLOW


def build_parse_table(grammar: Dict[str, List[List[str]]], terminals: Set[str],
                      FIRST: Dict[str, Set[str]], FOLLOW: Dict[str, Set[str]]):
    table: Dict[Tuple[str, str], List[str]] = {}
    for A, prods in grammar.items():
        for prod in prods:
            first_prod = set()
            if prod == ["ε"] or len(prod) == 0:
                first_prod.add("ε")
            else:
                contains_eps = True
                for sym in prod:
                    if sym in terminals:
                        first_prod.add(sym); contains_eps = False; break
                    else:
                        first_prod.update(x for x in FIRST.get(sym, set()) if x != "ε")
                        if "ε" in FIRST.get(sym, set()):
                            contains_eps = True
                        else:
                            contains_eps = False; break
                if contains_eps:
                    first_prod.add("ε")
            for a in first_prod:
                if a != "ε":
                    key = (A, a)
                    table[key] = prod
            if "ε" in first_prod:
                for b in FOLLOW.get(A, set()):
                    key = (A, b)
                    table[key] = prod
    return table


def ll1_simulate(tokens: List[Token],
                 grammar: Dict[str, List[List[str]]],
                 start_symbol: str,
                 terminals: Set[str],
                 on_expected: Optional[Callable[[List[str]], None]] = None):
    """
    打印 LL(1) 预测分析推导过程（不消费实际 tokens）
    on_expected: 可选回调，把当前非终结符可接受的 FIRST/FOLLOW（即“可能输入集”）传回给外部用于智能诊断。
    """
    FIRST = compute_first_sets(grammar, terminals)
    FOLLOW = compute_follow_sets(grammar, start_symbol, terminals, FIRST)
    table = build_parse_table(grammar, terminals, FIRST, FOLLOW)

    input_symbols = [token_to_symbol(t) for t in tokens] + ["#"]
    stack: List[str] = ["#", start_symbol]

    print("\n[LL1 推导过程]")
    def stack_str():
        return " ".join(stack)
    def input_str():
        return " ".join(input_symbols)

    ip = 0
    while stack:
        top = stack[-1]
        cur = input_symbols[ip] if ip < len(input_symbols) else "#"
        print(f"栈: {stack_str():<70} 输入: {input_str():<70} # 处理: {top}")
        if top == "#":
            if cur == "#":
                print("✅ 输入完整匹配，分析成功")
                return True
            else:
                print(f"❌ 出错: 栈到达底 (#)，但输入尚未结束 -> {cur}")
                return False
        if top in terminals:
            if top == cur:
                stack.pop(); ip += 1; continue
            else:
                tok = tokens[ip] if ip < len(tokens) else None
                if tok:
                    print(f"❌ 出错: 期望 {top}, 实际 {cur} (line={tok.line}, col={tok.column})")
                else:
                    print(f"❌ 出错: 期望 {top}, 实际 EOF")
                return False
        else:
            key = (top, cur)
            prod = table.get(key)
            if prod is None:
                expected = sorted({a for (A, a) in table.keys() if A == top})
                if on_expected:
                    on_expected(list(expected))
                tok = tokens[ip] if ip < len(tokens) else None
                if tok:
                    exp_str = ", ".join(expected) if expected else "N/A"
                    print(f"❌ 出错: 无法从 {top} 推导输入 {cur}")
                    print(f"[语法错误] 期望其中之一: {exp_str}, 实际 {cur} (line={tok.line}, col={tok.column})")
                else:
                    print(f"❌ 出错: 无法从 {top} 推导输入 EOF")
                return False
            # 成功使用产生式时，也把当前 non-terminal 的可选 FIRST/FOLLOW 抛给诊断侧
            if on_expected:
                expected_now = sorted({a for (A, a) in table.keys() if A == top})
                on_expected(list(expected_now))
            prod_str = " ".join(prod) if prod != ["ε"] else "ε"
            print(f"使用产生式: {top} -> {prod_str}")
            stack.pop()
            if prod != ["ε"]:
                for sym in reversed(prod):
                    stack.append(sym)
    return False


# ---------------- 递归下降解析器（真实构建 AST） ----------------
class Parser:
    def __init__(self, tokens: List[Token], source_text: str = ""):
        self.tokens = list(tokens)
        self.pos = 0
        self.source_text = source_text
        self._last_expected: List[str] = []   # LL(1) 仿真阶段采集的“可能项”

    def current_token(self) -> Optional[Token]:
        return self.tokens[self.pos] if self.pos < len(self.tokens) else None

    def _tok_repr(self, tok: Optional[Token]) -> str:
        if not tok:
            return "EOF"
        return f"{tok.value}({tok.type})"

    def _format_err(self, expected: str, tok: Optional[Token]):
        if tok:
            return f"[语法错误] 期望 {expected}, 实际 {tok.value} (line={tok.line}, col={tok.column})"
        else:
            return f"[语法错误] 期望 {expected}, 实际 EOF"

    def _smart_hints(self, expected_list: List[str], got_tok: Optional[Token]) -> str:
        """
        基于“上一个/上上个 token”的上下文来更精准地给出智能提示。
        只在命中明确场景时返回单一提示，避免互相干扰。
        """
        if not got_tok:
            return ""

        # 当前读到的（出错）符号
        got_upper = str(got_tok.value).upper() if isinstance(got_tok.value, str) else str(got_tok.value)
        exp = set(e.upper() for e in (expected_list or []))

        # 上下文：上一个 / 上上个 token
        prev_tok = self.tokens[self.pos - 1] if self.pos - 1 >= 0 else None
        prev2_tok = self.tokens[self.pos - 2] if self.pos - 2 >= 0 else None
        prev_upper = str(prev_tok.value).upper() if (prev_tok and isinstance(prev_tok.value, str)) else None
        prev2_upper = str(prev2_tok.value).upper() if (prev2_tok and isinstance(prev2_tok.value, str)) else None

        # ---- 规则 1：JOIN 之后缺 ON（或直接遇到 WHERE/GROUP/ORDER/;）----
        # 触发条件：预测集中包含 ON，且 got 是 WHERE/GROUP/ORDER/JOIN/;/# 之一
        # 示例：SELECT * FROM a JOIN b WHERE ...  -> 缺 ON
        if "ON" in exp and got_upper in {"WHERE", "GROUP", "ORDER", "JOIN"} or ("ON" in exp and got_upper in {";", "#"}):

            return "智能提示：在 JOIN 之后应有 ON ... 条件，是否缺少连接条件？"

        # ---- 规则 2：ON / WHERE 后缺布尔条件 ----
        # 触发条件：预计应该出现标识符/括号/NOT 等构成条件的起始符，
        # 但实际读到了 JOIN/WHERE/GROUP/ORDER/;/# 等
        # 且“上一个 token”为 ON / WHERE（最强信号）
        if prev_upper in {"ON", "WHERE"}:
            if any(k in exp for k in {"IDENTIFIER", "(", "NOT"}) and got_upper in {"JOIN", "WHERE", "GROUP", "ORDER", ";", "#"}:
                return "智能提示：需要一个布尔条件，例如 a.id = b.sid 或 age > 18"

        # ---- 规则 3：ORDER BY / GROUP BY 后缺列名 ----
        # 强上下文：上一个 token 是 BY，且上上个 token 是 ORDER 或 GROUP
        # got 为 ';' 或 '#'（EOF）或关键字（WHERE/JOIN/GROUP/ORDER）
        if prev_upper == "BY" and prev2_upper in {"ORDER", "GROUP"}:
            if got_upper in {";", "#", "WHERE", "JOIN", "GROUP", "ORDER"} or "IDENTIFIER" in exp:
                return "智能提示：ORDER BY / GROUP BY 后应跟列名，例如 ORDER BY col 或 GROUP BY col"

        # ---- 规则 4：SELECT 列表缺少列（直接读到 FROM）----
        # 宽松触发：当预测集中需要 IDENTIFIER，而实际读到了 FROM
        if "IDENTIFIER" in exp and got_upper == "FROM":
            return "智能提示：是否缺少选择列表？你可以写具体列名或使用 * ，例如 SELECT * FROM ..."

        # 默认不返回（避免误报）
        return ""


    def consume(self, expected: Optional[str] = None) -> Token:
        """
        灵活地消耗 token：
        - expected 为类型名（IDENTIFIER, NUMBER, STRING, OPERATOR, DELIMITER, KEYWORD）时按 tok.type 检查
        - expected 为字面量（如 'SELECT','FROM','(',')',',',';','.' 等）时比较 tok.value（不区分大小写）
        - 若 '=' 作为字面量，而 lexer 把它标为 OPERATOR，也能接受
        """
        tok = self.current_token()
        if not tok:
            raise Exception(self._format_err(expected or "TOKEN", None))

        def _raise(expected_str: str, got_tok: Optional[Token]):
            if got_tok:
                base = self._format_err(expected_str, got_tok)
                caret = caret_line(self.source_text, got_tok.line, got_tok.column, width=len(str(got_tok.value)))
                exp = self._last_expected or ([expected_str] if expected_str else [])
                hint_basic = suggest_expected_vs_got(exp, str(got_tok.value))
                msg = base + ("\n" + caret if caret else "")
                if hint_basic:
                    msg += ("\n" + hint_basic)
                smart = self._smart_hints(exp, got_tok)
                if smart:
                    msg += ("\n" + smart)
                raise Exception(msg)
            else:
                raise Exception(self._format_err(expected_str, None))

        if expected:
            up_exp = str(expected).upper()
            # 类型名
            if up_exp in ("IDENTIFIER", "NUMBER", "STRING", "OPERATOR", "DELIMITER", "KEYWORD"):
                if getattr(tok, "type", "").upper() != up_exp:
                    # 允许把 '=' 作为 OPERATOR
                    if up_exp == "OPERATOR" and str(tok.value) in ("=", ">", "<", "<=", ">=", "<>"):
                        pass
                    else:
                        _raise(expected, tok)
            else:
                # 字面量匹配
                if isinstance(tok.value, str):
                    if tok.value.upper() != up_exp and str(tok.value) != expected:
                        if up_exp == "IDENTIFIER" and getattr(tok, "type", "").upper() == "IDENTIFIER":
                            pass
                        else:
                            _raise(expected, tok)
                else:
                    if str(tok.value) != str(expected):
                        _raise(expected, tok)

        self.pos += 1
        return tok

    # 在每个 parse_xxx 前运行 LL1 仿真并打印（仿真使用当前剩余 token 序列）
    def run_ll1_debug(self, grammar: Dict[str, List[List[str]]], start_symbol: str, terminals: Set[str]):
        def _cap(exp):
            self._last_expected = list(exp)
        try:
            ll1_simulate(self.tokens[self.pos:], grammar, start_symbol, terminals, on_expected=_cap)
        except Exception as e:
            print(f"[LL1 调试异常，已忽略] {e}")

    # 入口
    def parse(self) -> Optional[ASTNode]:
        tok = self.current_token()
        if not tok:
            return None
        kw = str(tok.value).upper() if isinstance(tok.value, str) else str(tok.value)
        if kw == "CREATE":
            return self.parse_create_table()
        if kw == "INSERT":
            return self.parse_insert()
        if kw == "SELECT":
            return self.parse_select()
        if kw == "DELETE":
            return self.parse_delete()
        if kw == "UPDATE":
            return self.parse_update()
        raise Exception(f"[语法错误] 不支持的语句类型: {tok.value} (line={tok.line}, col={tok.column})")

    # ---------------- CREATE ----------------
    def parse_create_table(self) -> CreateTableNode:
        grammar = {
            "CreateStmt": [["CREATE", "TABLE", "IDENTIFIER", "(", "ColumnDefs", ")", ";"]],
            "ColumnDefs": [["IDENTIFIER", "Type", "ColumnDefsTail"]],
            "ColumnDefsTail": [[",", "IDENTIFIER", "Type", "ColumnDefsTail"], ["ε"]],
            "Type": [["INT"], ["VARCHAR"]],
        }
        terminals = {"CREATE", "TABLE", "IDENTIFIER", "(", ")", ",", "INT", "VARCHAR", ";", "#"}
        self.run_ll1_debug(grammar, "CreateStmt", terminals)

        self.consume("CREATE")
        self.consume("TABLE")
        table_tok = self.consume("IDENTIFIER")
        table_name = table_tok.value
        self.consume("(")
        columns: List[Tuple[str, str]] = []
        col_tok = self.consume("IDENTIFIER")
        type_tok = self.consume()  # INT/VARCHAR 可能被标成 KEYWORD/IDENTIFIER
        columns.append((col_tok.value, str(type_tok.value).upper()))
        while self.current_token() and self.current_token().value == ",":
            self.consume(",")
            col_tok = self.consume("IDENTIFIER")
            type_tok = self.consume()
            columns.append((col_tok.value, str(type_tok.value).upper()))
        self.consume(")")
        self.consume(";")
        return CreateTableNode(table_name, columns, pos=table_tok.line)

    # ---------------- INSERT ----------------
    def parse_insert(self) -> InsertNode:
        # ✅ 左因子化后的 LL(1) 文法，用于可视化推导（避免带列名/不带列名两条产生式冲突）
        grammar = {
            "InsertStmt": [["INSERT", "INTO", "IDENTIFIER", "InsertCols", "VALUES", "(", "ValueList", ")", "InsertTail", ";"]],
            "InsertCols": [["(", "ColumnList", ")"], ["ε"]],
            "ColumnList": [["IDENTIFIER", "ColumnListTail"]],
            "ColumnListTail": [[",", "IDENTIFIER", "ColumnListTail"], ["ε"]],
            "ValueList": [["Value", "ValueListTail"]],
            "ValueListTail": [[",", "Value", "ValueListTail"], ["ε"]],
            "Value": [["NUMBER"], ["STRING"], ["IDENTIFIER"]],
            "InsertTail": [[",", "(", "ValueList", ")", "InsertTail"], ["ε"]],
        }
        terminals = {
            "INSERT", "INTO", "IDENTIFIER", "(", ")", ",", "VALUES",
            "NUMBER", "STRING", ";", "#"
        }
        self.run_ll1_debug(grammar, "InsertStmt", terminals)

        # --- 真正解析（原实现已正确处理“列名可选”与多行 VALUES”） ---
        self.consume("INSERT")
        self.consume("INTO")
        tbl_tok = self.consume("IDENTIFIER")
        table_name = tbl_tok.value

        column_names: List[str] = []
        if self.current_token() and self.current_token().value == "(":
            self.consume("(")
            column_names.append(self.consume("IDENTIFIER").value)
            while self.current_token() and self.current_token().value == ",":
                self.consume(",")
                column_names.append(self.consume("IDENTIFIER").value)
            self.consume(")")

        self.consume("VALUES")
        rows: List[List[Any]] = []
        while True:
            self.consume("(")
            row: List[Any] = []
            t = self.consume()
            row.append(t.value)
            while self.current_token() and self.current_token().value == ",":
                self.consume(",")
                t = self.consume()
                row.append(t.value)
            self.consume(")")
            rows.append(row)
            if self.current_token() and self.current_token().value == ",":
                self.consume(",")
                continue
            break

        self.consume(";")
        return InsertNode(table_name, column_names, rows, pos=tbl_tok.line)

    # ---------------- SELECT ----------------
    def parse_select(self) -> SelectNode:
        # 教学可视化用的 LL(1) 文法（支持布尔表达式与括号）
        grammar = {
            "SelectStmt": [["SELECT", "SelectList", "FROM", "TableRef", "JoinList", "WhereOpt", "GroupOpt", "OrderOpt", ";"]],
            # 支持 SELECT * ...
            "SelectList": [["*"], ["ColumnRef", "SelectListTail"]],
            "SelectListTail": [[",", "ColumnRef", "SelectListTail"], ["ε"]],
            "ColumnRef": [["IDENTIFIER", "ColumnRefTail"]],
            "ColumnRefTail": [[".", "IDENTIFIER"], ["ε"]],
            "TableRef": [["IDENTIFIER", "AliasOpt"]],
            "AliasOpt": [["IDENTIFIER"], ["ε"]],
            "JoinList": [["Join", "JoinList"], ["ε"]],
            "Join": [["JOIN", "IDENTIFIER", "AliasOpt", "ON", "BoolExpr"]],
            "WhereOpt": [["WHERE", "BoolExpr"], ["ε"]],
            "GroupOpt": [["GROUP", "BY", "IDENTIFIER"], ["ε"]],
            "OrderOpt": [["ORDER", "BY", "IDENTIFIER", "OrderDir"], ["ε"]],
            "OrderDir": [["ASC"], ["DESC"], ["ε"]],
            # 布尔表达式（LL(1) 版本）
            "BoolExpr": [["BoolTerm", "BoolExprTail"]],
            "BoolExprTail": [["OR", "BoolTerm", "BoolExprTail"], ["ε"]],
            "BoolTerm": [["BoolFactor", "BoolTermTail"]],
            "BoolTermTail": [["AND", "BoolFactor", "BoolTermTail"], ["ε"]],
            "BoolFactor": [["NOT", "BoolFactor"], ["(", "BoolExpr", ")"], ["Predicate"]],
            "Predicate": [["ColumnRef", "OPERATOR", "Value"]],
            "Value": [["NUMBER"], ["STRING"], ["IDENTIFIER"], ["ColumnRef"]],
        }
        terminals = {"SELECT", "IDENTIFIER", ",", ".", "FROM", "JOIN", "ON", "WHERE", "GROUP", "BY", "ORDER",
                     "ASC", "DESC", "OPERATOR", "NUMBER", "STRING", "AND", "OR", "NOT", "(", ")", "*", ";", "#"}
        self.run_ll1_debug(grammar, "SelectStmt", terminals)

        self.consume("SELECT")

        def parse_column_ref() -> str:
            id_tok = self.consume("IDENTIFIER")
            col = str(id_tok.value).strip()
            if self.current_token() and self.current_token().value == ".":
                self.consume(".")
                right = self.consume("IDENTIFIER").value
                col = f"{col}.{str(right).strip()}"
            return col

        # ---- 递归式布尔表达式解析（生成原样 SQL 文本）----
        # precedence: NOT > AND > OR
        def parse_value_sql() -> str:
            t = self.current_token()
            if not t:
                raise Exception(self._format_err("Value", None))
            if getattr(t, "type", "") == "IDENTIFIER":
                if (self.pos + 1) < len(self.tokens) and self.tokens[self.pos + 1].value == ".":
                    return parse_column_ref()
                return self.consume("IDENTIFIER").value
            if getattr(t, "type", "") == "NUMBER":
                return str(self.consume("NUMBER").value)
            if getattr(t, "type", "") == "STRING":
                v = self.consume("STRING").value
                return f"'{v}'"
            if t.value == "(":
                self.consume("(")
                inner = parse_bool_expr()
                self.consume(")")
                return f"({inner})"
            # 退化为列引用
            return parse_column_ref()

        def parse_predicate() -> str:
            left = parse_column_ref()
            op_tok = self.consume("OPERATOR")
            op = str(op_tok.value)
            right = parse_value_sql()
            return f"{left} {op} {right}"

        def parse_bool_factor() -> str:
            t = self.current_token()
            if t and isinstance(t.value, str) and t.value.upper() == "NOT":
                self.consume("NOT")
                f = parse_bool_factor()
                return f"(NOT {f})"
            if t and t.value == "(":
                self.consume("(")
                e = parse_bool_expr()
                self.consume(")")
                return f"({e})"
            return parse_predicate()

        def parse_bool_term() -> str:
            left = parse_bool_factor()
            while self.current_token() and isinstance(self.current_token().value, str) and self.current_token().value.upper() == "AND":
                self.consume("AND")
                right = parse_bool_factor()
                left = f"({left} AND {right})"
            return left

        def parse_bool_expr() -> str:
            left = parse_bool_term()
            while self.current_token() and isinstance(self.current_token().value, str) and self.current_token().value.upper() == "OR":
                self.consume("OR")
                right = parse_bool_term()
                left = f"({left} OR {right})"
            return left
        # --------------------------------------------------

        # Select list
        select_items: List[Tuple[str, Optional[str]]] = []
        if self.current_token() and self.current_token().value == "*":
            self.consume("*")
            select_items.append(("*", None))
        else:
            select_items.append((parse_column_ref(), None))
            while self.current_token() and self.current_token().value == ",":
                self.consume(",")
                select_items.append((parse_column_ref(), None))

        # FROM table [alias]
        self.consume("FROM")
        tbl_tok = self.consume("IDENTIFIER")
        table_name = str(tbl_tok.value).strip()
        table_alias = None
        if self.current_token() and getattr(self.current_token(), "type", "").upper() == "IDENTIFIER":
            # 防止把关键字当别名
            nxt = str(self.current_token().value).upper()
            if nxt not in ("JOIN", "WHERE", "GROUP", "ORDER", ";"):
                table_alias = str(self.consume("IDENTIFIER").value).strip()
        if table_alias:
            table_alias = table_alias.strip().strip("()")

        node = SelectNode(select_items, table_name, from_alias=table_alias, pos=tbl_tok.line)

        # JOIN 列表
        while self.current_token() and isinstance(self.current_token().value, str) and self.current_token().value.upper() == "JOIN":
            self.consume("JOIN")
            right_tbl_tok = self.consume("IDENTIFIER")
            right_tbl = str(right_tbl_tok.value).strip()
            right_alias = None
            if self.current_token() and getattr(self.current_token(), "type", "").upper() == "IDENTIFIER":
                nxt = str(self.current_token().value).upper()
                if nxt not in ("ON", "JOIN", "WHERE", "GROUP", "ORDER", ";"):
                    right_alias = str(self.consume("IDENTIFIER").value).strip()
            if right_alias:
                right_alias = right_alias.strip().strip("()")
            self.consume("ON")
            cond_sql = parse_bool_expr()  # 支持 a=b AND/OR ... 以及括号/NOT
            node.joins.append((right_tbl, right_alias, cond_sql))

        # WHERE
        if self.current_token() and isinstance(self.current_token().value, str) and self.current_token().value.upper() == "WHERE":
            self.consume("WHERE")
            node.where_condition = parse_bool_expr()

        # GROUP BY
        if self.current_token() and isinstance(self.current_token().value, str) and self.current_token().value.upper() == "GROUP":
            self.consume("GROUP")
            self.consume("BY")
            node.group_by = str(self.consume("IDENTIFIER").value).strip()

        # ORDER BY
        if self.current_token() and isinstance(self.current_token().value, str) and self.current_token().value.upper() == "ORDER":
            self.consume("ORDER")
            self.consume("BY")
            node.order_by = str(self.consume("IDENTIFIER").value).strip()
            if self.current_token() and isinstance(self.current_token().value, str) and self.current_token().value.upper() in ("ASC", "DESC"):
                node.order_direction = self.consume().value.upper()

        self.consume(";")
        return node

    # ---------------- DELETE ----------------
    def parse_delete(self) -> DeleteNode:
        grammar = {
            "DeleteStmt": [["DELETE", "FROM", "IDENTIFIER", "WhereOpt", ";"]],
            "WhereOpt": [["WHERE", "IDENTIFIER", "OPERATOR", "Value"], ["ε"]],
            "Value": [["NUMBER"], ["STRING"], ["IDENTIFIER"]],
        }
        terminals = {"DELETE", "FROM", "IDENTIFIER", "WHERE", "OPERATOR", "NUMBER", "STRING", ";", "#"}
        self.run_ll1_debug(grammar, "DeleteStmt", terminals)

        self.consume("DELETE")
        self.consume("FROM")
        t = self.consume("IDENTIFIER")
        node = DeleteNode(t.value, pos=t.line)
        if self.current_token() and isinstance(self.current_token().value, str) and self.current_token().value.upper() == "WHERE":
            self.consume("WHERE")
            left = str(self.consume("IDENTIFIER").value).strip()
            op = self.consume("OPERATOR").value
            # 右值
            if self.current_token() and getattr(self.current_token(), "type", "").upper() == "IDENTIFIER":
                right = str(self.consume("IDENTIFIER").value).strip()
            else:
                right = self.consume().value
                if isinstance(right, str) and not (right.startswith("'") and right.endswith("'")):
                    right = f"'{right}'"
            node.where_condition = f"{left} {op} {right}"
        self.consume(";")
        return node

    # ---------------- UPDATE ----------------
    def parse_update(self) -> UpdateNode:
        grammar = {
            "UpdateStmt": [["UPDATE", "IDENTIFIER", "SET", "AssignmentList", "WhereOpt", ";"]],
            "AssignmentList": [["IDENTIFIER", "OPERATOR", "Value", "AssignTail"]],
            "AssignTail": [[",", "IDENTIFIER", "OPERATOR", "Value", "AssignTail"], ["ε"]],
            "WhereOpt": [["WHERE", "IDENTIFIER", "OPERATOR", "Value"], ["ε"]],
            "Value": [["NUMBER"], ["STRING"], ["IDENTIFIER"]],
        }
        terminals = {"UPDATE", "IDENTIFIER", "SET", "OPERATOR", ",", "WHERE", "NUMBER", "STRING", ";", "#"}
        self.run_ll1_debug(grammar, "UpdateStmt", terminals)

        self.consume("UPDATE")
        tbl_tok = self.consume("IDENTIFIER")
        table_name = tbl_tok.value
        self.consume("SET")
        assignments: List[Tuple[str, Any]] = []

        def parse_value_any():
            if self.current_token() and getattr(self.current_token(), "type", "").upper() == "IDENTIFIER":
                if (self.pos + 1) < len(self.tokens) and self.tokens[self.pos + 1].value == ".":
                    left = str(self.consume("IDENTIFIER").value).strip()
                    self.consume(".")
                    right = str(self.consume("IDENTIFIER").value).strip()
                    return f"{left}.{right}"
                else:
                    return self.consume("IDENTIFIER").value
            else:
                t = self.consume()
                return t.value

        # 第一个赋值
        col = str(self.consume("IDENTIFIER").value).strip()
        if self.current_token() and self.current_token().value == "=":
            self.consume("=")
        else:
            self.consume("OPERATOR")
        val = parse_value_any()
        assignments.append((col, val))
        # 后续赋值
        while self.current_token() and self.current_token().value == ",":
            self.consume(",")
            col = str(self.consume("IDENTIFIER").value).strip()
            if self.current_token() and self.current_token().value == "=":
                self.consume("=")
            else:
                self.consume("OPERATOR")
            val = parse_value_any()
            assignments.append((col, val))

        node = UpdateNode(table_name, assignments, pos=tbl_tok.line)
        # WHERE
        if self.current_token() and isinstance(self.current_token().value, str) and self.current_token().value.upper() == "WHERE":
            self.consume("WHERE")
            left = str(self.consume("IDENTIFIER").value).strip()
            op = self.consume("OPERATOR").value
            if self.current_token() and getattr(self.current_token(), "type", "").upper() == "IDENTIFIER":
                right = self.consume("IDENTIFIER").value
            else:
                right = self.consume().value
                if isinstance(right, str) and not (right.startswith("'") and right.endswith("'")):
                    right = f"'{right}'"
            node.where_condition = f"{left} {op} {right}"

        self.consume(";")
        return node
