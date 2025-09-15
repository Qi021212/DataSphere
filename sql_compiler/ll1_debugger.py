# sql_compiler/ll1_grammars_demo.py
from typing import Dict, List, Set, Tuple
from sql_compiler.lexer import Lexer, Token


class LL1ParserDebugger:
    pass


from sql_compiler.ll1_debugger import LL1ParserDebugger

# ---------- 文法集合（全是左因子/无左递归，适合 LL(1) 调试） ----------

def grammar_select_with_aggs() -> Tuple[Dict[str, List[List[str]]], Set[str], Set[str], str]:
    grammar = {
        "SelectStmt": [["SELECT", "SelectList", "FROM", "TableRef", "JoinList", "WhereOpt", "GroupOpt", "OrderOpt", ";"]],
        "SelectList": [["*"], ["SelectItem", "SelectListTail"]],
        "SelectListTail": [[",", "SelectItem", "SelectListTail"], ["ε"]],
        "SelectItem": [["Aggregate", "AliasOpt"], ["ColumnRef", "AliasOpt"]],
        "AliasOpt": [["AS", "IDENTIFIER"], ["IDENTIFIER"], ["ε"]],

        "Aggregate": [["COUNT", "(", "AggArg", ")"],
                      ["SUM", "(", "ColumnRef", ")"],
                      ["AVG", "(", "ColumnRef", ")"]],
        "AggArg": [["*"], ["ColumnRef"]],

        "ColumnRef": [["IDENTIFIER", "ColumnRefTail"]],
        "ColumnRefTail": [[".", "IDENTIFIER"], ["ε"]],

        "TableRef": [["IDENTIFIER", "TableAliasOpt"]],
        "TableAliasOpt": [["IDENTIFIER"], ["ε"]],

        "JoinList": [["Join", "JoinList"], ["ε"]],
        "Join": [["JOIN", "IDENTIFIER", "TableAliasOpt", "ON", "BoolExpr"]],

        "WhereOpt": [["WHERE", "BoolExpr"], ["ε"]],
        "GroupOpt": [["GROUP", "BY", "IDENTIFIER"], ["ε"]],
        "OrderOpt": [["ORDER", "BY", "IDENTIFIER", "OrderDir"], ["ε"]],
        "OrderDir": [["ASC"], ["DESC"], ["ε"]],

        # ---- Boolean Expr (NOT > AND > OR) ----
        "BoolExpr": [["BoolTerm", "BoolExprTail"]],
        "BoolExprTail": [["OR", "BoolTerm", "BoolExprTail"], ["ε"]],
        "BoolTerm": [["BoolFactor", "BoolTermTail"]],
        "BoolTermTail": [["AND", "BoolFactor", "BoolTermTail"], ["ε"]],
        "BoolFactor": [["NOT", "BoolFactor"], ["(", "BoolExpr", ")"], ["Predicate"]],
        "Predicate": [["ColumnRef", "OPERATOR", "Value"]],
        "Value": [["NUMBER"], ["STRING"], ["IDENTIFIER"], ["ColumnRef"]],
    }
    non_terminals = set(grammar.keys())
    terminals = {
        "SELECT","FROM","JOIN","ON","WHERE","GROUP","BY","ORDER","ASC","DESC",
        "AS","COUNT","SUM","AVG",
        "IDENTIFIER","NUMBER","STRING","OPERATOR",
        "(",")",",",".","*",";","#"
    }
    start = "SelectStmt"
    return grammar, terminals, non_terminals, start


def grammar_insert() -> Tuple[Dict[str, List[List[str]]], Set[str], Set[str], str]:
    grammar = {
        "InsertStmt": [["INSERT","INTO","IDENTIFIER","InsertCols","VALUES","(", "ValueList", ")", "InsertTail",";"]],
        "InsertCols": [["(", "ColumnList", ")"], ["ε"]],
        "ColumnList": [["IDENTIFIER","ColumnListTail"]],
        "ColumnListTail": [[",","IDENTIFIER","ColumnListTail"], ["ε"]],
        "ValueList": [["Value","ValueListTail"]],
        "ValueListTail": [[",","Value","ValueListTail"], ["ε"]],
        "Value": [["NUMBER"], ["STRING"], ["IDENTIFIER"]],
        "InsertTail": [[",","(", "ValueList", ")", "InsertTail"], ["ε"]],
    }
    non_terminals = set(grammar.keys())
    terminals = {"INSERT","INTO","VALUES","IDENTIFIER","NUMBER","STRING","(",")",",",";","#"}
    return grammar, terminals, non_terminals, "InsertStmt"


def grammar_update() -> Tuple[Dict[str, List[List[str]]], Set[str], Set[str], str]:
    grammar = {
        "UpdateStmt": [["UPDATE","IDENTIFIER","SET","AssignmentList","WhereOpt",";"]],
        "AssignmentList": [["IDENTIFIER","AssignOp","Value","AssignTail"]],
        "AssignTail": [[",","IDENTIFIER","AssignOp","Value","AssignTail"], ["ε"]],
        "AssignOp": [["OPERATOR"]],  # (=) 通常被 lexer 记为 OPERATOR
        "WhereOpt": [["WHERE","IDENTIFIER","OPERATOR","Value"], ["ε"]],
        "Value": [["NUMBER"], ["STRING"], ["IDENTIFIER"]],
    }
    non_terminals = set(grammar.keys())
    terminals = {"UPDATE","IDENTIFIER","SET","OPERATOR",",","WHERE","NUMBER","STRING",";","#"}
    return grammar, terminals, non_terminals, "UpdateStmt"


def grammar_delete() -> Tuple[Dict[str, List[List[str]]], Set[str], Set[str], str]:
    grammar = {
        "DeleteStmt": [["DELETE","FROM","IDENTIFIER","WhereOpt",";"]],
        "WhereOpt": [["WHERE","IDENTIFIER","OPERATOR","Value"], ["ε"]],
        "Value": [["NUMBER"], ["STRING"], ["IDENTIFIER"]],
    }
    non_terminals = set(grammar.keys())
    terminals = {"DELETE","FROM","IDENTIFIER","WHERE","OPERATOR","NUMBER","STRING",";","#"}
    return grammar, terminals, non_terminals, "DeleteStmt"


def grammar_create_table() -> Tuple[Dict[str, List[List[str]]], Set[str], Set[str], str]:
    grammar = {
        "CreateStmt": [["CREATE","TABLE","IDENTIFIER","(","ColumnDefs",")",";"]],
        "ColumnDefs": [["IDENTIFIER","Type","ColumnDefsTail"]],
        "ColumnDefsTail": [[",","IDENTIFIER","Type","ColumnDefsTail"], ["ε"]],
        "Type": [["INT"], ["VARCHAR"], ["FLOAT"], ["BOOL"]],
    }
    non_terminals = set(grammar.keys())
    terminals = {"CREATE","TABLE","IDENTIFIER","(",")",",","INT","VARCHAR","FLOAT","BOOL",";","#"}
    return grammar, terminals, non_terminals, "CreateStmt"


def grammar_explain_of(kind: str) -> Tuple[Dict[str, List[List[str]]], Set[str], Set[str], str]:
    """
    生成 EXPLAIN <kind> 的文法。为了保持 LL(1) 简洁，我们把 <kind> 嵌回去。
    kind ∈ {"select","insert","update","delete","create"}
    """
    if kind == "select":
        inner, t_in, nt_in, start_in = grammar_select_with_aggs()
        grammar = {"ExplainStmt": [["EXPLAIN", start_in]]}
        grammar.update(inner)
        terminals = set(t_in) | {"EXPLAIN"}
        non_terminals = set(nt_in) | {"ExplainStmt"}
        start = "ExplainStmt"
        return grammar, terminals, non_terminals, start

    if kind == "insert":
        inner, t_in, nt_in, start_in = grammar_insert()
        grammar = {"ExplainStmt": [["EXPLAIN", start_in]]}
        grammar.update(inner)
        terminals = set(t_in) | {"EXPLAIN"}
        non_terminals = set(nt_in) | {"ExplainStmt"}
        return grammar, terminals, non_terminals, "ExplainStmt"

    if kind == "update":
        inner, t_in, nt_in, start_in = grammar_update()
        grammar = {"ExplainStmt": [["EXPLAIN", start_in]]}
        grammar.update(inner)
        terminals = set(t_in) | {"EXPLAIN"}
        non_terminals = set(nt_in) | {"ExplainStmt"}
        return grammar, terminals, non_terminals, "ExplainStmt"

    if kind == "delete":
        inner, t_in, nt_in, start_in = grammar_delete()
        grammar = {"ExplainStmt": [["EXPLAIN", start_in]]}
        grammar.update(inner)
        terminals = set(t_in) | {"EXPLAIN"}
        non_terminals = set(nt_in) | {"ExplainStmt"}
        return grammar, terminals, non_terminals, "ExplainStmt"

    if kind == "create":
        inner, t_in, nt_in, start_in = grammar_create_table()
        grammar = {"ExplainStmt": [["EXPLAIN", start_in]]}
        grammar.update(inner)
        terminals = set(t_in) | {"EXPLAIN"}
        non_terminals = set(nt_in) | {"ExplainStmt"}
        return grammar, terminals, non_terminals, "ExplainStmt"

    raise ValueError(f"unsupported EXPLAIN kind: {kind}")


# ---------- 统一调试入口 ----------

def run_debug(sql_text: str, kind: str = "select") -> bool:
    """
    kind ∈ {"select","insert","update","delete","create","explain-select",
            "explain-insert","explain-update","explain-delete","explain-create"}
    """
    # 1) 词法
    lexer = Lexer(sql_text)
    tokens: List[Token] = lexer.get_tokens()

    # 2) 选择文法
    if kind == "select":
        grammar, terminals, non_terminals, start = grammar_select_with_aggs()
    elif kind == "insert":
        grammar, terminals, non_terminals, start = grammar_insert()
    elif kind == "update":
        grammar, terminals, non_terminals, start = grammar_update()
    elif kind == "delete":
        grammar, terminals, non_terminals, start = grammar_delete()
    elif kind == "create":
        grammar, terminals, non_terminals, start = grammar_create_table()
    elif kind.startswith("explain-"):
        sub = kind.split("-", 1)[1]
        grammar, terminals, non_terminals, start = grammar_explain_of(sub)
    else:
        raise ValueError(f"unknown kind: {kind}")

    # 3) 调试运行
    dbg = LL1ParserDebugger(tokens, grammar, start, terminals, non_terminals)
    return dbg.run()


# ---------- 用法示例（在你的 REPL/单元测试里调用） ----------
if __name__ == "__main__":
    cases = [
        ("SELECT COUNT(*) FROM t;", "select"),
        ("SELECT SUM(age) AS s, AVG(age) a FROM t WHERE age >= 18 ORDER BY a DESC;", "select"),
        ("SELECT name, COUNT(*) AS c FROM t GROUP BY name;", "select"),
        ("EXPLAIN SELECT AVG(age) FROM t WHERE age > 18;", "explain-select"),
        ("CREATE TABLE student(id INT, name VARCHAR, age INT);", "create"),
        ("EXPLAIN CREATE TABLE t(id INT, v FLOAT);", "explain-create"),
        ("INSERT INTO t(id,name) VALUES (1,'Alice'),(2,'Bob');", "insert"),
        ("UPDATE t SET age = 21 WHERE id = 1;", "update"),
        ("DELETE FROM t WHERE id = 2;", "delete"),
    ]
    for sql, k in cases:
        print("\n" + "="*80)
        print(f"-- {k} :: {sql}")
        ok = run_debug(sql, k)
        print("RESULT:", ok)
