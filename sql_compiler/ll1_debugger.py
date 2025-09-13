# sql_compiler/ll1_debugger.py
# 严格 LL(1) 调试器：计算 FIRST/FOLLOW，构建预测分析表，驱动推导过程

from typing import List, Dict, Set, Optional
from sql_compiler.lexer import Token


class LL1ParserDebugger:
    def __init__(self,
                 tokens: List[Token],
                 grammar: Dict[str, List[List[str]]],
                 start_symbol: str,
                 terminals: Set[str],
                 non_terminals: Set[str]):
        self.tokens = list(tokens)
        self.grammar = grammar
        self.start_symbol = start_symbol
        self.terminals = terminals
        self.non_terminals = non_terminals

        self.input_stream = list(self.tokens) + [None]  # None 表示输入结束符 #

        # FIRST/FOLLOW 集合
        self.FIRST = {nt: set() for nt in self.non_terminals}
        self.FOLLOW = {nt: set() for nt in self.non_terminals}

        # 预测分析表
        self.parse_table = {}

        self._compute_first_sets()
        self._compute_follow_sets()
        self._build_parse_table()

    # ---------- 工具函数 ----------
    def _tok_display(self, tok: Optional[Token]) -> str:
        if tok is None:
            return "#"
        if tok.type == "NUMBER":
            return str(tok.value)
        if isinstance(tok.value, str):
            return tok.value.upper()
        return str(tok.value)

    def _symbol_is_terminal(self, sym: str) -> bool:
        return sym in self.terminals or sym not in self.non_terminals

    # ---------- FIRST 集 ----------
    def _first(self, symbols: List[str]) -> Set[str]:
        """求符号串的 FIRST 集"""
        if not symbols:
            return {"ε"}
        first = set()
        for sym in symbols:
            if self._symbol_is_terminal(sym):
                first.add(sym)
                return first
            else:  # 非终结符
                first |= (self.FIRST[sym] - {"ε"})
                if "ε" not in self.FIRST[sym]:
                    return first
        first.add("ε")
        return first

    def _compute_first_sets(self):
        changed = True
        while changed:
            changed = False
            for A, prods in self.grammar.items():
                for prod in prods:
                    first = self._first(prod)
                    before = len(self.FIRST[A])
                    self.FIRST[A] |= first
                    if len(self.FIRST[A]) > before:
                        changed = True

    # ---------- FOLLOW 集 ----------
    def _compute_follow_sets(self):
        self.FOLLOW[self.start_symbol].add("#")
        changed = True
        while changed:
            changed = False
            for A, prods in self.grammar.items():
                for prod in prods:
                    trailer = self.FOLLOW[A].copy()
                    for sym in reversed(prod):
                        if sym in self.non_terminals:
                            before = len(self.FOLLOW[sym])
                            self.FOLLOW[sym] |= trailer
                            if "ε" in self.FIRST[sym]:
                                trailer |= (self.FIRST[sym] - {"ε"})
                            else:
                                trailer = self.FIRST[sym].copy()
                            if len(self.FOLLOW[sym]) > before:
                                changed = True
                        else:
                            trailer = {sym}

    # ---------- 构建预测分析表 ----------
    def _build_parse_table(self):
        for A, prods in self.grammar.items():
            for prod in prods:
                first = self._first(prod)
                for t in (first - {"ε"}):
                    self.parse_table[(A, t)] = prod
                if "ε" in first:
                    for t in self.FOLLOW[A]:
                        self.parse_table[(A, t)] = prod

    # ---------- 主执行方法 ----------
    def run(self):
        print("\n[LL1 推导过程]")
        stack = ["#", self.start_symbol]
        ip = 0

        while stack:
            top = stack[-1]
            cur_tok = self.input_stream[ip]
            cur_sym = self._tok_display(cur_tok)

            stack_str = " ".join(stack)
            input_str = " ".join(self._tok_display(t) for t in self.input_stream[ip:])
            print(f"栈: {stack_str:<40} 输入: {input_str:<50} 处理: {top}")

            if top == "#" and cur_tok is None:
                print("✅ 输入完整匹配，分析成功")
                return True

            # 如果栈顶是终结符
            if self._symbol_is_terminal(top):
                if ((cur_tok and str(cur_tok.value).upper() == top.upper())
                        or (top == cur_tok.type)  # e.g. IDENTIFIER, NUMBER
                        or (top == cur_tok.value)):
                    stack.pop()
                    ip += 1
                else:
                    print(f"❌ 出错: 期望 {top}, 实际 {cur_sym}")
                    return False
            # 栈顶为非终结符
            elif top in self.non_terminals:
                prod = self.parse_table.get((top, cur_sym))
                if not prod:
                    # 特殊情况：若 cur_sym 是 token.type（IDENTIFIER/NUMBER）
                    prod = self.parse_table.get((top, cur_tok.type if cur_tok else "#"))
                if not prod:
                    print(f"❌ 出错: 无法从 {top} 推导输入 {cur_sym}")
                    return False
                stack.pop()
                if prod != ["ε"]:
                    for sym in reversed(prod):
                        stack.append(sym)
                print(f"使用产生式: {top} -> {' '.join(prod)}")
            else:
                print(f"❌ 出错: 未知符号 {top}")
                return False

        print("❌ 栈清空但未接受输入")
        return False
