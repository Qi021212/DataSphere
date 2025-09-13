# cli/main.py
import sys
import os
from io import StringIO
from datetime import datetime
from contextlib import redirect_stdout

# 让 cli/.. 成为 import 根
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from utils.helpers import format_output
from sql_compiler.lexer import Lexer
from sql_compiler.parser import Parser
from sql_compiler.semantic_analyzer import SemanticAnalyzer
from sql_compiler.catalog import Catalog
from sql_compiler.planner import Planner
from storage.file_manager import FileManager
from engine.executor import Executor


# === 日志目录与“每次运行唯一”的日志文件 ===
LOG_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "log")
os.makedirs(LOG_DIR, exist_ok=True)
LOG_FILE = os.path.join(LOG_DIR, f"compile_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt")


def _clean_statement_for_lex(statement: str) -> str:
    """清理注释，仅用于写入日志时展示更稳定的词法输入。"""
    clean, in_string, i = "", False, 0
    while i < len(statement):
        ch = statement[i]
        if ch == "'" and (i == 0 or statement[i - 1] != '\\'):
            in_string = not in_string
            clean += ch; i += 1; continue
        if not in_string and ch == '-' and i + 1 < len(statement) and statement[i + 1] == '-':
            while i < len(statement) and statement[i] != '\n':
                i += 1
            continue
        if not in_string and ch == '/' and i + 1 < len(statement) and statement[i + 1] == '*':
            j = i + 2
            while j < len(statement) - 1:
                if statement[j] == '*' and statement[j + 1] == '/':
                    i = j + 2
                    break
                j += 1
            if j >= len(statement) - 1:
                i = j
            continue
        clean += ch; i += 1
    return clean.strip()


def _extract_smart_hints(msg: str):
    return [line.strip() for line in (msg or "").splitlines() if line.strip().startswith("智能提示：")]


class DatabaseCLI:
    def __init__(self):
        # —— 只实例化一次 Catalog（其内部若文件不存在会打印一次提示）
        self.catalog = Catalog()
        self.file_manager = FileManager()
        self.executor = Executor(self.file_manager, self.catalog)
        self.semantic_analyzer = SemanticAnalyzer(self.catalog)
        self.planner = Planner()

        # 日志与统计（仅本次会话）
        self._log_lines = []
        self._success_cnt = 0
        self._total_cnt = 0

    # ============== 详细编译日志 +（可选）执行 ==============
    def process_and_log(self, sql_with_semicolon: str, actually_execute: bool = True):
        """
        完整处理一条 SQL，并把详细过程写入内存日志。
        成功/失败统计也在这里维护；仅把“智能提示：...”即时输出到控制台。
        """
        stmt = sql_with_semicolon.strip()
        if not stmt:
            return None

        self._total_cnt += 1
        idx = self._total_cnt  # 当前语句编号

        # 详细日志缓冲（捕获 Parser 的 LL(1) 推导等 print）
        sink = StringIO()

        # 头
        self._log_lines.append(f"\n>>> 处理第 {idx} 条 SQL 语句 <<<")
        self._log_lines.append(f"SQL 语句: {stmt}")

        # 1) 词法
        self._log_lines.append("\n" + "=" * 50)
        self._log_lines.append(f"阶段: 1. 词法分析 (语句 {idx})")
        self._log_lines.append("=" * 50)
        clean = _clean_statement_for_lex(stmt)
        tokens = Lexer(clean).get_tokens()
        if not tokens:
            self._log_lines.append("[词法错误] 空语句")
            return None
        for tk in tokens:
            self._log_lines.append(repr(tk))

        try:
            # 2) 语法
            self._log_lines.append("\n" + "=" * 50)
            self._log_lines.append(f"阶段: 2. 语法分析 (语句 {idx})")
            self._log_lines.append("=" * 50)
            with redirect_stdout(sink):
                parser = Parser(tokens, source_text=stmt)
                ast = parser.parse()
            self._log_lines.append(sink.getvalue()); sink.seek(0); sink.truncate(0)

            # 3) 语义
            self._log_lines.append("\n" + "=" * 50)
            self._log_lines.append(f"阶段: 3. 语义分析 (语句 {idx})")
            self._log_lines.append("=" * 50)
            with redirect_stdout(sink):
                sem_res = self.semantic_analyzer.analyze(ast)
            sem_text = sink.getvalue(); sink.seek(0); sink.truncate(0)
            if sem_text.strip():
                self._log_lines.append(sem_text.strip())
            if isinstance(sem_res, str) and sem_res:
                self._log_lines.append(sem_res)

            # 4) 执行计划
            self._log_lines.append("\n" + "=" * 50)
            self._log_lines.append(f"阶段: 4. 执行计划生成 (语句 {idx})")
            self._log_lines.append("=" * 50)
            with redirect_stdout(sink):
                plan = self.planner.generate_plan(ast)
            plan_prints = sink.getvalue(); sink.seek(0); sink.truncate(0)
            if plan_prints.strip():
                self._log_lines.append(plan_prints.strip())

            # 为了兼容不同风格对象，做一个简短描述写日志：
            desc = None
            try:
                # 老字典风格：{'type': 'Select', 'details': {...}}
                if isinstance(plan, dict) and 'type' in plan and 'details' in plan:
                    desc = f"ExecutionPlan(type={plan['type']}, details_keys={list(plan['details'].keys())})"
                # dataclass ExecutionPlan 风格：有 root/explain
                elif hasattr(plan, 'root'):
                    desc = repr(plan)
            except Exception:
                pass
            self._log_lines.append("逻辑执行计划: " + (desc or repr(plan)))

            # === 如果有优化讲解（例如谓词下推前后对比），写入日志，并可选在控制台打印 ===
            explain_text = getattr(plan, "explain", None) if hasattr(plan, "explain") else None
            if isinstance(explain_text, str) and explain_text.strip():
                # 日志里：前面加一个空行，再加区块标题
                self._log_lines.append("")
                self._log_lines.append("—— 优化讲解（谓词下推） ——")
                self._log_lines.append(explain_text)

                # 控制台：也输出一个简版，便于交互立即看到
                # 如果不想在终端打印这段，把下面三行注释掉即可
                print()
                print("—— 优化讲解（谓词下推） ——")
                print(explain_text)

            # === 实际执行并回显 ===
            if actually_execute:
                result = self.executor.execute(plan)
                # 控制台显示
                if isinstance(result, str):
                    print(result)
                elif isinstance(result, list):
                    print(format_output(result) if result else "No results returned.")
                # 成功统计（执行到这里无异常即可算成功）
                self._success_cnt += 1

            return True

        except Exception as e:
            # 把 parser/semantic/plan 的打印先写入日志
            prints = sink.getvalue()
            if prints:
                self._log_lines.append(prints)
            self._log_lines.append(str(e))

            # 仅把“智能提示：...”行即时输出
            for h in _extract_smart_hints(str(e)):
                print(h)
            return None

    # ============== 交互 ==============
    def _read_stmt(self, prompt="SQL> "):
        """
        交互输入：支持多行；读到“分号（字符串外）”结束，返回**包含分号**的完整语句。
        'quit' / 'exit'（带不带分号）直接返回控制命令。
        """
        buf, in_str = [], False
        while True:
            try:
                line = input(prompt if not buf else "... ")
            except EOFError:
                return "__EXIT__"

            raw = line.strip()
            low = raw.lower()
            if not buf and low in ("quit", "quit;", "exit", "exit;"):
                return "__EXIT__"

            buf.append(line)
            text = "\n".join(buf)
            i = 0
            while i < len(text):
                ch = text[i]
                if ch == ";" and not in_str:
                    # 返回截至分号为止（包含分号）
                    return text[: i + 1].strip()
                if ch == "'" and (i == 0 or text[i - 1] != "\\"):
                    in_str = not in_str
                i += 1
            # 否则继续读下一行

    def run(self):
        print("Welcome to SimpleDB CLI")
        print("多行输入；以 ';' 结束一条语句。输入 quit/exit 退出。")

        while True:
            stmt = self._read_stmt()
            if stmt in ("__EXIT__", None):
                break
            if not stmt.strip():
                continue
            # 交互模式：既要执行，也要写入详细日志
            self.process_and_log(stmt, actually_execute=True)

        # 退出时落盘日志 + 汇总统计
        with open(LOG_FILE, "w", encoding="utf-8") as f:
            f.write("=== 详细编译日志（本次会话） ===\n")
            f.write("\n".join(self._log_lines))

        print(f"✅ 成功处理 {self._success_cnt} / {self._total_cnt} 条 SQL 语句！")
        print("🎉 SQL 编译器执行完成！详细编译日志已保存到：")
        print(LOG_FILE)


def main():
    cli = DatabaseCLI()
    cli.run()


if __name__ == "__main__":
    main()
