# cli/main.py
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
# 👇 新增：导入 format_output 函数
from utils.helpers import format_output
from sql_compiler.lexer import Lexer
from sql_compiler.parser import Parser
from sql_compiler.semantic import SemanticAnalyzer
from sql_compiler.planner import Planner
from sql_compiler.catalog import Catalog
from storage.file_manager import FileManager
from engine.executor import Executor

class DatabaseCLI:
    def __init__(self):
        self.catalog = Catalog()
        self.file_manager = FileManager()
        self.executor = Executor(self.file_manager, self.catalog)
        self.semantic_analyzer = SemanticAnalyzer(self.catalog)
        self.lexer = Lexer()
        self.parser = Parser()
        self.planner = Planner()
        self.is_running = False

    def run(self):
        self.is_running = True
        print("Welcome to SimpleDB CLI")
        print("Type 'exit;' or 'quit;' to exit")
        print("Type 'help;' for help")
        while self.is_running:
            try:
                command = self.get_input()
                if not command:
                    continue
                if command.lower() in ['exit', 'quit']:
                    self.is_running = False
                    continue
                if command.lower() == 'help':
                    self.show_help()
                    continue
                result = self.execute_sql(command)
                if result:
                    if isinstance(result, str):
                        print(result)
                    elif isinstance(result, list):
                        if result:
                            # 👇 关键修改：调用 format_output 函数来格式化输出
                            formatted_output = format_output(result)
                            print(formatted_output)
                        else:
                            print("No results returned.")
            except Exception as e:
                print(f"Error: {e}")

    def get_input(self) -> str:
        try:
            line = input("SQL> ")
            command = line.strip()
            # 支持多行输入，直到遇到分号
            while not command.endswith(';'):
                next_line = input("... ")
                command += " " + next_line.strip()
            return command[:-1]  # 去掉结尾的分号
        except EOFError:
            return "exit;"
        except KeyboardInterrupt:
            print("\nType 'exit;' to exit")
            return ""

    def execute_sql(self, sql: str):
        # 词法分析
        tokens = self.lexer.tokenize(sql)
        # 语法分析
        ast = self.parser.parse(tokens)
        # 语义分析
        errors = self.semantic_analyzer.analyze(ast)
        if errors:
            for error in errors:
                print(f"Semantic error: {error}")
            return None
        # 生成执行计划
        plan = self.planner.generate_plan(ast)
        # 执行
        result = self.executor.execute(plan)

        # 👇👇👇 新增调试打印 👇👇👇
        print(f"DEBUG: Executor returned: {result}")  # 打印执行器返回的结果
        # 👆👆👆 新增调试打印 👆👆👆

        return result

    def show_help(self):
        help_text = """
Supported SQL commands:
  CREATE TABLE table_name (col1 type1, col2 type2, ...);
  INSERT INTO table_name [(col1, col2, ...)] VALUES (val1, val2, ...);
  SELECT col1, col2, ... FROM table_name [WHERE condition];
  DELETE FROM table_name [WHERE condition];
Supported data types: INT, VARCHAR, FLOAT, BOOL
Supported operators: =, >, <, >=, <=, !=
Examples:
  CREATE TABLE users (id INT, name VARCHAR, age INT);
  INSERT INTO users VALUES (1, 'Alice', 25);
  SELECT * FROM users WHERE age > 20;
        """
        print(help_text)

def main():
    cli = DatabaseCLI()
    cli.run()

if __name__ == "__main__":
    main()