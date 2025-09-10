# cli/main.py
import sys
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
                            # --- 修改开始：增强健壮性和添加调试 ---
                            # 检查第一行是否为非空字典
                            first_row = result[0]
                            if not isinstance(first_row, dict) or len(first_row) == 0:
                                print("Warning: Result rows are empty or not dictionaries.")
                                print("Raw result data (for debugging):")
                                for i, row in enumerate(result):
                                    print(f"Row {i}: {row} (type: {type(row)})")
                                print(f"\n{len(result)} row(s) returned")
                                continue

                            headers = list(first_row.keys())
                            if not headers:
                                print("Warning: No columns to display.")
                                print(f"\n{len(result)} row(s) returned")
                                continue

                            # 打印表头
                            header_str = " | ".join(headers)
                            print(header_str)
                            print("-" * len(header_str))

                            # 打印数据
                            for row in result:
                                # 确保row是字典
                                if not isinstance(row, dict):
                                    print(f"Warning: Row is not a dictionary: {row}")
                                    continue
                                values = [str(row.get(col, 'NULL')) for col in headers]  # 使用 .get 避免 KeyError
                                print(" | ".join(values))
                            # --- 修改结束 ---
                        print(f"\n{len(result)} row(s) returned")

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
        return self.executor.execute(plan)

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