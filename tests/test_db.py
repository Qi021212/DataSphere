import unittest
import os
import shutil
from sql_compiler.lexer import Lexer
from sql_compiler.parser import Parser
from sql_compiler.semantic import SemanticAnalyzer
from sql_compiler.planner import Planner
from sql_compiler.catalog import Catalog
from storage.file_manager import FileManager
from engine.executor import Executor

class TestDatabaseSystem(unittest.TestCase):
    def setUp(self):
        # 清理测试数据
        if os.path.exists('test_data'):
            shutil.rmtree('test_data')

        os.makedirs('test_data', exist_ok=True)

        self.catalog = Catalog('test_data/catalog.json')
        self.file_manager = FileManager('test_data')
        self.executor = Executor(self.file_manager, self.catalog)
        self.semantic_analyzer = SemanticAnalyzer(self.catalog)
        self.lexer = Lexer()
        self.parser = Parser()
        self.planner = Planner()

    def tearDown(self):
        if os.path.exists('test_data'):
            shutil.rmtree('test_data')

    def test_lexer(self):
        sql = "SELECT * FROM users WHERE age > 25;"
        tokens = self.lexer.tokenize(sql)

        self.assertEqual(len(tokens), 8)
        self.assertEqual(tokens[0].lexeme, 'SELECT')
        self.assertEqual(tokens[2].lexeme, '*')
        self.assertEqual(tokens[4].lexeme, 'users')

    def test_parser(self):
        sql = "CREATE TABLE users (id INT, name VARCHAR);"
        tokens = self.lexer.tokenize(sql)
        ast = self.parser.parse(tokens)

        self.assertEqual(ast.node_type, 'CreateTable')
        self.assertEqual(ast.children[0].value, 'users')

    def test_semantic_analysis(self):
        # 先创建表
        create_sql = "CREATE TABLE users (id INT, name VARCHAR);"
        tokens = self.lexer.tokenize(create_sql)
        ast = self.parser.parse(tokens)
        errors = self.semantic_analyzer.analyze(ast)
        self.assertEqual(len(errors), 0)

        # 测试插入语句的语义分析
        insert_sql = "INSERT INTO users VALUES (1, 'Alice');"
        tokens = self.lexer.tokenize(insert_sql)
        ast = self.parser.parse(tokens)
        errors = self.semantic_analyzer.analyze(ast)
        self.assertEqual(len(errors), 0)

    def test_execution(self):
        # 创建表
        create_sql = "CREATE TABLE users (id INT, name VARCHAR);"
        tokens = self.lexer.tokenize(create_sql)
        ast = self.parser.parse(tokens)
        plan = self.planner.generate_plan(ast)
        result = self.executor.execute(plan)
        self.assertIn("created successfully", result)

        # 插入数据
        insert_sql = "INSERT INTO users VALUES (1, 'Alice');"
        tokens = self.lexer.tokenize(insert_sql)
        ast = self.parser.parse(tokens)
        plan = self.planner.generate_plan(ast)
        result = self.executor.execute(plan)
        self.assertIn("inserted", result)

        # 查询数据
        select_sql = "SELECT * FROM users;"
        tokens = self.lexer.tokenize(select_sql)
        ast = self.parser.parse(tokens)
        plan = self.planner.generate_plan(ast)
        result = self.executor.execute(plan)
        self.assertEqual(len(result), 2)  # 模拟返回2行数据


if __name__ == "__main__":
    unittest.main()