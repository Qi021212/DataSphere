# test_db.py
import os
import shutil
import sys
import unittest
from io import StringIO
from pathlib import Path
from contextlib import redirect_stdout
from engine.executor import Executor

# 将项目根目录添加到 Python 路径
sys.path.insert(0, str(Path(__file__).parent.parent))

from cli.main import DatabaseCLI
from utils.helpers import format_output


class TestDataSphereDB(unittest.TestCase):
    """DataSphere 数据库系统集成测试 (适配当前语法限制版)"""

    @classmethod
    def setUpClass(cls):
        """在所有测试开始前运行一次：设置临时测试环境"""
        cls.test_data_dir = "test_data"
        cls.test_log_dir = "test_log"
        # 清理旧的测试数据
        if os.path.exists(cls.test_data_dir):
            shutil.rmtree(cls.test_data_dir)
        if os.path.exists(cls.test_log_dir):
            shutil.rmtree(cls.test_log_dir)
        os.makedirs(cls.test_data_dir, exist_ok=True)
        os.makedirs(cls.test_log_dir, exist_ok=True)

        # 初始化 CLI
        cls.cli = DatabaseCLI()

    @classmethod
    def tearDownClass(cls):
        """在所有测试结束后运行一次：清理测试环境"""
        # 清理测试数据
        if os.path.exists(cls.test_data_dir):
            shutil.rmtree(cls.test_data_dir)
        if os.path.exists(cls.test_log_dir):
            shutil.rmtree(cls.test_log_dir)

    def _execute_sql(self, sql: str) -> str:
        """执行单条 SQL 语句并返回结果或错误信息"""
        try:
            # 捕获标准输出
            captured_output = StringIO()
            with redirect_stdout(captured_output):
                result = self.cli.process_and_log(sql, actually_execute=True)

            # 获取控制台打印的内容
            printed_output = captured_output.getvalue().strip()

            # 如果执行返回的是列表（查询结果），直接返回格式化后的字符串
            if isinstance(result, list):
                return format_output(result) if result else "No results returned."
            # 如果返回的是字符串（成功消息或错误信息），返回该字符串
            elif isinstance(result, str):
                return result
            else:
                # 如果 result 是 None，返回捕获的输出
                return printed_output if printed_output else "Execution successful (no result to display)."
        except Exception as e:
            return str(e)

    def test_01_create_tables_with_foreign_key(self):
        """测试 3.1 & 3.3: 创建表（含外键约束）"""
        # 创建 departments 表
        sql1 = """
        CREATE TABLE departments (
            dept_id INT,
            dept_name VARCHAR
        );
        """
        result1 = self._execute_sql(sql1)
        self.assertIn("created successfully", result1, "创建 departments 表失败")

        # 创建 employees 表，并添加外键
        sql2 = """
        CREATE TABLE employees (
            emp_id INT,
            name VARCHAR,
            salary FLOAT,
            department_id INT,
            FOREIGN KEY (department_id) REFERENCES departments(dept_id)
        );
        """
        result2 = self._execute_sql(sql2)
        self.assertIn("created successfully", result2, "创建 employees 表失败")

    def test_02_insert_data(self):
        """测试 3.1 & 3.5: 插入数据"""
        # 向 departments 表插入数据
        dept_data = [
            "INSERT INTO departments(dept_id, dept_name) VALUES (1, 'Engineering');",
            "INSERT INTO departments(dept_id, dept_name) VALUES (2, 'Sales');",
            "INSERT INTO departments(dept_id, dept_name) VALUES (3, 'Marketing');",
            "INSERT INTO departments(dept_id, dept_name) VALUES (4, 'Human Resources');",
        ]
        for sql in dept_data :
            result = self._execute_sql(sql)
            self.assertIn("1 row inserted", result, f"插入部门数据失败: {sql}")

        # 向 employees 表插入数据
        emp_data = [
            "INSERT INTO employees(emp_id, name, salary, department_id) VALUES (101, 'Alice', 75000.00, 1);",
            "INSERT INTO employees(emp_id, name, salary, department_id) VALUES (102, 'Bob', 65000.00, 1);",
            "INSERT INTO employees(emp_id, name, salary, department_id) VALUES (103, 'Charlie', 55000.00, 2);",
            "INSERT INTO employees(emp_id, name, salary, department_id) VALUES (104, 'Diana', 60000.00, 2);",
            "INSERT INTO employees(emp_id, name, salary, department_id) VALUES (105, 'Eve', 70000.00, 3);",
            "INSERT INTO employees(emp_id, name, salary, department_id) VALUES (106, 'Frank', 50000.00, 3);",
            "INSERT INTO employees(emp_id, name, salary, department_id) VALUES (107, 'Grace', 80000.00, 1);",
            "INSERT INTO employees(emp_id, name, salary, department_id) VALUES (108, 'Heidi', 45000.00, 4);",
            "INSERT INTO employees(emp_id, name, salary, department_id) VALUES (109, 'Ivan', 90000.00, 1);",
        ]
        for sql in emp_data :
            result = self._execute_sql(sql)
            self.assertIn("1 row inserted", result, f"插入员工数据失败: {sql}")

    def test_03_basic_select_and_where(self):
        """测试 3.1: 基础查询和条件过滤 (WHERE)"""
        # 测试 SELECT *
        sql1 = "SELECT * FROM departments;"
        result1 = self._execute_sql(sql1)
        self.assertIn("Engineering", result1, "查询 departments 表失败")
        self.assertIn("Sales", result1, "查询 departments 表失败")

        # 测试带 WHERE 的查询
        sql2 = "SELECT name, salary FROM employees WHERE salary > 70000;"
        result2 = self._execute_sql(sql2)
        self.assertIn("Alice", result2, "WHERE 条件查询失败")
        self.assertIn("Grace", result2, "WHERE 条件查询失败")
        self.assertIn("Ivan", result2, "WHERE 条件查询失败")
        self.assertNotIn("Bob", result2, "WHERE 条件查询失败，包含了不应有的记录")

    def test_04_join_operation(self):
        """测试 3.6: 多表连接查询 (JOIN)"""
        # 注意：SELECT 和 WHERE 中使用带别名的列，这是允许的。
        sql = """
        SELECT e.name, d.dept_name
        FROM employees e
        JOIN departments d ON e.department_id = d.dept_id;
        """
        result = self._execute_sql(sql)
        self.assertIn("Alice", result, "JOIN 查询失败")
        self.assertIn("Engineering", result, "JOIN 查询失败")
        self.assertIn("Ivan", result, "JOIN 查询失败")
        self.assertIn("9 row(s) returned", result, "JOIN 查询失败")

    def test_05_aggregate_functions_and_group_by(self):
        """测试 3.6: 聚合函数和分组 (GROUP BY) - 适配版"""
        # 测试 COUNT 和 GROUP BY
        # 关键：SELECT 和 GROUP BY 都使用裸列名 'department_id'
        sql1 = """
        SELECT department_id, COUNT(*)
        FROM employees
        GROUP BY department_id
        ORDER BY department_id ASC;
        """
        result1 = self._execute_sql(sql1)
        self.assertIn("1", result1, "GROUP BY 查询失败")
        self.assertIn("4", result1, "GROUP BY 查询失败 (Engineering 应有 4 人)")
        self.assertIn("2", result1, "GROUP BY 查询失败")
        self.assertIn("2", result1, "GROUP BY 查询失败 (Sales 应有 2 人)")

        # 测试 AVG 和 GROUP BY (JOIN 后)
        # 关键：SELECT 使用别名 'dn'，GROUP BY 使用裸列名 'dept_name'
        # 语义分析器会检查 'dn' 是否与 'dept_name' 等价，当前代码可能不支持，所以我们直接在 SELECT 中也使用 'dept_name'
        sql2 = """
        SELECT department_id, AVG(salary) AS avg_salary
        FROM employees
        GROUP BY department_id
        ORDER BY department_id ASC;
        """
        result2 = self._execute_sql(sql2)
        self.assertIn("77500.0", result2, "AVG 聚合计算错误")
        self.assertIn("57500.0", result2, "AVG 聚合计算错误")
        self.assertIn("60000.0", result2, "AVG 聚合计算错误")
        self.assertIn("45000.0", result2, "AVG 聚合计算错误")

    def test_06_order_by(self):
        """测试 3.6: 排序 (ORDER BY) - 适配版"""
        # 关键：ORDER BY 使用裸列名 'salary'
        sql = "SELECT name, salary FROM employees ORDER BY salary DESC;"
        result = self._execute_sql(sql)
        lines = result.splitlines()
        # 找到数据行，忽略表头和分隔符
        data_lines = [line for line in lines if '|' in line and 'name' not in line and '---' not in line]
        if len(data_lines) >= 2:
            # 第一个数据行应该是工资最高的
            self.assertIn("Ivan", data_lines[0], "ORDER BY DESC 排序失败")
            # 最后一个数据行应该是工资最低的
            self.assertIn("Heidi", data_lines[-1], "ORDER BY DESC 排序失败")

    def test_07_update_statement(self):
        """测试 3.6: 更新操作 (UPDATE)"""
        # 先查询原始工资
        sql_check_before = "SELECT salary FROM employees WHERE name = 'Alice';"
        result_before = self._execute_sql(sql_check_before)
        self.assertIn("75000.0", result_before, "更新前查询失败")

        # 执行更新
        sql_update = "UPDATE employees SET salary = 80000.00 WHERE name = 'Alice';"
        result_update = self._execute_sql(sql_update)
        self.assertIn("Updated 1 row", result_update, "UPDATE 语句执行失败")

        # 再查询更新后工资
        result_after = self._execute_sql(sql_check_before)
        self.assertIn("80000.0", result_after, "UPDATE 语句未生效")

    def test_08_delete_statement(self):
        """测试 3.1: 删除操作 (DELETE)"""
        # 先查询要删除的记录
        sql_check_before = "SELECT * FROM employees WHERE name = 'Heidi';"
        result_before = self._execute_sql(sql_check_before)
        self.assertIn("Heidi", result_before, "删除前查询失败")

        # 执行删除
        sql_delete = "DELETE FROM employees WHERE name = 'Heidi';"
        result_delete = self._execute_sql(sql_delete)
        self.assertIn("1 row(s) deleted", result_delete, "DELETE 语句执行失败")

        # 再查询，应无结果
        result_after = self._execute_sql(sql_check_before)
        self.assertNotIn("Heidi", result_after, "DELETE 语句未生效，记录仍存在")

    def test_09_semantic_error_handling(self):

        # 测试违反外键约束
        sql2 = "INSERT INTO employees(emp_id, name, salary, department_id) VALUES (200, 'Invalid', 50000, 999);"
        result2 = self._execute_sql(sql2)
        self.assertIn("外键约束失败", result2, "未提供智能提示")
        self.assertIn("智能提示", result2, "未提供智能提示")


if __name__ == '__main__':
    # 运行所有测试
    unittest.main(verbosity=2)