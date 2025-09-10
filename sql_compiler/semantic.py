#SQL编译器 - 语义分析器

# sql_compiler/semantic.py
from typing import Dict, List, Any, Optional
from sql_compiler.lexer import TokenType
from sql_compiler.parser import ASTNode
from sql_compiler.catalog import Catalog


class SemanticAnalyzer:
    def __init__(self, catalog: Catalog):
        self.catalog = catalog
        self.errors = []

    def analyze(self, ast: ASTNode) -> List[str]:
        self.errors = []

        if ast.node_type == 'CreateTable':
            self.analyze_create_table(ast)
        elif ast.node_type == 'Insert':
            self.analyze_insert(ast)
        elif ast.node_type == 'Select':
            self.analyze_select(ast)
        elif ast.node_type == 'Delete':
            self.analyze_delete(ast)
        elif ast.node_type == 'MultipleStatements':
            for child in ast.children:
                self.analyze(child)

        return self.errors

    def analyze_create_table(self, ast: ASTNode):
        table_name = self.get_table_name(ast)
        # 检查表是否已存在
        if self.catalog.table_exists(table_name):
            self.errors.append(f"Semantic error: Table '{table_name}' already exists")
            return  # 👈 关键：如果表存在，直接返回，不执行后续操作

        # 解析列定义
        columns_ast = self.find_child(ast, 'Columns')
        if not columns_ast:
            self.errors.append("Semantic error: No columns defined in CREATE TABLE")
            return

        columns = []
        for column_ast in columns_ast.children:
            column_def = column_ast.value
            columns.append({
                'name': column_def['name'],
                'type': column_def['type']
            })

        # 👇 关键修改：移除 self.catalog.create_table 调用
        # 语义分析阶段只负责检查，不负责创建。
        # 创建操作将在 Executor.execute_create_table 中执行。
        pass

    def analyze_insert(self, ast: ASTNode):
        table_name = self.get_table_name(ast)
        if not self.catalog.table_exists(table_name):
            self.errors.append(f"Semantic error: Table '{table_name}' does not exist")
            return

        table_info = self.catalog.get_table_info(table_name)
        column_names_ast = self.find_child(ast, 'ColumnNames')
        specified_columns = []
        if column_names_ast and column_names_ast.children:
            specified_columns = [child.value for child in column_names_ast.children]
        else:
            specified_columns = [col['name'] for col in table_info['columns']]

        values_ast = self.find_child(ast, 'Values')
        if not values_ast:
            self.errors.append("Semantic error: No values specified in INSERT")
            return

        if len(values_ast.children) != len(specified_columns):
            self.errors.append(f"Semantic error: Number of values ({len(values_ast.children)}) "
                               f"does not match number of columns ({len(specified_columns)})")
            return

        # 增强类型检查
        for i, value_ast in enumerate(values_ast.children):
            col_name = specified_columns[i]
            col_info = next((col for col in table_info['columns'] if col['name'] == col_name), None)
            if not col_info:
                continue

            expected_type = col_info['type']
            if value_ast.node_type == 'IntConstant':
                if expected_type not in ['INT', 'FLOAT']:
                    self.errors.append(f"Semantic error: Type mismatch for column '{col_name}', "
                                       f"expected {expected_type}, got INT")
            elif value_ast.node_type == 'FloatConstant':
                if expected_type != 'FLOAT':
                    self.errors.append(f"Semantic error: Type mismatch for column '{col_name}', "
                                       f"expected {expected_type}, got FLOAT")
            elif value_ast.node_type == 'StringConstant':
                if expected_type != 'VARCHAR':
                    self.errors.append(f"Semantic error: Type mismatch for column '{col_name}', "
                                       f"expected {expected_type}, got VARCHAR")

    def analyze_select(self, ast: ASTNode):
        table_name = self.get_table_name(ast)

        # 检查表是否存在
        if not self.catalog.table_exists(table_name):
            self.errors.append(f"Semantic error: Table '{table_name}' does not exist")
            return

        # 获取表结构
        table_info = self.catalog.get_table_info(table_name)

        # 检查选择的列是否存在
        columns_ast = self.find_child(ast, 'Columns')
        if columns_ast:
            for column_ast in columns_ast.children:
                if column_ast.node_type == 'ColumnName':
                    col_name = column_ast.value
                    if not any(col['name'] == col_name for col in table_info['columns']):
                        self.errors.append(
                            f"Semantic error: Column '{col_name}' does not exist in table '{table_name}'")

    def analyze_delete(self, ast: ASTNode):
        table_name = self.get_table_name(ast)

        # 检查表是否存在
        if not self.catalog.table_exists(table_name):
            self.errors.append(f"Semantic error: Table '{table_name}' does not exist")
            return

    def get_table_name(self, ast: ASTNode) -> str:
        table_name_ast = self.find_child(ast, 'TableName')
        if table_name_ast:
            return table_name_ast.value
        return ""

    def find_child(self, ast: ASTNode, node_type: str) -> Optional[ASTNode]:
        for child in ast.children:
            if child.node_type == node_type:
                return child
            result = self.find_child(child, node_type)
            if result:
                return result
        return None