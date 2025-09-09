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
            return

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

        # 添加到目录
        self.catalog.create_table(table_name, columns)

    def analyze_insert(self, ast: ASTNode):
        table_name = self.get_table_name(ast)

        # 检查表是否存在
        if not self.catalog.table_exists(table_name):
            self.errors.append(f"Semantic error: Table '{table_name}' does not exist")
            return

        # 获取表结构
        table_info = self.catalog.get_table_info(table_name)

        # 解析列名（如果有）
        column_names_ast = self.find_child(ast, 'ColumnNames')
        if column_names_ast and column_names_ast.children:
            specified_columns = [child.value for child in column_names_ast.children]

            # 检查指定的列是否存在
            for col_name in specified_columns:
                if not any(col['name'] == col_name for col in table_info['columns']):
                    self.errors.append(f"Semantic error: Column '{col_name}' does not exist in table '{table_name}'")

            # 检查是否有重复列
            if len(specified_columns) != len(set(specified_columns)):
                self.errors.append(f"Semantic error: Duplicate column names in INSERT statement")
        else:
            # 如果没有指定列名，使用所有列
            specified_columns = [col['name'] for col in table_info['columns']]

        # 解析值
        values_ast = self.find_child(ast, 'Values')
        if not values_ast:
            self.errors.append("Semantic error: No values specified in INSERT")
            return

        # 检查值数量是否匹配
        if len(values_ast.children) != len(specified_columns):
            self.errors.append(f"Semantic error: Number of values ({len(values_ast.children)}) "
                               f"does not match number of columns ({len(specified_columns)})")
            return

        # 检查类型一致性
        for i, value_ast in enumerate(values_ast.children):
            col_name = specified_columns[i]
            column_info = next((col for col in table_info['columns'] if col['name'] == col_name), None)

            if column_info and value_ast.node_type in ['IntConstant', 'FloatConstant', 'StringConstant']:
                expected_type = column_info['type']
                actual_type = value_ast.node_type.replace('Constant', '').lower()

                # 简单的类型检查
                if expected_type == 'INT' and actual_type != 'int':
                    self.errors.append(f"Semantic error: Type mismatch for column '{col_name}', "
                                       f"expected INT, got {actual_type.upper()}")
                elif expected_type == 'FLOAT' and actual_type not in ['int', 'float']:
                    self.errors.append(f"Semantic error: Type mismatch for column '{col_name}', "
                                       f"expected FLOAT, got {actual_type.upper()}")

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