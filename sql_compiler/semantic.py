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
        elif ast.node_type == 'Update': # 新增对 Update 的支持
            self.analyze_update(ast)
        elif ast.node_type == 'MultipleStatements':
            for child in ast.children:
                self.analyze(child)
        return self.errors

    def analyze_create_table(self, ast: ASTNode):
        table_name = self.get_table_name(ast)
        # 检查表是否已存在 (只检查，不创建)
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

        # --- 新增：检查外键约束 ---
        constraints_ast = self.find_child(ast, 'Constraints')
        if constraints_ast:
            for constraint in constraints_ast.value:
                if constraint[0] == 'FOREIGN_KEY':
                    _, fk_col, ref_table, ref_col = constraint
                    # 检查引用的表是否存在
                    if not self.catalog.table_exists(ref_table):
                        self.errors.append(f"Semantic error: Referenced table '{ref_table}' does not exist")
                        continue
                    # 检查引用的列是否存在
                    ref_table_info = self.catalog.get_table_info(ref_table)
                    if not any(col['name'] == ref_col for col in ref_table_info['columns']):
                        self.errors.append(f"Semantic error: Column '{ref_col}' does not exist in table '{ref_table}'")

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
        table_source_ast = self.find_child(ast, 'TableSource')
        if not table_source_ast or not table_source_ast.children:
            self.errors.append("Semantic error: No table source specified in SELECT")
            return

        alias_to_table = {}
        self._collect_table_aliases(table_source_ast.children[0], alias_to_table)

        columns_ast = self.find_child(ast, 'Columns')
        if columns_ast:
            for column_ast in columns_ast.children:
                if column_ast.node_type == 'AggregateFunction':
                    func_name_ast = self.find_child(column_ast, 'FunctionName')
                    param_ast = self.find_child(column_ast, 'Parameter')

                    if not func_name_ast or not param_ast or not param_ast.children:
                        self.errors.append("Semantic error: Malformed aggregate function")
                        continue

                    func_name = func_name_ast.value
                    param = param_ast.children[0]

                    if param.node_type == 'AllColumns':
                        if func_name != 'COUNT':
                            self.errors.append(f"Semantic error: '{func_name}' cannot be applied to '*'")
                        continue

                    if param.node_type == 'Identifier' or param.node_type == 'ColumnRef':
                        full_col_name = param.value
                        if '.' in full_col_name:
                            alias, col_name = full_col_name.split('.', 1)
                            actual_table_name = alias_to_table.get(alias)
                            if not actual_table_name:
                                self.errors.append(f"Semantic error: Table alias '{alias}' is not defined")
                                continue
                            table_info = self.catalog.get_table_info(actual_table_name)
                            col_info = next((col for col in table_info['columns'] if col['name'] == col_name), None)
                        else:
                            col_name = full_col_name
                            col_info = None
                            for actual_table_name in alias_to_table.values():
                                table_info = self.catalog.get_table_info(actual_table_name)
                                col_info = next((col for col in table_info['columns'] if col['name'] == col_name), None)
                                if col_info:
                                    break

                            if not col_info:
                                self.errors.append(
                                    f"Semantic error: Column '{col_name}' does not exist in any of the specified tables")
                                continue

                        if func_name in ['SUM', 'AVG'] and col_info['type'] not in ['INT', 'FLOAT']:
                            self.errors.append(
                                f"Semantic error: '{func_name}' requires a numeric column, got '{col_info['type']}' for column '{col_name}'")

                    else:
                        self.errors.append(
                            f"Semantic error: Unsupported parameter type for aggregate function '{func_name}'")

                elif column_ast.node_type == 'ColumnName':
                    full_col_name = column_ast.value
                    if '.' in full_col_name:
                        alias, col_name = full_col_name.split('.', 1)
                        actual_table_name = alias_to_table.get(alias)
                        if not actual_table_name:
                            self.errors.append(f"Semantic error: Table alias '{alias}' is not defined")
                            continue
                        table_info = self.catalog.get_table_info(actual_table_name)
                        if not any(col['name'] == col_name for col in table_info['columns']):
                            self.errors.append(
                                f"Semantic error: Column '{col_name}' does not exist in table '{actual_table_name}' (aliased as '{alias}')"
                            )
                    else:
                        col_name = full_col_name
                        found = False
                        for actual_table_name in alias_to_table.values():
                            table_info = self.catalog.get_table_info(actual_table_name)
                            if any(col['name'] == col_name for col in table_info['columns']):
                                found = True
                                break
                        if not found:
                            self.errors.append(
                                f"Semantic error: Column '{col_name}' does not exist in any of the specified tables"
                            )

        condition_ast = self.find_child(ast, 'Condition')
        if condition_ast and condition_ast.children:
            self._analyze_condition(condition_ast.children[0], alias_to_table)

    def _collect_table_aliases(self, ts_ast: ASTNode, alias_to_table: dict):
        """递归收集表源中的别名映射"""
        if ts_ast.node_type == 'Table':
            table_name_ast = self.find_child(ts_ast, 'TableName')
            alias_ast = self.find_child(ts_ast, 'Alias')
            table_name = table_name_ast.value
            alias = alias_ast.value if alias_ast else table_name  # 如果没有显式别名，则用表名作为别名
            alias_to_table[alias] = table_name
        elif ts_ast.node_type == 'Join':
            left_ast = self.find_child(ts_ast, 'LeftTable')
            right_ast = self.find_child(ts_ast, 'RightTable')
            if left_ast and left_ast.children:
                self._collect_table_aliases(left_ast.children[0], alias_to_table)
            if right_ast and right_ast.children:
                self._collect_table_aliases(right_ast.children[0], alias_to_table)

    def _analyze_table_source(self, ts_ast: ASTNode, involved_tables: dict):
        """递归分析表源节点，验证表存在性"""
        if ts_ast.node_type == 'TableName':
            table_name = ts_ast.value
            if not self.catalog.table_exists(table_name):
                self.errors.append(f"Semantic error: Table '{table_name}' does not exist")
            # 假设表名即别名，在不支持显式别名的情况下
            involved_tables[table_name] = table_name
        elif ts_ast.node_type == 'Join':
            # 递归分析左表和右表
            left_ast = self.find_child(ts_ast, 'LeftTable')
            right_ast = self.find_child(ts_ast, 'RightTable')
            if left_ast and left_ast.children:
                self._analyze_table_source(left_ast.children[0], involved_tables)
            if right_ast and right_ast.children:
                self._analyze_table_source(right_ast.children[0], involved_tables)

            # 验证 JoinCondition
            cond_ast = self.find_child(ts_ast, 'JoinCondition')
            if cond_ast and cond_ast.children:
                self._analyze_join_condition(cond_ast.children[0], involved_tables)

    def _analyze_join_condition(self, cond_ast: ASTNode, involved_tables: dict):
        """分析 JOIN ON 条件，确保引用的列存在于对应的表中"""
        # 这里可以复用或参考 _extract_condition 的逻辑来解析条件
        # 然后检查 left 和 right 表达式中的列
        # 由于实现细节较多，此处为简化示例，假设条件是简单的 column = column
        # 实际需要递归遍历 AST 来找到所有的 Identifier 节点
        pass  # 需要根据你的 AST 结构详细实现

    def _analyze_condition(self, cond_ast: ASTNode, alias_to_table: dict):
        """分析条件表达式中的列引用"""
        left_ast = self.find_child(cond_ast, 'Left')
        right_ast = self.find_child(cond_ast, 'Right')

        if left_ast and left_ast.children:
            self._analyze_expression(left_ast.children[0], alias_to_table)
        if right_ast and right_ast.children:
            self._analyze_expression(right_ast.children[0], alias_to_table)

    def analyze_delete(self, ast: ASTNode):
        table_name = self.get_table_name(ast)
        # 检查表是否存在
        if not self.catalog.table_exists(table_name):
            self.errors.append(f"Semantic error: Table '{table_name}' does not exist")
            return

    # --- 新增方法：分析 UPDATE 语句 ---
    def analyze_update(self, ast: ASTNode):
        table_name = self.get_table_name(ast)
        # 检查表是否存在
        if not self.catalog.table_exists(table_name):
            self.errors.append(f"Semantic error: Table '{table_name}' does not exist")
            return

        table_info = self.catalog.get_table_info(table_name)

        # 检查 SET 子句中的列是否存在和类型
        set_clause_ast = self.find_child(ast, 'SetClause')
        if set_clause_ast:
            for col_name, value_info in set_clause_ast.value:
                # 检查列是否存在
                if not any(col['name'] == col_name for col in table_info['columns']):
                    self.errors.append(f"Semantic error: Column '{col_name}' does not exist in table '{table_name}'")
                    continue
                # 检查类型 (简化)
                col_info = next((col for col in table_info['columns'] if col['name'] == col_name), None)
                value_type, value = value_info
                expected_type = col_info['type']
                if value_type == 'int' and expected_type not in ['INT', 'FLOAT']:
                    self.errors.append(f"Semantic error: Type mismatch for column '{col_name}', "
                                       f"expected {expected_type}, got INT")
                elif value_type == 'float' and expected_type != 'FLOAT':
                    self.errors.append(f"Semantic error: Type mismatch for column '{col_name}', "
                                       f"expected {expected_type}, got FLOAT")
                elif value_type == 'string' and expected_type != 'VARCHAR':
                    self.errors.append(f"Semantic error: Type mismatch for column '{col_name}', "
                                       f"expected {expected_type}, got VARCHAR")

        # 检查 WHERE 条件中的列是否存在
        condition_ast = self.find_child(ast, 'Condition')
        if condition_ast and condition_ast.children:
            condition = self._extract_condition(condition_ast.children[0])
            left = condition.get('left', {})
            if left.get('type') == 'column':
                col_name = left['value']
                if not any(col['name'] == col_name for col in table_info['columns']):
                    self.errors.append(f"Semantic error: Column '{col_name}' in WHERE clause does not exist")

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

    # 从 planner.py 复制过来的辅助方法，用于语义分析
    def _extract_condition(self, condition_ast: ASTNode) -> Dict[str, Any]:
        left_ast = self.find_child(condition_ast, 'Left')
        operator_ast = self.find_child(condition_ast, 'Operator')
        right_ast = self.find_child(condition_ast, 'Right')
        if left_ast and operator_ast and right_ast:
            left = self._extract_expression(left_ast.children[0])
            operator = operator_ast.value
            right = self._extract_expression(right_ast.children[0])
            return {
                'left': left,
                'operator': operator,
                'right': right
            }
        return {}

    def _extract_expression(self, expr_ast: ASTNode) -> Dict[str, Any]:
        if expr_ast.node_type == 'Identifier':
            return {'type': 'column', 'value': expr_ast.value}
        elif expr_ast.node_type == 'IntConstant':
            return {'type': 'constant', 'value_type': 'int', 'value': expr_ast.value}
        elif expr_ast.node_type == 'FloatConstant':
            return {'type': 'constant', 'value_type': 'float', 'value': expr_ast.value}
        elif expr_ast.node_type == 'StringConstant':
            return {'type': 'constant', 'value_type': 'string', 'value': expr_ast.value}
        else:
            return {'type': 'unknown'}

    def _analyze_expression(self, expr_ast: ASTNode, alias_to_table: dict):
        """分析表达式节点，如果是列引用则进行验证"""
        if expr_ast.node_type == 'Identifier':
            full_col_name = expr_ast.value
            if '.' in full_col_name:
                alias, col_name = full_col_name.split('.', 1)
                actual_table_name = alias_to_table.get(alias)
                if not actual_table_name:
                    self.errors.append(f"Semantic error: Table alias '{alias}' is not defined")
                    return
                table_info = self.catalog.get_table_info(actual_table_name)
                if not any(col['name'] == col_name for col in table_info['columns']):
                    self.errors.append(
                        f"Semantic error: Column '{col_name}' does not exist in table '{actual_table_name}'"
                    )
            else:
                # 处理无别名的列，在所有表中查找
                col_name = full_col_name
                found = False
                for actual_table_name in alias_to_table.values():
                    table_info = self.catalog.get_table_info(actual_table_name)
                    if any(col['name'] == col_name for col in table_info['columns']):
                        found = True
                        break
                if not found:
                    self.errors.append(
                        f"Semantic error: Column '{col_name}' does not exist in any of the specified tables"
                    )