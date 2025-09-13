#SQL编译器 - 执行计划生成器

# sql_compiler/planner.py
from typing import Dict, List, Any
from sql_compiler.parser import ASTNode


class ExecutionPlan:
    def __init__(self, plan_type: str, details: Dict[str, Any] = None):
        self.plan_type = plan_type
        self.details = details if details is not None else {}

    def __repr__(self):
        return f"{self.plan_type}({self.details})"


class Planner:
    def __init__(self):
        pass

    def generate_plan(self, ast: ASTNode) -> ExecutionPlan:
        if ast.node_type == 'CreateTable':
            return self.plan_create_table(ast)
        elif ast.node_type == 'Insert':
            return self.plan_insert(ast)
        elif ast.node_type == 'Select':
            return self.plan_select(ast)
        elif ast.node_type == 'Delete':
            return self.plan_delete(ast)
        elif ast.node_type == 'Update':  # 👈 新增
            return self.plan_update(ast)
        else:
            raise Exception(f"Unsupported AST node type: {ast.node_type}")

    def plan_create_table(self, ast: ASTNode) -> ExecutionPlan:
        table_name = self._get_table_name(ast)
        columns_ast = self._find_child(ast, 'Columns')
        columns = []
        for column_ast in columns_ast.children:
            columns.append(column_ast.value)

        # 👇 新增：提取约束
        constraints = []
        constraints_ast = self._find_child(ast, 'Constraints')
        if constraints_ast and constraints_ast.node_type != 'NoConstraints':
            for constraint_ast in constraints_ast.children:
                constraints.append(constraint_ast.value)

        return ExecutionPlan('CreateTable', {
            'table_name': table_name,
            'columns': columns,
            'constraints': constraints  # 👈 传递约束
        })

    def plan_insert(self, ast: ASTNode) -> ExecutionPlan:
        table_name = self._get_table_name(ast)

        # 获取列名
        column_names = []
        column_names_ast = self._find_child(ast, 'ColumnNames')
        if column_names_ast:
            column_names = [child.value for child in column_names_ast.children]

        # 获取值
        values = []
        values_ast = self._find_child(ast, 'Values')
        if values_ast:
            for value_ast in values_ast.children:
                if value_ast.node_type == 'IntConstant':
                    values.append(('int', value_ast.value))
                elif value_ast.node_type == 'FloatConstant':
                    values.append(('float', value_ast.value))
                elif value_ast.node_type == 'StringConstant':
                    values.append(('string', value_ast.value))
                elif value_ast.node_type == 'Identifier':
                    values.append(('identifier', value_ast.value))

        return ExecutionPlan('Insert', {
            'table_name': table_name,
            'column_names': column_names,
            'values': values
        })

    def plan_select(self, ast: ASTNode) -> ExecutionPlan:
        # 获取 TableSource
        table_source_ast = self._find_child(ast, 'TableSource')
        if not table_source_ast or not table_source_ast.children:
            raise Exception("No table source in SELECT")
        # 生成表源的执行计划
        table_source_plan = self._plan_table_source(table_source_ast.children[0])

        # 👇👇👇 关键修复：初始化 aggregates 列表 👇👇👇
        aggregates = []
        columns = []  # 这个列表现在只用于非聚合的普通列
        # 👆👆👆 关键修复结束 👆👆👆

        columns_ast = self._find_child(ast, 'Columns')
        if columns_ast:
            for column_ast in columns_ast.children:
                if column_ast.node_type == 'AllColumns':
                    columns.append('*')
                # 👇👇👇 关键修复：添加对 AggregateFunction 节点的处理 👇👇👇
                elif column_ast.node_type == 'AggregateFunction':
                    func_name_ast = self._find_child(column_ast, 'FunctionName')
                    param_ast = self._find_child(column_ast, 'Parameter')
                    if not func_name_ast or not param_ast or not param_ast.children:
                        raise Exception("Malformed aggregate function in AST")
                    func_name = func_name_ast.value
                    param_node = param_ast.children[0]
                    if param_node.node_type == 'AllColumns':
                        col_name = '*'  # COUNT(*)
                    elif param_node.node_type == 'Identifier' or param_node.node_type == 'ColumnRef':
                        col_name = param_node.value  # e.g., 'e.salary'
                    else:
                        raise Exception(f"Unsupported parameter type for aggregate function: {param_node.node_type}")
                    # 将聚合函数信息添加到 aggregates 列表，而不是 columns 列表
                    aggregates.append({
                        'function': func_name,
                        'column': col_name
                    })
                # 👆👆👆 关键修复结束 👆👆👆
                else:
                    # 处理普通列
                    columns.append(column_ast.value)

        # 获取 SELECT 语句的 WHERE 条件
        # 获取 SELECT 语句的 WHERE 条件
        where_condition = None
        if len(ast.children) > 2:  # Select 节点应该有 3 个子节点: Columns, TableSource, Condition/NoCondition
            condition_node = ast.children[2]
            if condition_node.node_type == 'Condition' and condition_node.children:
                where_condition = self._extract_condition(condition_node.children[0])

        return ExecutionPlan('Select', {
            'table_source': table_source_plan,
            'columns': columns,  # 👈 只包含普通列
            'aggregates': aggregates,  # 👈 新增：包含聚合函数信息
            'condition': where_condition
        })

    # 👇 新增方法：为表源生成执行计划
    def _plan_table_source(self, ts_ast: ASTNode) -> Dict:
        """为表源生成执行计划"""
        if ts_ast.node_type == 'Table':
            # 处理单表
            table_name_ast = self._find_child(ts_ast, 'TableName')
            if not table_name_ast:
                raise Exception("Table node missing TableName child")
            table_name = table_name_ast.value

            # 可选：处理别名
            alias_ast = self._find_child(ts_ast, 'Alias')
            alias = alias_ast.value if alias_ast else table_name

            return {
                'type': 'TableScan',
                'table_name': table_name,
                'alias': alias  # 传递别名，供后续阶段使用
            }
        elif ts_ast.node_type == 'Join':
            # 处理 JOIN
            join_type_ast = self._find_child(ts_ast, 'JoinType')
            left_ast = self._find_child(ts_ast, 'LeftTable')
            right_ast = self._find_child(ts_ast, 'RightTable')
            cond_ast = self._find_child(ts_ast, 'JoinCondition')

            if not (
                    left_ast and left_ast.children and right_ast and right_ast.children and cond_ast and cond_ast.children):
                raise Exception("Malformed Join AST")

            return {
                'type': 'Join',
                'join_type': join_type_ast.value if join_type_ast else 'INNER',
                'left': self._plan_table_source(left_ast.children[0]),
                'right': self._plan_table_source(right_ast.children[0]),
                'condition': self._extract_condition(cond_ast.children[0])
            }
        else:
            raise Exception(f"Unsupported table source type: {ts_ast.node_type}")

    def plan_delete(self, ast: ASTNode) -> ExecutionPlan:
        table_name = self._get_table_name(ast)

        # 获取条件
        condition = None
        condition_ast = self._find_child(ast, 'Condition')
        if condition_ast and condition_ast.children:
            condition = self._extract_condition(condition_ast.children[0])

        return ExecutionPlan('Delete', {
            'table_name': table_name,
            'condition': condition
        })

    def plan_update(self, ast: ASTNode) -> ExecutionPlan:  # 👈 新增方法
        table_name = self._get_table_name(ast)
        # 提取 SET 子句
        set_clause = []
        set_clause_ast = self._find_child(ast, 'SetClause')
        if set_clause_ast:
            for assignment_ast in set_clause_ast.children:
                col_ast = self._find_child(assignment_ast, 'Column')
                val_ast = assignment_ast.children[1]  # 假设第二个子节点是值
                col_name = col_ast.value
                value = self._extract_expression(val_ast)
                set_clause.append((col_name, value))
        # 提取条件
        condition = None
        condition_ast = self._find_child(ast, 'Condition')
        if condition_ast and condition_ast.children:
            condition = self._extract_condition(condition_ast.children[0])
        return ExecutionPlan('Update', {  # 👈 计划类型为 'Update'
            'table_name': table_name,
            'set_clause': set_clause,
            'condition': condition
        })

    def _get_table_name(self, ast: ASTNode) -> str:
        table_name_ast = self._find_child(ast, 'TableName')
        if table_name_ast:
            return table_name_ast.value
        return ""

    def _find_child(self, ast: ASTNode, node_type: str) -> Any:
        # 👇 修改：只遍历直接子节点，不递归
        for child in ast.children:
            if child.node_type == node_type:
                return child
        # 👇 移除递归调用
        # result = self._find_child(child, node_type)
        # if result:
        #     return result
        return None

    def _extract_condition(self, condition_ast: ASTNode) -> Dict[str, Any]:
        left_ast = self._find_child(condition_ast, 'Left')
        operator_ast = self._find_child(condition_ast, 'Operator')
        right_ast = self._find_child(condition_ast, 'Right')

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
        elif expr_ast.node_type == 'ColumnRef':  # 👈 确保这个分支能被触发
            return {'type': 'column', 'value': expr_ast.value}
        elif expr_ast.node_type == 'IntConstant':
            return {'type': 'constant', 'value_type': 'int', 'value': expr_ast.value}
        elif expr_ast.node_type == 'FloatConstant':
            return {'type': 'constant', 'value_type': 'float', 'value': expr_ast.value}
        elif expr_ast.node_type == 'StringConstant':
            return {'type': 'constant', 'value_type': 'string', 'value': expr_ast.value}
        else:
            print(f"DEBUG: Unknown expression type: {expr_ast.node_type}")  # 👈 新增调试
            return {'type': 'unknown'}