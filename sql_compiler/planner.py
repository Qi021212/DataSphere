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
        table_name = self._get_table_name(ast)

        # 获取选择的列
        columns = []
        columns_ast = self._find_child(ast, 'Columns')
        if columns_ast:
            for column_ast in columns_ast.children:
                if column_ast.node_type == 'AllColumns':
                    columns.append('*')
                else:
                    columns.append(column_ast.value)

        # 获取条件
        condition = None
        condition_ast = self._find_child(ast, 'Condition')
        if condition_ast and condition_ast.children:
            condition = self._extract_condition(condition_ast.children[0])

        return ExecutionPlan('Select', {
            'table_name': table_name,
            'columns': columns,
            'condition': condition
        })

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
        for child in ast.children:
            if child.node_type == node_type:
                return child
            result = self._find_child(child, node_type)
            if result:
                return result
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
        elif expr_ast.node_type == 'IntConstant':
            return {'type': 'constant', 'value_type': 'int', 'value': expr_ast.value}
        elif expr_ast.node_type == 'FloatConstant':
            return {'type': 'constant', 'value_type': 'float', 'value': expr_ast.value}
        elif expr_ast.node_type == 'StringConstant':
            return {'type': 'constant', 'value_type': 'string', 'value': expr_ast.value}
        else:
            return {'type': 'unknown'}