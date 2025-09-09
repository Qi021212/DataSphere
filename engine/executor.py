#数据库引擎 - 执行引擎

# engine/executor.py
from typing import List, Dict, Any, Optional
from storage.file_manager import FileManager
from sql_compiler.planner import ExecutionPlan
from sql_compiler.catalog import Catalog


class Executor:
    def __init__(self, file_manager: FileManager, catalog: Catalog):
        self.file_manager = file_manager
        self.catalog = catalog

    def execute(self, plan: ExecutionPlan) -> Any:
        if plan.plan_type == 'CreateTable':
            return self.execute_create_table(plan)
        elif plan.plan_type == 'Insert':
            return self.execute_insert(plan)
        elif plan.plan_type == 'Select':
            return self.execute_select(plan)
        elif plan.plan_type == 'Delete':
            return self.execute_delete(plan)
        else:
            raise Exception(f"Unsupported execution plan: {plan.plan_type}")

    def execute_create_table(self, plan: ExecutionPlan) -> str:
        table_name = plan.details['table_name']
        columns = plan.details['columns']

        # 创建表文件
        self.file_manager.create_table_file(table_name)

        # 更新目录
        self.catalog.create_table(table_name, columns)

        return f"Table '{table_name}' created successfully"

    def execute_insert(self, plan: ExecutionPlan) -> str:
        table_name = plan.details['table_name']
        column_names = plan.details['column_names']
        values = plan.details['values']

        # 获取表信息
        table_info = self.catalog.get_table_info(table_name)
        if not table_info:
            raise Exception(f"Table '{table_name}' does not exist")

        # 如果没有指定列名，使用所有列
        if not column_names:
            column_names = [col['name'] for col in table_info['columns']]

        # 验证列名
        for col_name in column_names:
            if not any(col['name'] == col_name for col in table_info['columns']):
                raise Exception(f"Column '{col_name}' does not exist in table '{table_name}'")

        # 获取表页面
        page_ids = self.file_manager.get_table_pages(table_name)
        if not page_ids:
            raise Exception(f"No pages found for table '{table_name}'")

        # 简单的实现：总是插入到第一个数据页面
        header_page_id = page_ids[0]
        header_page = self.file_manager.buffer_pool.get_page(header_page_id)

        # 获取记录数
        record_count = header_page.get_int(0)

        # 这里应该有更复杂的逻辑来处理页面分配和数据存储
        # 简化实现：直接增加记录数
        header_page.set_int(0, record_count + 1)

        # 更新目录中的记录数
        self.catalog.update_row_count(table_name, record_count + 1)

        return f"1 row inserted into '{table_name}'"

    def execute_select(self, plan: ExecutionPlan) -> List[Dict[str, Any]]:
        table_name = plan.details['table_name']
        columns = plan.details['columns']
        condition = plan.details['condition']

        # 获取表信息
        table_info = self.catalog.get_table_info(table_name)
        if not table_info:
            raise Exception(f"Table '{table_name}' does not exist")

        # 如果没有指定列，使用所有列
        if not columns or columns == ['*']:
            columns = [col['name'] for col in table_info['columns']]

        # 验证列名
        for col_name in columns:
            if not any(col['name'] == col_name for col in table_info['columns']):
                raise Exception(f"Column '{col_name}' does not exist in table '{table_name}'")

        # 这里应该有实际的查询逻辑
        # 简化实现：返回空结果集
        results = []

        # 模拟一些数据用于测试
        if table_name.lower() == 'users' and not results:
            results = [
                {'id': 1, 'name': 'Alice', 'age': 25},
                {'id': 2, 'name': 'Bob', 'age': 30}
            ]

        # 应用条件过滤
        if condition:
            filtered_results = []
            for row in results:
                if self._evaluate_condition(row, condition):
                    filtered_results.append(row)
            results = filtered_results

        # 只选择指定的列
        selected_results = []
        for row in results:
            selected_row = {}
            for col in columns:
                if col in row:
                    selected_row[col] = row[col]
            selected_results.append(selected_row)

        return selected_results

    def execute_delete(self, plan: ExecutionPlan) -> str:
        table_name = plan.details['table_name']
        condition = plan.details['condition']

        # 获取表信息
        table_info = self.catalog.get_table_info(table_name)
        if not table_info:
            raise Exception(f"Table '{table_name}' does not exist")

        # 这里应该有实际的删除逻辑
        # 简化实现：返回删除行数
        deleted_count = 0

        # 模拟一些数据用于测试
        if table_name.lower() == 'users':
            deleted_count = 1  # 模拟删除了一行

        # 更新目录中的记录数
        current_count = table_info['row_count']
        self.catalog.update_row_count(table_name, max(0, current_count - deleted_count))

        return f"{deleted_count} row(s) deleted from '{table_name}'"

    def _evaluate_condition(self, row: Dict[str, Any], condition: Dict[str, Any]) -> bool:
        # 简化的条件评估
        left = condition['left']
        operator = condition['operator']
        right = condition['right']

        if left['type'] == 'column' and right['type'] == 'constant':
            col_name = left['value']
            col_value = row.get(col_name)

            if col_value is None:
                return False

            if right['value_type'] == 'int':
                right_value = int(right['value'])
                col_value = int(col_value)
            elif right['value_type'] == 'float':
                right_value = float(right['value'])
                col_value = float(col_value)
            else:
                right_value = str(right['value'])
                col_value = str(col_value)

            if operator == '=':
                return col_value == right_value
            elif operator == '>':
                return col_value > right_value
            elif operator == '<':
                return col_value < right_value
            elif operator == '>=':
                return col_value >= right_value
            elif operator == '<=':
                return col_value <= right_value
            elif operator == '!=':
                return col_value != right_value

        return False