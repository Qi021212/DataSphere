#数据库引擎 - 执行引擎

# engine/executor.py
from typing import List, Dict, Any, Optional

from storage.file_manager import FileManager
from sql_compiler.planner import ExecutionPlan
from sql_compiler.catalog import Catalog


def _evaluate_condition(row: Dict[str, Any], condition: Dict[str, Any]) -> bool:
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


class Executor:
    def __init__(self, file_manager: FileManager, catalog: Catalog):
        self.storage_engine = None
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

        # 调用存储引擎创建表
        self.file_manager.create_table_file(table_name, columns)

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

        # 构建记录字典
        record = {}
        for i, col_name in enumerate(column_names):
            value_info = values[i]
            value_type, value = value_info
            record[col_name] = value

        # 调用 FileManager 插入记录 (核心修改)
        success = self.file_manager.insert_record(table_name, record)
        if not success:
            raise Exception("Failed to insert record")

        # 更新目录中的记录数
        current_count = table_info['row_count']
        self.catalog.update_row_count(table_name, current_count + 1)

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

        # 调用 FileManager 读取记录 (核心修改)
        raw_results = self.file_manager.read_records(table_name, condition)

        # 只选择指定的列
        selected_results = []
        for row in raw_results:
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

        # 调用 FileManager 删除记录
        deleted_count = self.file_manager.delete_records(table_name, condition)

        # 更新目录中的记录数
        current_count = table_info['row_count']
        self.catalog.update_row_count(table_name, max(0, current_count - deleted_count))

        return f"{deleted_count} row(s) deleted from '{table_name}'"


