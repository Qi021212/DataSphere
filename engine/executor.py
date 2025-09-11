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
        elif plan.plan_type == 'Update':  # 👈 新增
            return self.execute_update(plan)
        else:
            raise Exception(f"Unsupported execution plan: {plan.plan_type}")

    def execute_create_table(self, plan: ExecutionPlan) -> str:
        table_name = plan.details['table_name']
        columns = plan.details['columns']
        constraints = plan.details.get('constraints', [])  # 👈 获取约束
        # 调用存储引擎创建表
        self.file_manager.create_table_file(table_name, columns)
        # 更新目录，并保存约束
        # 👇 关键：调用修改后的 create_table 方法
        if hasattr(self.catalog, 'create_table') and len(self.catalog.create_table.__code__.co_varnames) > 2:
            self.catalog.create_table(table_name, columns, constraints)
        else:
            # 兼容旧的 create_table 方法
            self.catalog.create_table(table_name, columns)
            # 手动添加约束
            if constraints:
                table_info = self.catalog.get_table_info(table_name)
                table_info['constraints'] = constraints
                self.catalog._save_catalog()
        return f"Table '{table_name}' created successfully"

    def execute_insert(self, plan: ExecutionPlan) -> str:
        table_name = plan.details['table_name']
        column_names = plan.details['column_names']
        values = plan.details['values']
        table_info = self.catalog.get_table_info(table_name)
        if not table_info:
            raise Exception(f"Table '{table_name}' does not exist")
        if not column_names:
            column_names = [col['name'] for col in table_info['columns']]

        # 构建记录字典
        record = {}
        for i, col_name in enumerate(column_names):
            value_info = values[i]
            value_type, value = value_info
            record[col_name] = value

        # 👇 关键新增：外键检查
        table_info = self.catalog.get_table_info(table_name)  # 确保获取最新信息
        for col_name, value in record.items():
            for constraint in table_info.get('constraints', []):
                if constraint[0] == 'FOREIGN_KEY' and constraint[1] == col_name:
                    _, _, ref_table, ref_col = constraint
                    if not self._check_reference_exists(ref_table, ref_col, value):
                        raise Exception(f"❌ 外键约束失败：{col_name}={value} 在 {ref_table}({ref_col}) 中不存在")

        # --- 核心修改：调用FileManager真正插入记录 ---
        success = self.file_manager.insert_record(table_name, record)
        if not success:
            raise Exception("Failed to insert record")
        # 更新目录中的记录数
        current_count = table_info['row_count']
        self.catalog.update_row_count(table_name, current_count + 1)
        self.file_manager.flush_all()
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

    def execute_update(self, plan: ExecutionPlan) -> str:
        """执行 UPDATE 语句"""
        table_name = plan.details['table_name']
        set_clause = plan.details['set_clause']  # [(col, value_dict), ...]
        condition = plan.details['condition']

        # 获取表信息
        table_info = self.catalog.get_table_info(table_name)
        if not table_info:
            raise Exception(f"Table '{table_name}' does not exist")

        # 👇 关键修复：正确提取值并进行类型转换
        typed_set_clause = []
        for col_name, value_dict in set_clause:
            # 找到该列的类型
            col_type = next((col['type'] for col in table_info['columns'] if col['name'] == col_name), None)
            if col_type is None:
                raise Exception(f"Column '{col_name}' does not exist in table '{table_name}'")
            # 从字典中提取实际的值字符串
            str_value = value_dict['value']  # 👈 关键：先提取 'value'
            # 根据类型转换值
            if col_type == 'INT':
                typed_value = int(str_value)  # 将字符串转为整数
            elif col_type == 'FLOAT':
                typed_value = float(str_value)
            else:  # VARCHAR 或其他类型，保持为字符串
                typed_value = str_value
            typed_set_clause.append((col_name, typed_value))

        # 👇 关键修复：同样，对 WHERE 条件中的值进行类型转换
        typed_condition = condition
        if condition:
            # 假设 condition 结构为 {'left': {...}, 'operator': '...', 'right': {...}}
            right_value_dict = condition['right']
            right_value_str = right_value_dict['value']  # 👈 关键：先提取 'value'
            # 更可靠的方式：根据列名查找实际类型
            col_name = condition['left']['value']
            col_actual_type = next((col['type'] for col in table_info['columns'] if col['name'] == col_name), None)
            if col_actual_type == 'INT':
                typed_condition['right']['value'] = int(right_value_str)
            elif col_actual_type == 'FLOAT':
                typed_condition['right']['value'] = float(right_value_str)
            # 对于 VARCHAR，保持字符串，无需转换

        # 调用 FileManager 执行更新 (传入已转换类型的值)
        updated_count = self.file_manager.update_records(table_name, typed_set_clause, typed_condition)

        # 👇 关键新增：级联更新逻辑
        if updated_count > 0 and condition:
            # 获取 WHERE 条件，我们假设它用于定位被更新的旧值
            where_col, where_op, where_value_str = condition['left']['value'], condition['operator'], condition['right']['value']
            # 我们只处理 `=` 操作符的简单情况
            if where_op == '=':
                old_value = where_value_str
                # 检查被更新的列是否是其他表的外键目标
                for set_col, new_value in set_clause: # 这里用原始的 set_clause，因为 new_value 用于构造新语句
                    if set_col == where_col:
                        referencing_tables = self.catalog.find_referencing_tables(table_name, set_col)
                        for ref_table_name, ref_col_name in referencing_tables:
                            # 构造级联更新的执行计划
                            cascade_plan = ExecutionPlan('Update', {
                                'table_name': ref_table_name,
                                'set_clause': [(ref_col_name, new_value)], # new_value 是字符串，符合 ExecutionPlan 期望
                                'condition': {
                                    'left': {'type': 'column', 'value': ref_col_name},
                                    'operator': '=',
                                    'right': {'type': 'constant', 'value_type': 'string', 'value': old_value}
                                }
                            })
                            # 递归调用 execute_update 来执行级联更新
                            cascade_result = self.execute_update(cascade_plan)

        return f"Updated {updated_count} row(s)"

    def _check_reference_exists(self, table_name, column_name, value):
        """检查引用表中是否存在该值"""
        table_info = self.catalog.get_table_info(table_name)
        if not table_info:
            return False

        # 构造查询条件
        condition = {
            'left': {'type': 'column', 'value': column_name},
            'operator': '=',
            'right': {'type': 'constant', 'value_type': 'string', 'value': str(value)}
        }

        # 使用 FileManager 读取记录
        records = self.file_manager.read_records(table_name, condition)
        return len(records) > 0