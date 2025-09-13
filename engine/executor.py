#数据库引擎 - 执行引擎

# engine/executor.py
from typing import List, Dict, Any, Optional

from storage.file_manager import FileManager
from sql_compiler.planner import ExecutionPlan
from sql_compiler.catalog import Catalog
from engine.storage_engine import StorageEngine


def _evaluate_condition(row: Dict[str, Any], condition: Dict[str, Any]) -> bool:
    def extract_value(expr: Dict[str, Any], row: Dict[str, Any]):
        if expr['type'] == 'column':
            col_name = expr['value']
            return row.get(col_name)
        elif expr['type'] == 'constant':
            return expr['value']
        else:
            return None

    left = condition['left']
    operator = condition['operator']
    right = condition['right']

    left_value = extract_value(left, row)
    right_value = extract_value(right, row)

    if left_value is None or right_value is None:
        return False

    try:
        if operator == '=':
            result = left_value == right_value
        elif operator == '>':
            result = left_value > right_value
        elif operator == '<':
            result = left_value < right_value
        elif operator == '>=':
            result = left_value >= right_value
        elif operator == '<=':
            result = left_value <= right_value
        elif operator == '!=':
            result = left_value != right_value
        else:
            return False

        return result
    except TypeError as e:
        print(f"DEBUG: TypeError during comparison: {e}")
        return False


class Executor:
    def __init__(self, file_manager: FileManager, catalog: Catalog):
        self.storage_engine = StorageEngine(file_manager)
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
        # 获取必要信息
        table_source_plan = plan.details['table_source']
        columns = plan.details['columns']
        condition = plan.details.get('condition')  # SELECT 的 WHERE 条件
        aggregates = plan.details.get('aggregates', [])  # 获取聚合函数信息

        # 执行表源计划，获取原始数据
        if table_source_plan['type'] == 'TableScan':
            table_name = table_source_plan['table_name']
            table_info = self.catalog.get_table_info(table_name)
            if not table_info:
                raise Exception(f"Table '{table_name}' does not exist")
            raw_results = self.file_manager.read_records(table_name)
        elif table_source_plan['type'] == 'Join':
            raw_results = self._execute_join(table_source_plan)
        else:
            raise Exception(f"Unsupported table source type: {table_source_plan['type']}")

        # 应用 SELECT 语句的 WHERE 条件
        if condition:
            filtered_raw_results = []
            for row in raw_results:
                if _evaluate_condition(row, condition):
                    filtered_raw_results.append(row)
            raw_results = filtered_raw_results

        # 处理聚合函数
        if aggregates:
            result_row = {}
            for agg in aggregates:
                func_name = agg['function']
                col_name = agg['column']
                values = []
                for row in raw_results:  # 现在 raw_results 已经过 WHERE 过滤
                    if col_name == '*':
                        values.append(1)
                    else:
                        value = row.get(col_name)
                        if value is not None:
                            if func_name in ['SUM', 'AVG']:
                                try:
                                    value = float(value)
                                    values.append(value)
                                except ValueError:
                                    pass
                            else:
                                values.append(value)
                if func_name == 'COUNT':
                    result = len(values)
                elif func_name == 'SUM':
                    result = sum(values) if values else 0
                elif func_name == 'AVG':
                    result = sum(values) / len(values) if values else 0
                else:
                    raise Exception(f"Unsupported aggregate function: {func_name}")
                column_alias = f"{func_name}({col_name})"
                result_row[column_alias] = result
            return [result_row]

        # 处理普通列选择
        selected_results = []
        for row in raw_results:  # 此时的 raw_results 已经过 WHERE 过滤
            selected_row = {}
            for col in columns:
                if col == '*':
                    selected_row = row.copy()
                    break
                else:
                    if col in row:
                        selected_row[col] = row[col]
                    else:
                        selected_row[col] = None
            selected_results.append(selected_row)

        return selected_results

    # 👇 新增核心方法：执行表源计划
    def _execute_table_source(self, ts_plan: Dict) -> List[Dict[str, Any]]:
        """执行表源计划"""
        if ts_plan['type'] == 'TableScan':
            table_name = ts_plan['table_name']
            # 直接从文件管理器读取所有记录
            return self.file_manager.read_records(table_name)
        elif ts_plan['type'] == 'Join':
            return self._execute_join(ts_plan)
        else:
            raise Exception(f"Unsupported table source plan type: {ts_plan['type']}")

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

    def _execute_join(self, join_plan: Dict) -> List[Dict[str, Any]]:
        """执行 JOIN 操作"""
        join_type = join_plan['join_type']
        left_plan = join_plan['left']
        right_plan = join_plan['right']
        join_condition = join_plan['condition']

        # 递归执行左表和右表
        left_results = self._execute_table_source(left_plan)
        right_results = self._execute_table_source(right_plan)

        joined_results = []

        if join_type == 'INNER':
            for left_row in left_results:
                for right_row in right_results:
                    # 创建一个合并的行，为列名添加表别名前缀以避免冲突
                    combined_row = {}
                    # 为左表的每一列添加别名前缀
                    for col_name, value in left_row.items():
                        prefixed_name = f"{left_plan.get('alias', left_plan['table_name'])}.{col_name}"
                        combined_row[prefixed_name] = value
                    # 为右表的每一列添加别名前缀
                    for col_name, value in right_row.items():
                        prefixed_name = f"{right_plan.get('alias', right_plan['table_name'])}.{col_name}"
                        combined_row[prefixed_name] = value
                    # 评估连接条件
                    if _evaluate_condition(combined_row, join_condition):
                        joined_results.append(combined_row)
        elif join_type == 'LEFT':
            for left_row in left_results:
                match_found = False
                for right_row in right_results:
                    # 创建一个合并的行，为列名添加表别名前缀以避免冲突
                    combined_row = {}
                    # 为左表的每一列添加别名前缀
                    for col_name, value in left_row.items():
                        prefixed_name = f"{left_plan.get('alias', left_plan['table_name'])}.{col_name}"
                        combined_row[prefixed_name] = value
                    # 为右表的每一列添加别名前缀
                    for col_name, value in right_row.items():
                        prefixed_name = f"{right_plan.get('alias', right_plan['table_name'])}.{col_name}"
                        combined_row[prefixed_name] = value
                    # 评估连接条件
                    if _evaluate_condition(combined_row, join_condition):
                        joined_results.append(combined_row)
                        match_found = True
                if not match_found:
                    # 左连接：左表行保留，右表列填充 NULL
                    # 为右表的每一列生成 NULL 值，并添加前缀
                    for col_name in right_results[0].keys() if right_results else []:
                        prefixed_name = f"{right_plan.get('alias', right_plan['table_name'])}.{col_name}"
                        combined_row[prefixed_name] = None
                    # 左表的列已经添加了前缀，在上面的循环中
                    joined_results.append(combined_row)
        else:
            raise Exception(f"Unsupported join type: {join_type}")

        return joined_results