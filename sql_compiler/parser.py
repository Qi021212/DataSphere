# SQL编译器 - 语法分析器

# sql_compiler/parser.py
from typing import List, Dict, Any, Optional
from sql_compiler.lexer import Token, TokenType
from utils.constants import KEYWORDS


class ASTNode:
    def __init__(self, node_type: str, children: List[Any] = None, value: Any = None):
        self.node_type = node_type
        self.children = children if children is not None else []
        self.value = value

    def __repr__(self):
        if self.value is not None:
            return f"{self.node_type}({self.value})"
        return f"{self.node_type}{self.children}"


class Parser:
    def __init__(self):
        self.tokens = []
        self.current_pos = 0
        self.current_token = None

    def parse(self, tokens: List[Token]) -> ASTNode:
        self.tokens = tokens
        self.current_pos = 0
        self.current_token = self.tokens[0] if self.tokens else None
        statements = []
        while self.current_token is not None:
            if self.current_token.token_type == TokenType.KEYWORD:
                if self.current_token.lexeme == 'CREATE':
                    statements.append(self.parse_create_table())
                elif self.current_token.lexeme == 'INSERT':
                    statements.append(self.parse_insert())
                elif self.current_token.lexeme == 'SELECT':
                    statements.append(self.parse_select())
                elif self.current_token.lexeme == 'DELETE':
                    statements.append(self.parse_delete())
                elif self.current_token.lexeme == 'UPDATE':  # 👈 新增
                    statements.append(self.parse_update())
                else:
                    self.error(f"Unexpected keyword: {self.current_token.lexeme}")
            else:
                self.error(f"Unexpected token: {self.current_token.lexeme}")
            # 检查分号
            if self.current_token and self.current_token.lexeme == ';':
                self.advance()
        if len(statements) == 1:
            return statements[0]
        return ASTNode('MultipleStatements', statements)

    def advance(self):
        self.current_pos += 1
        if self.current_pos < len(self.tokens):
            self.current_token = self.tokens[self.current_pos]
        else:
            self.current_token = None

    def expect(self, expected_type: TokenType = None, expected_lexeme: str = None):
        if (expected_type and self.current_token.token_type != expected_type) or \
                (expected_lexeme and self.current_token.lexeme != expected_lexeme):
            self.error(f"Expected {expected_type.name if expected_type else expected_lexeme}, "
                       f"got {self.current_token.token_type.name}: {self.current_token.lexeme}")

        token = self.current_token
        self.advance()
        return token

    def error(self, message: str):
        if self.current_token:
            line, column = self.current_token.line, self.current_token.column
            raise Exception(f"Syntax error at line {line}, column {column}: {message}")
        else:
            raise Exception(f"Syntax error: {message}")

    def parse_create_table(self) -> ASTNode:
        self.expect(TokenType.KEYWORD, 'CREATE')
        self.expect(TokenType.KEYWORD, 'TABLE')
        table_name = self.expect(TokenType.IDENTIFIER).lexeme
        self.expect(TokenType.DELIMITER, '(')
        columns = []
        constraints = []  # 👈 新增

        while self.current_token and self.current_token.lexeme != ")":
            # 👇 新增：解析 FOREIGN KEY 约束
            if (self.current_token.token_type == TokenType.KEYWORD and
                    self.current_token.lexeme == "FOREIGN"):
                self.advance()  # FOREIGN
                self.expect(TokenType.KEYWORD, "KEY")
                self.expect(TokenType.DELIMITER, "(")
                fk_col = self.expect(TokenType.IDENTIFIER).lexeme
                self.expect(TokenType.DELIMITER, ")")
                self.expect(TokenType.KEYWORD, "REFERENCES")
                ref_table = self.expect(TokenType.IDENTIFIER).lexeme
                self.expect(TokenType.DELIMITER, "(")
                ref_col = self.expect(TokenType.IDENTIFIER).lexeme
                self.expect(TokenType.DELIMITER, ")")
                constraints.append(('FOREIGN_KEY', fk_col, ref_table, ref_col))  # 存储约束
            else:
                # 解析普通列
                col_name = self.expect(TokenType.IDENTIFIER).lexeme
                col_type = self.expect(TokenType.KEYWORD).lexeme
                columns.append({'name': col_name, 'type': col_type})

            if self.current_token and self.current_token.lexeme == ",":
                self.advance()

        self.expect(TokenType.DELIMITER, ")")
        # 👇 修改：在AST中包含约束
        return ASTNode('CreateTable', [
            ASTNode('TableName', value=table_name),
            ASTNode('Columns', [ASTNode('Column', value=col) for col in columns]),
            ASTNode('Constraints',
                    [ASTNode('Constraint', value=con) for con in constraints]) if constraints else ASTNode(
                'NoConstraints')
        ])

    def parse_insert(self) -> ASTNode:
        self.expect(TokenType.KEYWORD, 'INSERT')
        self.expect(TokenType.KEYWORD, 'INTO')

        table_name = self.expect(TokenType.IDENTIFIER).lexeme

        # 解析列名（可选）
        column_names = []
        if self.current_token and self.current_token.lexeme == '(':
            self.advance()
            while self.current_token and self.current_token.lexeme != ')':
                column_names.append(self.expect(TokenType.IDENTIFIER).lexeme)
                if self.current_token and self.current_token.lexeme == ',':
                    self.advance()
            self.expect(TokenType.DELIMITER, ')')

        self.expect(TokenType.KEYWORD, 'VALUES')
        self.expect(TokenType.DELIMITER, '(')

        values = []
        while self.current_token and self.current_token.lexeme != ')':
            if self.current_token.token_type == TokenType.CONSTANT:
                values.append(self.parse_constant())
            elif self.current_token.token_type == TokenType.IDENTIFIER:
                values.append(ASTNode('Identifier', value=self.current_token.lexeme))
                self.advance()
            else:
                self.error(f"Unexpected token in VALUES: {self.current_token.lexeme}")

            if self.current_token and self.current_token.lexeme == ',':
                self.advance()

        self.expect(TokenType.DELIMITER, ')')

        return ASTNode('Insert', [
            ASTNode('TableName', value=table_name),
            ASTNode('ColumnNames', [ASTNode('ColumnName', value=name) for name in column_names]),
            ASTNode('Values', values)
        ])

    def parse_select(self) -> ASTNode:

        self.expect(TokenType.KEYWORD, 'SELECT')

        # 解析选择的列
        columns = []
        while self.current_token and self.current_token.lexeme != 'FROM':
            # 处理聚合函数
            if (self.current_token.token_type == TokenType.KEYWORD and
                    self.current_token.lexeme in ['COUNT', 'SUM', 'AVG']):
                agg_func = self.current_token.lexeme
                self.advance()  # 消费 'COUNT', 'SUM', 或 'AVG'
                self.expect(TokenType.DELIMITER, '(')

                if self.current_token.lexeme == '*':
                    if agg_func != 'COUNT':
                        self.error(f"Aggregate function '{agg_func}' does not support '*'")
                    self.advance()  # 消费 '*'
                    param = ASTNode('AllColumns')
                else:
                    param = self.parse_expression()

                self.expect(TokenType.DELIMITER, ')')

                agg_node = ASTNode('AggregateFunction', [
                    ASTNode('FunctionName', value=agg_func),
                    ASTNode('Parameter', [param])
                ])
                columns.append(agg_node)
            elif self.current_token.lexeme == '*':
                columns.append(ASTNode('AllColumns'))
                self.advance()
            else:
                column_expr = self.parse_expression()
                columns.append(ASTNode('ColumnName', value=column_expr.value))

            # 在每次处理完一个列后，检查下一个 token 是否是逗号
            if self.current_token and self.current_token.lexeme == ',':
                self.advance()  # 消费逗号，继续循环
            else:
                break  # 如果不是逗号，退出循环，准备解析 'FROM'

        self.expect(TokenType.KEYWORD, 'FROM')
        table_source = self.parse_table_source()

        condition = None
        if self.current_token and self.current_token.lexeme == 'WHERE':
            self.advance()
            condition = self.parse_condition()

        return ASTNode('Select', [
            ASTNode('Columns', columns),
            ASTNode('TableSource', [table_source]),
            ASTNode('Condition', [condition]) if condition else ASTNode('NoCondition')
        ])

    # 👇 新增方法：解析表源 (单表或 JOIN)
    def parse_table_source(self) -> ASTNode:
        """解析表源，支持别名和 JOIN"""
        # 解析基础表名
        table_name = self.expect(TokenType.IDENTIFIER).lexeme

        # 解析可选的表别名
        alias = None
        if (self.current_token and
                self.current_token.token_type == TokenType.IDENTIFIER and
                # 简单启发式：如果下一个 token 不是保留关键字或分隔符，则认为是别名
                self.current_token.lexeme not in KEYWORDS and
                self.current_token.lexeme not in [',', 'WHERE', 'JOIN', 'INNER', 'LEFT', 'RIGHT', 'ON']):
            alias = self.current_token.lexeme
            self.advance()

        # 创建基础表节点
        table_node = ASTNode('Table', [
            ASTNode('TableName', value=table_name)
        ])
        if alias:
            table_node.children.append(ASTNode('Alias', value=alias))

        # 检查是否有 JOIN 关键字
        if (self.current_token and
                self.current_token.token_type == TokenType.KEYWORD and
                self.current_token.lexeme in ['INNER', 'LEFT', 'RIGHT', 'JOIN']):

            # 解析连接类型
            join_type = 'INNER'  # 默认是 INNER JOIN
            if self.current_token.lexeme in ['INNER', 'LEFT', 'RIGHT']:
                join_type = self.current_token.lexeme
                self.advance()
                # 期望接下来是 'JOIN'
                self.expect(TokenType.KEYWORD, 'JOIN')
            else:  # 只有 'JOIN'
                self.advance()  # 消费 'JOIN'

            # 解析右表
            right_table = self.parse_table_source()  # 递归调用

            # 期望 'ON'
            self.expect(TokenType.KEYWORD, 'ON')
            # 解析连接条件
            join_condition = self.parse_condition()

            # 返回一个 JOIN 节点
            return ASTNode('Join', [
                ASTNode('JoinType', value=join_type),
                ASTNode('LeftTable', [table_node]),
                ASTNode('RightTable', [right_table]),
                ASTNode('JoinCondition', [join_condition])
            ])

        # 如果没有 JOIN，返回单表节点
        return table_node

    def parse_delete(self) -> ASTNode:
        self.expect(TokenType.KEYWORD, 'DELETE')
        self.expect(TokenType.KEYWORD, 'FROM')

        table_name = self.expect(TokenType.IDENTIFIER).lexeme

        # 解析WHERE条件（可选）
        condition = None
        if self.current_token and self.current_token.lexeme == 'WHERE':
            self.advance()
            condition = self.parse_condition()

        return ASTNode('Delete', [
            ASTNode('TableName', value=table_name),
            ASTNode('Condition', [condition]) if condition else ASTNode('NoCondition')
        ])

    def parse_update(self) -> ASTNode:  # 👈 新增方法
        self.expect(TokenType.KEYWORD, 'UPDATE')
        table_name = self.expect(TokenType.IDENTIFIER).lexeme
        self.expect(TokenType.KEYWORD, 'SET')
        set_clause = []
        while True:
            col_name = self.expect(TokenType.IDENTIFIER).lexeme
            self.expect(TokenType.OPERATOR, '=')
            if self.current_token.token_type == TokenType.CONSTANT:
                value_ast = self.parse_constant()
                set_clause.append(ASTNode('Assignment', [
                    ASTNode('Column', value=col_name),
                    value_ast
                ]))
            else:
                self.error(f"Expected constant value, got {self.current_token.lexeme}")
            if self.current_token and self.current_token.lexeme == ',':
                self.advance()
            else:
                break
        condition = None
        if self.current_token and self.current_token.lexeme == 'WHERE':
            self.advance()
            condition = self.parse_condition()
        return ASTNode('Update', [
            ASTNode('TableName', value=table_name),
            ASTNode('SetClause', set_clause),
            ASTNode('Condition', [condition]) if condition else ASTNode('NoCondition')
        ])

    def parse_condition(self) -> ASTNode:
        left = self.parse_expression()  # 👈 修改：使用 parse_expression
        operator = self.expect(TokenType.OPERATOR).lexeme
        right = self.parse_expression()  # 👈 修改：使用 parse_expression

        return ASTNode('Condition', [
            ASTNode('Left', [left]),
            ASTNode('Operator', value=operator),
            ASTNode('Right', [right])
        ])

    # sql_compiler/parser.py

    def parse_expression(self) -> ASTNode:
        # 首先解析一个标识符或常量
        if self.current_token.token_type == TokenType.IDENTIFIER:
            # 解析标识符
            identifier_value = self.current_token.lexeme
            self.advance()  # 👈 关键：消费掉这个标识符 token

            # 检查下一个 token 是否是 '.' (用于处理 table.column)
            if self.current_token and self.current_token.lexeme == '.':
                self.advance()  # 消费 '.'
                if self.current_token and self.current_token.token_type == TokenType.IDENTIFIER:
                    column_name = self.current_token.lexeme
                    self.advance()  # 👈 关键：消费掉列名
                    return ASTNode('ColumnRef', value=f"{identifier_value}.{column_name}")
                else:
                    self.error(
                        f"Expected column name after '.', got {self.current_token.lexeme if self.current_token else 'EOF'}")
            else:
                # 如果不是 '.'，则返回普通的标识符节点
                return ASTNode('Identifier', value=identifier_value)

        elif self.current_token.token_type == TokenType.CONSTANT:
            # 解析常量
            return self.parse_constant()  # parse_constant 内部会调用 self.advance()

        else:
            self.error(f"Unexpected token in expression: {self.current_token.lexeme if self.current_token else 'EOF'}")

    def parse_constant(self) -> ASTNode:
        value = self.current_token.lexeme
        self.advance()

        # 解析字符串常量
        if value.startswith("'") and value.endswith("'"):
            return ASTNode('StringConstant', value=value[1:-1])

        # 解析数字常量
        if '.' in value:
            return ASTNode('FloatConstant', value=float(value))
        else:
            return ASTNode('IntConstant', value=int(value))