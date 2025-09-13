# sql_compiler/lexer.py
import re

# === 种别码映射（与现有流水线保持一致） ===
TOKEN_TYPE_MAP = {
    'KEYWORD': 1,
    'IDENTIFIER': 2,
    'NUMBER': 3,
    'STRING': 3,   # 字符串与数字同用 3
    'OPERATOR': 4,
    'DELIMITER': 5,
    'END': 0
}


class Token:
    def __init__(self, type_, value, line, column):
        self.type = type_               # 词法类别字符串，如 'KEYWORD'
        self.value = value              # 词素内容
        self.line = line                # 行号（从1开始）
        self.column = column            # 列号（从1开始）
        self.type_code = TOKEN_TYPE_MAP.get(type_, 0)

    def __repr__(self):
        # 方便调试：与示例输出风格保持一致 [type_code, "value", line, column]
        v = self.value
        if isinstance(v, str):
            return f'[{self.type_code}, "{v}", {self.line}, {self.column}]'
        return f'[{self.type_code}, "{v}", {self.line}, {self.column}]'


class Lexer:
    """
    简单 SQL 词法分析器：
      - 关键字大小写不敏感，统一转为大写后输出
      - 标识符大小写保留原样（可按需改为统一）
      - 字符串仅支持单引号包裹，内部不处理转义（可按需扩展）
      - 数字仅支持十进制整数
    """
    # 关键字集合（统一大写）
    KEYWORDS = {
        # DDL / DML / 查询
        'SELECT', 'FROM', 'WHERE', 'CREATE', 'TABLE', 'INSERT', 'INTO',
        'VALUES', 'DELETE', 'UPDATE', 'SET', 'JOIN', 'ON',
        'ORDER', 'BY', 'GROUP', 'HAVING', 'AS',

        # 本题要求新增：
        'AND', 'OR', 'NOT', 'ASC', 'DESC',

        # 简单类型
        'INT', 'VARCHAR'
    }

    # 运算符集合（用于必要时精确匹配；正则仍以字符类为主）
    # 可扩展：<= >= <> != = > <
    OPERATORS = {
        '=', '>', '<', '>=', '<=', '<>', '!='
    }

    # 分隔符集合（用于参考/注释；真正匹配以正则为准）
    DELIMITERS = {
        '(', ')', ',', ';', '.', '*', '='  # 注意：'=' 既在 OPERATOR 中也在这里，但 OPERATOR 优先匹配
    }

    def __init__(self, text: str):
        self.text = text
        self.tokens = []
        self.errors = []
        self._tokenize()

    def _tokenize(self):
        """
        使用正则分组匹配整篇文本。为保证关键字优先于标识符，
        先匹配 IDENTIFIER，再在 Python 侧判断是否为关键字。
        """
        token_spec = [
            # 空白
            ('WHITESPACE', r'\s+'),

            # 标识符（后续再判断是否是关键字）
            ('IDENTIFIER', r'[A-Za-z_][A-Za-z0-9_]*'),

            # 数字（整数）
            ('NUMBER', r'\d+'),

            # 字符串（单引号，不处理转义）
            ('STRING', r"'[^']*'"),

            # 运算符（多字符优先）
            ('OPERATOR', r'<>|!=|>=|<=|=|>|<'),

            # 分隔符：加入 '*' 以支持 SELECT * FROM ...
            ('DELIMITER', r'[(),;.*]'),
        ]

        tok_regex = '|'.join(
            f'(?P<{name}>{pattern})' for name, pattern in token_spec
        )

        # 用 finditer 逐个匹配，大小写不敏感
        for mo in re.finditer(tok_regex, self.text, flags=re.IGNORECASE | re.MULTILINE):
            kind = mo.lastgroup
            lexeme = mo.group()
            start = mo.start()

            # 计算行/列（1-based）
            before = self.text[:start]
            line = before.count('\n') + 1
            last_newline = before.rfind('\n')
            column = (start - (last_newline + 1)) + 1

            if kind == 'WHITESPACE':
                continue

            if kind == 'IDENTIFIER':
                up = lexeme.upper()
                if up in self.KEYWORDS:
                    token = Token('KEYWORD', up, line, column)
                else:
                    token = Token('IDENTIFIER', lexeme, line, column)

            elif kind == 'NUMBER':
                token = Token('NUMBER', int(lexeme), line, column)

            elif kind == 'STRING':
                token = Token('STRING', lexeme[1:-1], line, column)

            elif kind == 'OPERATOR':
                token = Token('OPERATOR', lexeme, line, column)

            elif kind == 'DELIMITER':
                token = Token('DELIMITER', lexeme, line, column)

            else:
                # 理论不会到这里
                self.errors.append((line, column, f"Unrecognized token: {lexeme}"))
                continue

            self.tokens.append(token)

    def get_tokens(self):
        """返回 Token 列表。"""
        return self.tokens

    def get_errors(self):
        return self.errors
