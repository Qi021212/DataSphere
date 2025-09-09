#SQL编译器 - 词法分析器

# sql_compiler/lexer.py
import re
from typing import List, Tuple, Optional
from utils.constants import TokenType, KEYWORDS, OPERATORS, DELIMITERS


class Token:
    def __init__(self, token_type: TokenType, lexeme: str, line: int, column: int):
        self.token_type = token_type
        self.lexeme = lexeme
        self.line = line
        self.column = column

    def __repr__(self):
        return f"[{self.token_type.name}, '{self.lexeme}', {self.line}, {self.column}]"


class Lexer:
    def __init__(self):
        self.tokens = []
        self.line = 1
        self.column = 1
        self.current_pos = 0
        self.input = ""

    def tokenize(self, input_text: str) -> List[Token]:
        self.input = input_text
        self.tokens = []
        self.line = 1
        self.column = 1
        self.current_pos = 0

        while self.current_pos < len(self.input):
            char = self.input[self.current_pos]

            # 跳过空白字符
            if char.isspace():
                if char == '\n':
                    self.line += 1
                    self.column = 1
                else:
                    self.column += 1
                self.current_pos += 1
                continue

            # 处理注释
            if char == '-' and self.current_pos + 1 < len(self.input) and self.input[self.current_pos + 1] == '-':
                self.skip_comment()
                continue

            # 处理字符串常量
            if char == "'":
                token = self.process_string()
                if token:
                    self.tokens.append(token)
                continue

            # 处理数字常量
            if char.isdigit():
                token = self.process_number()
                if token:
                    self.tokens.append(token)
                continue

            # 处理标识符和关键字
            if char.isalpha() or char == '_':
                token = self.process_identifier()
                if token:
                    self.tokens.append(token)
                continue

            # 处理运算符
            if char in OPERATORS:
                token = self.process_operator()
                if token:
                    self.tokens.append(token)
                continue

            # 处理分隔符
            if char in DELIMITERS:
                self.tokens.append(Token(TokenType.DELIMITER, char, self.line, self.column))
                self.column += 1
                self.current_pos += 1
                continue

            # 未知字符
            raise Exception(f"Lexical error: Unknown character '{char}' at line {self.line}, column {self.column}")

        return self.tokens

    def skip_comment(self):
        while (self.current_pos < len(self.input) and
               self.input[self.current_pos] != '\n'):
            self.current_pos += 1
            self.column += 1

    def process_string(self) -> Optional[Token]:
        start_pos = self.current_pos
        start_line = self.line
        start_column = self.column

        self.current_pos += 1  # 跳过开始的引号
        self.column += 1

        string_content = []
        while (self.current_pos < len(self.input) and
               self.input[self.current_pos] != "'"):
            if self.input[self.current_pos] == '\n':
                self.line += 1
                self.column = 1
            else:
                self.column += 1
            string_content.append(self.input[self.current_pos])
            self.current_pos += 1

        if self.current_pos >= len(self.input):
            raise Exception(f"Lexical error: Unclosed string at line {start_line}, column {start_column}")

        # 跳过结束的引号
        self.current_pos += 1
        self.column += 1

        lexeme = "'" + ''.join(string_content) + "'"
        return Token(TokenType.CONSTANT, lexeme, start_line, start_column)

    def process_number(self) -> Optional[Token]:
        start_pos = self.current_pos
        start_line = self.line
        start_column = self.column

        number_content = []
        while (self.current_pos < len(self.input) and
               (self.input[self.current_pos].isdigit() or self.input[self.current_pos] == '.')):
            number_content.append(self.input[self.current_pos])
            self.current_pos += 1
            self.column += 1

        lexeme = ''.join(number_content)
        return Token(TokenType.CONSTANT, lexeme, start_line, start_column)

    def process_identifier(self) -> Optional[Token]:
        start_pos = self.current_pos
        start_line = self.line
        start_column = self.column

        identifier_content = []
        while (self.current_pos < len(self.input) and
               (self.input[self.current_pos].isalnum() or self.input[self.current_pos] == '_')):
            identifier_content.append(self.input[self.current_pos])
            self.current_pos += 1
            self.column += 1

        lexeme = ''.join(identifier_content)
        if lexeme.upper() in KEYWORDS:
            return Token(TokenType.KEYWORD, lexeme.upper(), start_line, start_column)
        else:
            return Token(TokenType.IDENTIFIER, lexeme, start_line, start_column)

    def process_operator(self) -> Optional[Token]:
        start_pos = self.current_pos
        start_line = self.line
        start_column = self.column

        operator_content = [self.input[self.current_pos]]
        self.current_pos += 1
        self.column += 1

        # 处理多字符运算符
        if (self.current_pos < len(self.input) and
                operator_content[0] + self.input[self.current_pos] in OPERATORS):
            operator_content.append(self.input[self.current_pos])
            self.current_pos += 1
            self.column += 1

        lexeme = ''.join(operator_content)
        return Token(TokenType.OPERATOR, lexeme, start_line, start_column)