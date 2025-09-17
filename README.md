> # DataSphere
>

**DataSphere** 是一个用 Python 实现的简易关系型数据库系统，作为《大型平台软件设计实习》的项目成果。该项目旨在实践数据库系统的核心概念，包括 SQL 编译（词法、语法、语义分析）、查询计划生成与优化、页式存储管理、缓冲池以及基本的执行引擎。

## 目录

1. [功能特性](#功能特性)
2. [系统架构](#系统架构)
3. [环境依赖](#环境依赖)
4. [安装指南](#安装指南)
5. [运行方式](#运行方式)
   *   [交互式命令行 (CLI)](#交互式命令行-cli)
   *   [执行 SQL 脚本文件](#执行-sql-脚本文件)
6. [支持的 SQL 语法](#支持的-sql-语法)
7. [项目结构](#项目结构)
8. [数据存储](#数据存储)
9. [示例](#示例)

## 功能特性

*   **SQL 解析**: 支持基本的词法和语法分析。
*   **语义检查**: 验证表/列是否存在、数据类型匹配等。
*   **查询计划**: 生成逻辑执行计划，并实现简单的谓词下推优化。
*   **存储管理**: 基于文件的页式存储 (`4KB` 页)。
*   **缓冲池**: 使用 LRU (Least Recently Used) /FIFO策略缓存页面，提高访问效率。
*   **基本执行引擎**: 支持 `CREATE TABLE`, `INSERT`, `SELECT`, `UPDATE` 操作。
*   **简单 JOIN**: 支持内连接 (INNER JOIN) 的逻辑计划生成。
*   **基本聚合**: 支持 `COUNT`, `SUM`, `AVG` 聚合函数。
*   **元数据管理**: 通过 `Catalog` 管理表结构信息。

## 系统架构

SimpleDB 主要由以下三个核心模块构成：

1.  **SQL 编译器 (`sql_compiler/`)**:
    *   `Lexer`: 将 SQL 字符串分解为 Token 流。
    *   `Parser`: 根据语法规则将 Token 流解析为抽象语法树 (AST)。
    *   `SemanticAnalyzer`: 检查 AST 的语义正确性（如表/列存在性、类型检查）。
    *   `Planner`: 将 AST 转换为逻辑执行计划，并进行优化（如谓词下推）。
2.  **存储系统 (`storage/`)**:
    *   `Page`: 表示数据库中的一个固定大小（4KB）的数据页。
    *   `PageManager`: 负责页面的物理读写和分配。
    *   `BufferPool`: 实现页面缓存，使用 LRU 算法管理页面。
    *   `FileManager`: 管理表文件，处理记录的序列化、插入、读取等。
3.  **数据库引擎 (`engine/`)**:
    *   `Catalog`: 管理数据库的元数据（表名、列定义等）。
    *   `Executor`: 执行逻辑计划，与存储系统交互完成数据操作。
    *   `Database`: 核心入口点，整合编译器、存储和执行引擎。
    *   `DatabaseCLI`: 提供交互式命令行界面。

## 环境依赖

*   **Python**: 3.7 或更高版本

## 安装指南

本项目为纯 Python 实现，无需额外安装第三方库（除非有扩展需求）。

1.  克隆或下载此项目代码到本地目录。
2.  确保你的系统已安装 Python 3.7 或更高版本。

```bash
# 检查 Python 版本
python --version # 或 python3 --version
```

## 运行方式

DataSphere 提供了两种主要的运行方式：

### 交互式命令行 (CLI)

启动一个交互式 SQL 命令行界面，可以逐条输入和执行 SQL 语句。

1. 打开终端或命令行工具。

2. 切换到项目根目录。

3. 运行启动脚本：

   ```bash
   python -m cli.main
   ```

4. 看到 `Welcome to DataSphere CLI` 提示后，即可输入 SQL 语句。每条语句以分号 `;` 结尾，如：

   ```sql
   SQL > CREATE TABLE users (id INT, name VARCHAR(50));
   SQL > INSERT INTO users VALUES (1, 'Alice');
   SQL > SELECT * FROM users;
   SQL > exit; -- 退出 CLI
   ```

### 执行 SQL 脚本文件

可以将多条 SQL 语句写入一个 `.sql` 文件，然后一次性执行。

1. 创建一个包含 SQL 语句的文本文件，例如 `tests/demo.sql`。

2. 在终端中运行：

   ```bash
   python cli/main.py tests/demo.sql
   # 或者在交互中：
   python cli/main.py
   ：read tests/demo.sql
   ```

## 支持的 SQL 语法

*   **数据定义语言 (DDL)**:
    *   `CREATE TABLE table_name (column1 type, column2 type, ...);`
        *   支持 `INT`, `VARCHAR(N)` 数据类型。
*   **数据操作语言 (DML)**:
    *   `INSERT INTO table_name VALUES (value1, value2, ...);`
    *   `DELETE FROM table_name [WHERE condition];`
    *   `SELECT columns FROM table_name [WHERE condition];`
        *   `columns` 可以为 `*` 或具体列名列表。
        *   `condition` 支持 `=`, `!=`, `<`, `>`, `<=`, `>=` 比较操作符，以及 `AND` 连接。
    *   `UPDATE table_name SET column1 = value1 [, column2 = value2, ...] [WHERE condition];`
        *   `condition` 支持 `=`, `!=`, `<`, `>`, `<=`, `>=` 比较操作符。
*   **查询扩展**:
    *   `SELECT ... FROM table1 JOIN table2 ON condition;` (INNER JOIN)
    *   `SELECT ... FROM table1 GROUP BY condition;` (GROUP BY)
    *   `SELECT ... FROM table1 ORDER BY condition;` (ORDER BY)
    *   `SELECT COUNT(*), SUM(column), AVG(column) FROM table_name [WHERE condition];`
*   **元命令 (CLI)**:
    *   `exit;` 或 `quit;`: 退出 CLI。

## 项目结构

```
Datasphere/
├── sql_compiler/              # SQL编译器模块
│   ├── __init__.py
│   ├── lexer.py               # 词法分析器
│   ├── parser.py              # 语法分析器
│   ├── semantic.py			   # 语义分析器
│   ├── planner.py             # 执行计划生成器
│   └── catalog.py             # 系统目录
│   └── diag.py                # 智能提示
│   └── ll1_debugger.py        # LL1调试
│   └── optimizer.py           # 谓词下推
├── storage/                   # 存储管理模块
│   ├── __init__.py
│   ├── page.py                # 数据页管理
│   ├── buffer.py              # 缓存管理（LRU/FIFO）
│   └── file_manager.py        # 文件管理器
├── engine/					   # 数据库引擎模块
│   ├── __init__.py
│   ├── executor.py            # SQL执行引擎
│   ├── storage_engine.py      # 存储引擎
│   └── catalog_manager.py     # 目录管理
├── cli/                       # 命令行界面
│   ├── __init__.py
│   └── main.py                # 主程序入口
├── tests/                     # 测试文件
│   ├── __init__.py
│   └── test_db.py             # SQL语句测试
│   └── test_buffer.py         # 缓存功能测试
├── utils/					   # 工具类
│   ├── __init__.py
│   ├── constants.py		   # 常量定义
│   └── helpers.py			   # 辅助函数
├── data/   				   # (运行时生成) 数据目录
│   ├── catalog.json		   # 系统目录文件
│   └── pages/ 				   # 数据页文件 (.dat)
└── log/ 					   # (运行时生成) 详细编译日志
```

## 数据存储

*   **数据文件**: 表数据存储在 `data/pages/` 目录下的 `.dat` 文件中，每个文件对应一个数据页。
*   **元数据文件**: 表结构等元数据存储在 `data/catalog.json` 文件中。
*   **首次运行**: 如果 `data/` 目录不存在，系统会自动创建。首次创建表时会初始化相关文件。

## 示例

```sql
-- 创建表
CREATE TABLE students (id INT, name VARCHAR(100), age INT);
CREATE TABLE courses (cid INT, cname VARCHAR(100));

-- 插入数据
INSERT INTO students VALUES (1, 'Alice', 20);
INSERT INTO students VALUES (2, 'Bob', 22);
INSERT INTO courses VALUES (101, 'Database Systems');
INSERT INTO courses VALUES (102, 'Operating Systems');

-- 查询所有学生
SELECT * FROM students;

-- 条件查询
SELECT name, age FROM students WHERE age > 20;

-- 更新数据
UPDATE students SET age = 21 WHERE name = 'Alice';

-- 简单聚合
SELECT COUNT(*) AS total_students FROM students;
SELECT AVG(age) FROM student;

-- 扩展查询
SELECT s.name, c.cname FROM students s JOIN courses c ON s.id = c.cid;
SELECT age FROM student GROUP BY age;
SELECT id, age FROM student WHERE age > 18 ORDER BY age DESC;
```

---
