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

-- 连接查询
SELECT s.name, c.cname FROM students s JOIN courses c ON s.id = c.cid;
