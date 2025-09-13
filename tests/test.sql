-- 测试 CREATE
CREATE TABLE student(id INT, name VARCHAR, age INT);
CREATE TABLE class(sid INT, name VARCHAR);

-- 测试 INSERT（补充多行，便于分组）
INSERT INTO student(id, name, age) VALUES (1, 'Alice', 20);
INSERT INTO student(id, name, age) VALUES (2, 'Bob',   21);
INSERT INTO student(id, name, age) VALUES (3, 'Cindy', 20);

INSERT INTO class(sid, name) VALUES (1, 'CS101');
INSERT INTO class(sid, name) VALUES (2, 'CS102');
INSERT INTO class(sid, name) VALUES (3, 'CS101');

-- 测试 SELECT
SELECT id, name FROM student WHERE age > 18 ORDER BY name;

-- 测试 DELETE
DELETE FROM student WHERE id = 1;

-- 测试 UPDATE
UPDATE student SET age = 21, name = 'Bob' WHERE id = 3;

-- 测试 JOIN
SELECT s.id, s.name FROM student s JOIN class c ON s.id = c.sid;

-- 测试 GROUP BY（示例1：单表分组，按 student.age）
SELECT age FROM student GROUP BY age;

-- 测试 GROUP BY（示例2：JOIN 后分组，仍按未限定列 age）
-- 注意：你的文法只允许 GROUP BY IDENTIFIER，因此这里使用未限定的 age
SELECT s.age FROM student s JOIN class c ON s.id = c.sid GROUP BY age;

-- order by
SELECT id, name FROM student ORDER BY name ASC;
SELECT id, age FROM student WHERE age > 18 ORDER BY age DESC;

-- 可显示把各自表的条件下推到各自 SeqScan 的例子
SELECT s.id, s.name
FROM student s JOIN class c ON s.id = c.sid
WHERE s.age > 18 AND c.name = 'CS101';



-- 智能纠错 --
-- 1) JOIN 后缺少 ON（会提示“在 JOIN 之后应有 ON ... 条件，是否缺少连接条件？”）
SELECT s.id FROM student s JOIN class c;

-- 2) JOIN 后用 WHERE 了（还是缺 ON，提示同上 + 可能提示 ON 后要跟条件）
SELECT * FROM a JOIN b WHERE a.id = b.id;

-- 3) ON 后缺布尔条件（会提示“ON / WHERE 后应跟布尔条件，例如 a.id = b.sid 或 age > 18”）
SELECT * FROM a JOIN b ON ;

-- 4) WHERE 后缺布尔条件（同样会提示“ON / WHERE 后应跟布尔条件 …”）
SELECT * FROM users WHERE ;

-- 5) WHERE 后直接 GROUP（仍会提示 WHERE 后应跟条件）
SELECT * FROM users WHERE GROUP BY id;

-- 6) ORDER BY 后缺列（会提示“ORDER BY / GROUP BY 后应跟列名 …”）
SELECT id FROM users ORDER BY ;

-- 7) GROUP BY 后缺列（同上提示）
SELECT id FROM users GROUP BY ;

-- 8) SELECT 列表缺少列（会提示“是否缺少选择列表？你可以写具体列名或使用 * …”）
SELECT FROM users;

-- 9) 语句末尾缺分号（会提示“语句末尾需要分号 ';'”）
SELECT * FROM users

-- 10) 组合错误：JOIN 缺 ON 且 WHERE 缺条件（双重命中，上面两个提示都可能出现）
SELECT s.id, s.name FROM student s JOIN class c WHERE ;

-- 正确
SELECT s.id, s.name FROM student s JOIN class c ON s.id = c.sid WHERE s.age > 18;

-- 故意漏 ON，触发 JOIN 智能提示
SELECT s.id, s.name FROM student s JOIN class c WHERE s.age > 18;


-- 谓词下推 --

-- 例子1：单表条件 + 多表 JOIN
SELECT s.id, c.name
FROM student s
JOIN class c ON s.id = c.sid
WHERE s.age > 18 AND c.name = 'CS101';

-- 例子2：混合条件，部分可下推，部分残余
SELECT s.id, c.name
FROM student s
JOIN class c ON s.id = c.sid
WHERE s.age > 18 AND c.name = 'CS101' AND s.id = c.sid;
