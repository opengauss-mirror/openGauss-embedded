-- 创建表
DROP TABLE IF EXISTS users;
CREATE TABLE users (
    id INT PRIMARY KEY,
    name TEXT,
    age INT
);

-- 插入数据
INSERT INTO users (id, name, age) VALUES (1, 'Alice', 25);
INSERT INTO users (id, name, age) VALUES (2, 'Bob', 30);
INSERT INTO users (id, name, age) VALUES (3, 'Charlie', 22);

-- 查询所有数据
SELECT * FROM users;

-- 条件查询
SELECT name FROM users WHERE age > 24;

-- 更新数据
UPDATE users SET age = 26 WHERE id = 1;

-- 删除数据
DELETE FROM users WHERE id = 3;

-- 最终查询
SELECT * FROM users;
