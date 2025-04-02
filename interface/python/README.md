# IntarkDB Python DB API

#### 一、运行环境配置

如果数据库动态库已经安装到系统下，该步骤可以忽略。如果未安装，则需要配置LD_LIBRARY_PATH，指定数据库动态库文件地址：

```shell
export LD_LIBRARY_PATH=~/openGauss-embedded/output/debug/lib:$LD_LIBRARY_PATH
export LD_LIBRARY_PATH=~/openGauss-embedded/output/release/lib:$LD_LIBRARY_PATH
```

如果不想每次打开终端执行该命令，则可以将该命令添加到~/.bashrc或者/etc/profile文件中，例如：

```shell
# 打开 ~/.bashrc 文件
sudo vim ~/.bashrc

# 复制下面语句，插入到 ~/.bashrc 文件的最后一行，保存并退出
export LD_LIBRARY_PATH=~/openGauss-embedded/output/debug/lib:$LD_LIBRARY_PATH
export LD_LIBRARY_PATH=~/openGauss-embedded/output/release/lib:$LD_LIBRARY_PATH

# 激活 ~/.bashrc 文件
source ~/.bashrc
```

#### 二、SQL 命令接口使用说明

1. 导入 intarkdb 模块

```shell
# 可使用 pip 安装 intarkdb 的安装包
from intarkdb import intarkdb
```

2. 创建连接对象

```shell
# 1.0.0 版本支持连接本地数据库，1.1.0 版本支持连接远程数据库
conn = intarkdb.connect()
```

3. 创建游标对象

```shell
c = conn.cursor()
```

4. 调用 execute 方法执行 SQL 语句

```shell
# DROP
sql_str1 = "DROP TABLE IF EXISTS TEST_TABLE;"
c.execute(sql_str1)

# CREATE
sql_str2 = "CREATE TABLE TEST_TABLE (ID INTEGER, NAME VARCHAR(20), AGE FLOAT, GENDER BOOLEAN, PRIMARY KEY (ID));"
c.execute(sql_str2)

# INSERT
sql_str3 = "INSERT INTO TEST_TABLE VALUES (1, 'TOM', 23.0, True), (2, 'AMY', 25.0, False);"
c.execute(sql_str3)

# SELECT
sql_str4 = "SELECT ID,NAME,AGE,GENDER FROM TEST_TABLE;"
c.execute(sql_str4)

# UPDATE
sql_str5 = "UPDATE TEST_TABLE SET AGE = 30 WHERE ID = 1;"
c.execute(sql_str5)

# DELETE
sql_str6 = "DELETE FROM TEST_TABLE WHERE ID = 1;"
c.execute(sql_str6)
```

5. 调用 fetch* 方法查询

```shell
# 执行 SELETE 语句, 使得 rownumner 归零
sql_str4 = "SELECT ID,NAME,AGE,GENDER FROM TEST_TABLE;"
c.execute(sql_str4)

# 取查询结果的一行
result = c.fetchone()
print("fetchone:",result)

# 取查询结果的多行
result2 = c.fetchmany(2)
print("fetchmany:",result2)

# 取查询结果的剩余所有行
result3 = c.fetchall()
print("fetchall",result3)
```

6. 关闭游标

```shell
c.close()
```

7. 关闭连接

```shell
# 关闭连接后，当连接数量为0时，关闭数据库
conn.close()
```

#### 三、ORM 使用说明

1. 导入 sqlalchemy 与 intarkdb_orm 模块

```shell
# 可使用 pip 安装 sqlalchemy 的安装包
from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.orm import sessionmaker, declarative_base
# 1.2.0 版本支持 ORM
from intarkdb import intarkdb_orm
```

2.  启动 intarkdb_orm

```shell
# 注册 IntarkDB 方言，启动 IntarkDB ORM
intarkdb_orm.register()
```

3.  创建数据库引擎与基类

```shell
# 创建数据库引擎，1.2.0 版本仅支持通过ORM访问本地数据库
engine = create_engine("IntarkDB:///intarkdb")

# 创建基类
Base = declarative_base()
```

4.  定义模型

```shell
# 定义 User 模型
class User(Base):
    __tablename__ = 'users'
    __mapper_args__ = {"confirm_deleted_rows": False}

    id = Column(Integer, primary_key=True)
    name = Column(String(255), nullable=False)
    age = Column(Integer)

    def __repr__(self):
        return f"<User(name='{self.name}', age={self.age})>"
```

5.  创建数据库表与会话

```shell
# 创建数据库表
Base.metadata.create_all(engine)

# 创建会话
Session = sessionmaker(bind=engine)
```

6.  CRUD 操作

```shell
# IntarkDB 事务自动提交
with Session() as session:
    # 创建新用户
    new_user = User(id = 1, name='Michael', age=26)
    try:
        session.add(new_user)
    except:
        session.rollback()

    # 查询用户
    users = session.query(User).all()
    print("users:", users)

    # 更新用户
    try:
        session.query(User).filter_by(name='Michael').update({"age": 27})
    except:
        session.rollback()

    # 查询用户
    users = session.query(User).all()
    print("users:", users)

    # 删除用户
    try:
        session.query(User).filter_by(name='Michael').delete()
    except:
        session.rollback()

    # 查询用户
    users = session.query(User).all()
    print("users:", users)
```

#### 四、KV 命令接口使用说明
1. 导入 intarkdb 模块

```shell
from intarkdb import intarkdb
```

2. 创建 KV 连接对象

```shell
# 1.3.0 版本支持连接 KV 数据库
conn = intarkdb.connect_kv()
```

3. 创建游标对象

```shell
cur = conn.cursor()
```

4. 执行 KV 命令语句

```shell
# SET
key = "key1"
value = "value1"
cur.set(key, value)

# GET
get_key = "key1"
get_value = cur.get(get_key)
print("get_value:", get_value)

# DELETE
delete_key = "key1"
cur.delete(delete_key)
delete_value = cur.get(delete_key)
print("delete_value:", delete_value)
```

5. 关闭游标

```shell
cur.close()
```

6. 关闭连接

```shell
# 关闭 KV 连接后，当连接数量为0时，关闭数据库
conn.close_kv()
```
