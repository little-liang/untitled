#coding=utf-8
from sqlalchemy import Column, Integer, String, Text, DateTime, VARCHAR, CHAR
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
import sqlalchemy

import os
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'


'''
How do I map a table that has no primary key?
http://docs.sqlalchemy.org/en/rel_1_0/faq/ormconfiguration.html#how-do-i-map-a-table-that-has-no-primary-key
'''
# 创建对象的基类:
Base = declarative_base()

# class structured_news(Base):
#
#     # 表名
#     __tablename__ = 'p_news'
#
#     # 表结构
#     URL = Column(String)
#     name = Column(String)
#     title = Column(Text)
#     content = Column(Text)
#     publish_time = Column(String)
#     catagory = Column(String)
#     keywords = Column(String)
#     separate = Column(Text)
#     html = Column(Text)
#     id = Column(Integer, primary_key=True)



class BBERRY(Base):
    # 表名
    __tablename__ = 'CUST_IDENTIFI_INTERFACE'

    # 表结构
    data_source = Column(VARCHAR(30), primary_key=True)
    data_source_name = Column(VARCHAR(12), primary_key=True)
    cust_id = Column(VARCHAR(30), primary_key=True)
    cust_name = Column(VARCHAR(100), primary_key=True)
    identifi_id = Column(CHAR(1), primary_key=True)
    logic_type = Column(CHAR(8), primary_key=True)
    logic_num = Column(VARCHAR(200), primary_key=True)
    cust_mgr = Column(VARCHAR(200), primary_key=True)


# # 初始化数据库连接,:
# engine1 = create_engine('mysql://test001:test001@119.31.210.76:3306/structured_news?charset=utf8')
# # 创建DBSession类型:
# Session1 = sessionmaker(bind=engine1)
# session1 = Session1()





#
#
# # 初始化数据库连接,:
# engine2 = create_engine('postgresql://blueberry_data:lPHZR1TSCoKGyUt4@119.31.210.118:5432/blueberry_data')
# # 创建DBSession类型:
# Session2 = sessionmaker(bind=engine2)
# session2 = Session2()



############

#
# # 初始化数据库连接,:
engine2 = create_engine('postgresql://postgres:j78Uq4bcMG@119.31.210.76:5432/test')
# 创建DBSession类型:
Session2 = sessionmaker(bind=engine2)
session2 = Session2()

# 1. 创建表（如果表已经存在，则不会创建）
#绑定元信息
metadata = sqlalchemy.MetaData(engine2)

#创建表格，初始化数据库
user = sqlalchemy.Table('user', metadata,
    Column('id', Integer, primary_key = True),
    Column('name', String(20)),
    Column('fullname', String(40)))

########


# 初始化数据库连接,:
# engine3 = create_engine('oracle://bberry:bberry@10.138.22.223:1521/edw')

# engine3 = create_engine('oracle://@etl:etl@10.138.22.223:1521/edw')
# engine3 = create_engine("oracle+cx_oracle://bberry:bberry@(DESCRIPTION = (LOAD_BALANCE=on) (FAILOVER=ON) (ADDRESS = (PROTOCOL = TCP)(HOST = 10.138.22.223)(PORT = 1521)) (CONNECT_DATA = (SERVER = DEDICATED) (SERVICE_NAME = edw)))")
#
# # 创建DBSession类型:
# Session3 = sessionmaker(bind=engine3)
# session3 = Session3()

# for instance in session1.query(User).order_by(User.id):
#     print(instance.id)




# query = session3.query(BBERRY)
#
# for line in query.all():
#     print(
#     line.data_source,
#     line.data_source_name,
#     line.cust_id,
#     line.cust_name,
#     line.identifi_id,
#     line.logic_type,
#     line.logic_num,
#     line.cust_mgr
#     )
#
#     # info = structured_news(
#     #     URL=line.URL,
#     #     name=line.name,
#     #     title=line.title,
#     #     content=line.content,
#     #     publish_time=line.publish_time,
#     #     catagory=line.catagory,
#     #     keywords=line.keywords,
#     #     separate=line.separate,
#     #     html=line.html,
#     #     id=line.id
#     # )
#     #
#     # session2.add(info)
#     # session2.commit()
#     break

# meta = sqlalchemy.MetaData()
# t = sqlalchemy.Table("CUST_IDENTIFI_INTERFACE", meta, autoload=True, autoload_with=engine3)
# print(t.c.keys())

# ins = t.insert()
# print(str(ins))

# # check all the column names and do a select to fetch the data directly from table
# conn = engine3.connect()
# s = sqlalchemy.select([t]).where(t.c.id != '')  # 提供查询条件
# result = conn.execute(s)
# for row in result:
#     print(row[t.c.id], row[t.c.name])
#     # 看这里，直接用t.c.name就可以调用name列的值了，c代表column。不用做映射，不用配置文件，简单到无语吧？...
#
# # remember to close the cursor
# result.close()