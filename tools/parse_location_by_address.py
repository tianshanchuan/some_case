#!/usr/local/bin/python3
# -*- coding: utf-8 -*-

import pymysql
import cpca
import datetime
import sys
from multiprocessing import Pool
#import com.medlinker.tool.configure as con
sys.path.append('/home/med/www/med-data-tool/')
import com.medlinker.tool.configure as con
from DBUtils.PersistentDB import PersistentDB

# db3 user
POOL = PersistentDB(
    creator=pymysql,  # 使用链接数据库的模块
    maxusage=None,    # 一个链接最多被重复使用的次数，None表示无限制
    setsession=[],    # 开始会话前执行的命令列表。如：["set datestyle to ...", "set time zone ..."]
    ping=0,
    # ping MySQL服务端，检查是否服务可用。# 如：0 = None = never, 1 = default = whenever it is requested, 2 = when a cursor is created, 4 = when a query is executed, 7 = always
    closeable=False,
    # 如果为False时， conn.close() 实际上被忽略，供下次使用，再线程关闭时，才会自动关闭链接。如果为True时， conn.close()则关闭链接，那么再次调用pool.connection时就会报错，因为已经真的关闭了连接（pool.steady_connection()可以获取一个新的链接）
    threadlocal=None,  # 本线程独享值得对象，用于保存链接对象，如果链接对象被重置

    host = con.get_config('db', 'host'),    #'218.244.141.205'
    port = con.get_config('db', 'port'),
    user = con.get_config('db', 'user'),
    passwd = con.get_config('db', 'passwd'),
    charset='utf8'
)

def cpca_parse_address(data):
    '''把传入的收货地址解析到省市区后组成新列表返回
    
    :param  data: (12314, "浙江省杭州市下城区青云街40号3楼")
    :rturn: example [12314, "浙江省杭州市下城区青云街40号3楼","浙江省","杭州市","下城区"]
    :rtype: list 
    '''
    data = list(data)
    address = data[-1:]
    
    df = cpca.transform(address, cut=False, lookahead=3, open_warning=False)

    province = df['省'][0]
    city = df['市'][0]
    area = df['区'][0]

    data += [province, city, area]

    return data

def get_last_n_datetime(n=-1):
    now = datetime.datetime.now()
    delta = datetime.timedelta(days=n)
    n_days = now + delta
    last_day_time = n_days.strftime('%Y-%m-%d 00:00:00')

    return last_day_time

def get_dw_address():
    ''' 从订单宽表取出新电商系统有收货地址，地址ID=0 的记录
    :rtype: tuple
    :rturn: example: ((113512, '南京市浦口区华航车辆城'))
    '''
    
    conn = POOL.connection(shareable=False)
    cursor = conn.cursor()
    
    
    last_day_time = get_last_n_datetime()
    
    sql = '''
        SELECT a.drug_order_medication_id, a.delivery_address 
        from 
            (
            select
                drug_order_medication_id,
                delivery_address
            from 
                med_dw_drug.tf_drug_order_drug_detail
            where 
                is_new_mall=1 and delivery_areaid=0 and delivery_address is not null
            )as a
        left join 
            med_DRReport.ods_drug_order_address_id as b
            on a.drug_order_medication_id=b.drug_order_id
        where 
            b.drug_order_id is null
        '''
    
#     sql_bak = '''
#         SELECT drug_order_medication_id, delivery_address 
#         from med_dw_drug.tf_drug_order_drug_detail 
#         where is_new_mall=1 and delivery_areaid=0 and delivery_address is not null
#         and create_time >= %s
#         '''
    cursor.execute(sql)
    ret = cursor.fetchall()

    cursor.close()
    conn.close()
    
    return ret
    
def update_to_ods_drug_order_address_id():
    '''把解析结果写回数据库，drug_order_id作为主键，有则更新无则新增
    
    '''
    ret = get_dw_address()
    with Pool(5) as p:
        ret_many = p.map(cpca_parse_address, ret)
    

    conn = POOL_1.connection(shareable=False)
    
    cur = conn.cursor()
    sql = '''
    insert into med_DRReport.ods_drug_order_address_id(drug_order_id, delivery_address, province, city, area)
    values(%s, %s, %s, %s, %s)
    ON DUPLICATE KEY update drug_order_id=values(drug_order_id)
    '''
    
    cur.executemany(sql, ret_many)
    conn.commit()
    
    cur.close()
    conn.close()



#if __name__ == '__main__':
update_to_ods_drug_order_address_id()