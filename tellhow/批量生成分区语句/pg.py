# -*- coding: utf-8 -*-
# @Time : 2019/11/13 13:56
# @Author : len
# @Email : ysling129@126.com
# @File : pg
# @Project : tellhow
# @description :
import psycopg2
import psycopg2.extras


class DbHandle:
    def __init__(self):
        self.link_pgsql = {
            'database': 'test',
            'user': 'spider',
                    'password': '123456',
                    'host': '127.0.0.1',
                    'port': 5432
        }
        self.link = psycopg2.connect(**self.link_pgsql)
        self.corsur = self.link.cursor(
            cursor_factory=psycopg2.extras.RealDictCursor)

    def update_db(self, data):
        """批量更新"""
        # sql = 'update grid_weather set data=new_data.data::json from (values %s) as new_data (data, grid_id, published_at_time) where grid_weather.grid_id=new_data.grid_id and published_at=published_at_time;'
        sql = 'update grid_dynamic_feature set data=new_data.data::json from (values %s) as new_data (data, id) where grid_dynamic_feature.id=new_data.id;'
        try:
            print(data)
            print(len(data))
            psycopg2.extras.execute_values(
                self.corsur, sql, data, page_size=4900)
            self.link.commit()
            print('更新成功')
            return True
        except Exception as e:
            print(e)
            print('更新失败')
            return False

    def insert_lots_of_by_many(self, df, name, columns):
        """简单实用，属于游标的对象方法"""
        # sql = f'insert into {name}(grid_id, data, published_at) values (%s, %s, %s);'
        sql = f"""insert into {name}({','.join(columns)}) values ({','.join(['%s'] * len(columns))});"""
        print(sql)
        data = df.to_numpy()
        print(data)
        self.corsur.executemany(sql, data)
        self.link.commit()

    def insert_lots_of_by_values(self, data, name, columns):
        """官方推荐，要批量操作的字段的值必须相同"""
        sql = f'insert into {name}({",".join(columns)}) values %s;'
        print(sql)
        try:
            data = data.to_numpy()
            print(data)
            print(len(data))
            psycopg2.extras.execute_values(
                self.corsur, sql, data, page_size=4900)
            self.link.commit()
            print('更新成功')
            return True
        except Exception as e:
            print(e)
            print('更新失败')
            return False

    def insert_lots_of_by_batch(self, data, name, columns):
        """性能好，速度快，属于类方法"""
        # sql = f"""insert into {name}(grid_id, data, published_at) values (%s, %s, %s);"""
        sql = f"""insert into {name}({','.join(columns)}) values ({','.join(['%s']*len(columns))});"""
        print(sql)
        try:
            data = data.to_numpy()
            psycopg2.extras.execute_batch(
                self.corsur, sql, data, page_size=4900)
            self.link.commit()
            print('更新成功')
            return True
        except Exception as e:
            print(e)
            print('更新失败')
            return False
