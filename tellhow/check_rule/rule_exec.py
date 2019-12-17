# -*- coding: utf-8 -*-
# @Time : 2019/11/30 12:08
# @Author : ling
# @Email : ysling129@126.com
# @File : rule_exec
# @Project : tellhow
# @description :  用于生成检核结果。

from datetime import datetime
from psycopg2 import extras
import sys
import psycopg2 as pg
import pandas as pd


pg_con = pg.connect(
    database='bj_tz_ldjsc1.1',
    user='uetl',
    # password='tellhow123',
    host='10.10.10.44',
    port=5432)
chk_cfg_data = pd.read_sql(
    'select * from mgmt_etl.e_chk_cfg where not chk_discard;',
    con=pg_con)


def insert_chk_results(value: list):
    """
    向数据库中插入数据
    :param value:
    :return:
    """
    sql = f"""
    insert into mgmt_etl.e_chk_results values %s ;
    """
    pg_cur = pg_con.cursor()
    # pg_cur.execute("""DELETE FROM mgmt_etl.e_chk_results WHERE chk_db <>'e_chk_results';""")
    # pg_con.commit()
    extras.execute_values(pg_cur, sql, value, page_size=len(value))
    pg_con.commit()


def get_imp_values(tab_rule_info: pd.Series):
    """
        获取某个表的稽核结果
    :param tab_rule_info:
    :return:
    """
    values = []
    data_bat_new = pd.read_sql(f"""SELECT
tab_a.db_nm,
tab_a.data_tbl_nm,
tab_b.imp_id
FROM mgmt_etl.e_data_intfc tab_a,
mgmt_etl.e_intfc_files_imp_job tab_b
WHERE tab_a.intfc_cd=tab_b.intfc_cd
AND tab_b.job_state = 'S' EXCEPT
(SELECT distinct tab_a.chk_db, tab_a.chk_obj,tab_a.data_bat from mgmt_etl.e_chk_results tab_a
WHERE   tab_a.chk_id = {tab_rule_info.chk_id})""", con=pg_con)
    for index, row in data_bat_new[(data_bat_new.db_nm == tab_rule_info.chk_db) & (
            data_bat_new.data_tbl_nm == tab_rule_info.chk_obj)].iterrows():
        all_cont = pd.read_sql(
            f""" select * from {tab_rule_info.chk_db}.{tab_rule_info.chk_obj}
        where imp_id = '{row['imp_id']}' ; """, con=pg_con)
        begin_time = datetime.now()
        err_cont = pd.read_sql(
            f"""select * from ({tab_rule_info.chk_sql.replace(';','')}) tab
        where imp_id = '{row['imp_id']}';
        """, con=pg_con)
        end_time = datetime.now()
        value = (
            tab_rule_info.chk_id,
            tab_rule_info.rule_code,
            tab_rule_info.rule_name,
            tab_rule_info.chk_db,
            tab_rule_info.chk_obj,
            tab_rule_info.chk_obj_dec,
            tab_rule_info.chk_clo,
            tab_rule_info.chk_clo_dec,
            err_cont.shape[0],
            all_cont.shape[0],
            row['imp_id'],
            '增量',
            end_time.strftime('%Y%m%d%H'),
            err_cont.head(1000).to_json(
                force_ascii=False, orient='table', index=False),
            # err_cont.to_json(force_ascii=False,orient='table',index=False),
            begin_time,
            end_time,
            f"""对 {tab_rule_info.chk_obj_dec}({tab_rule_info.chk_obj})表 的增量进行{tab_rule_info.rule_name}检核({tab_rule_info.chk_dec})，其中错误数据量为{str(err_cont.shape[0])}，检查总量为{str(all_cont.shape[0])}，检核字段为{tab_rule_info.chk_clo}。""",
        )
        values.append(value)
    return values


def get_all_values(tab_rule_info: pd.DataFrame):
    """
    某个表按照某个规则全量检查结果
    :param tab_rule_info:
    :return:
    """
    values = []
    print(tab_rule_info.chk_db)
    for index, row in tab_rule_info.iterrows():
        all_cont = pd.read_sql(
            f""" select * from {row.chk_db}.{row.chk_obj}  ; """,
            con=pg_con)
        begin_time = datetime.now()
        err_cont = pd.read_sql(
            f"""select * from ({row.chk_sql.replace(';', '')}) tab;
                """, con=pg_con)
        end_time = datetime.now()
        value = (
            row.chk_id,
            row.rule_code,
            row.rule_name,
            row.chk_db,
            row.chk_obj,
            row.chk_obj_dec,
            row.chk_clo,
            row.chk_clo_dec,
            err_cont.shape[0],
            all_cont.shape[0],
            None,
            '全量',
            end_time.strftime('%Y%m%d%H'),
            err_cont.head(1000).to_json(
                force_ascii=False, orient='table', index=False),
            # err_cont.to_json(force_ascii=False,orient='table',index=False),
            begin_time,
            end_time,
            f"""对 {tab_rule_info.chk_obj_dec}({tab_rule_info.chk_obj})表 的全量进行{tab_rule_info.rule_name}检核({tab_rule_info.chk_dec})，其中错误数据量为{str(err_cont.shape[0])}，检查总量为{str(all_cont.shape[0])}，检核字段为{tab_rule_info.chk_clo}。""",
        )
        values.append(value)
    return values


def not_o_values(tab_rule_info: pd.Series):
    """
    用于检测非源层数据
    :param tab_rule_info:
    :return:
    """
    values = []

    all_count = pd.read_sql(
        f""" select * from {tab_rule_info.chk_db}.{tab_rule_info.chk_obj}  ; """,
        con=pg_con)
    begin_time = datetime.now()
    err_cont = pd.read_sql(
        f"""select * from ({tab_rule_info.chk_sql.replace(';', '')}) tab;
                    """, con=pg_con)
    end_time = datetime.now()
    value = (
        tab_rule_info.chk_id,
        tab_rule_info.rule_code,
        tab_rule_info.rule_name,
        tab_rule_info.chk_db,
        tab_rule_info.chk_obj,
        tab_rule_info.chk_obj_dec,
        tab_rule_info.chk_clo,
        tab_rule_info.chk_clo_dec,
        err_cont.shape[0],
        all_count.shape[0],
        None,
        '全量',
        end_time.strftime('%Y%m%d%H'),
        err_cont.head(1000).to_json(
            force_ascii=False, orient='table', index=False),
        # err_cont.to_json(force_ascii=False,orient='table',index=False),
        begin_time,
        end_time,
        f"""对 {tab_rule_info.chk_obj_dec}({tab_rule_info.chk_obj})表 的全量进行{tab_rule_info.rule_name}检核({tab_rule_info.chk_dec})，其中错误数据量为{str(err_cont.shape[0])}，检查总量为{str(all_count.shape[0])}，检核字段为{tab_rule_info.chk_clo}。""",
    )
    values.append(value)

    return values


if __name__ == '__main__':
    if len(sys.argv) != 1:
        chk_db = sys.argv[1].split('.')[0]
        chk_obj = sys.argv[1].split('.')[1]
        table_rule_info = chk_cfg_data[(chk_cfg_data.chk_obj == chk_obj) & (
            chk_cfg_data.chk_db == chk_db)]
        print(table_rule_info)
        result_values = get_all_values(table_rule_info)
        insert_chk_results(result_values)
        pg_con.close()
    else:
        for row_num, row in chk_cfg_data.iterrows():
            if row.chk_db == 'biz_origin':

                result_values = get_imp_values(row)
                if result_values:
                    insert_chk_results(result_values)
                    print(f'插入{len(result_values)} 条数据！')
                else:
                    print(f'没有新增数据')
            else:
                result_values = not_o_values(row)
                if result_values:
                    insert_chk_results(result_values)
                    print(f'插入{len(result_values)} 条数据！')
                else:
                    print(f'没有新增数据')

        pg_con.close()
