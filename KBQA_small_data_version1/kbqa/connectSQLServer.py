# -*- coding: utf-8 -*-
"""
Created on Mon Feb 27 10:26:39 2017

@author: 390645
"""
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import pymssql
class connectSQL():
    def __init__(self,host,user,password,database):
        self.host=host
        self.user=user
        self.password=password
        self.database=database
        self.engine = create_engine("mssql+pymssql://" + self.user + ":" + self.password + "@" + self.host + "/" + self.database + "?charset=utf8")
    def Getconnet(self):
        self.conn=pymssql.connect(host=self.host,user=self.user,password=self.password,database=self.database)
        cur=self.conn.cursor()
        if not cur:
            raise (NameError,"连接数据库失败")
        else:
            return cur
    def Query(self,sql):
        connection=self.engine.connect()
        result=pd.read_sql_query(sql,connection)
        connection.close()  #关闭
        return result
    def update(self,sql):
        # try:
        pd.read_sql_query(sql,self.engine)
        # except:
        #     print("auorcecodecloseerror")
    def ExecNonquery(self,sql):
        cur=self.Getconnet()
        cur.execute(sql)
        self.conn.commit()
        self.conn.close()

    def pd_sql(self,data_frame):
        #利用pd的dataframe
        data_frame.to_sql("intelligent_qa1",self.engine,if_exists="append")


def main():
    # data_frame = pd.read_excel("E:\chenmingwei\KBQA_small_data_version1\intelligent_qa_version1\data\second_ways.xlsx")
    dataSQL=connectSQL(host,user,password,database)

    sql ="select * from [chentian].[dbo].[member_infor]"
    data=dataSQL.ExecNonquery(sql)
    print(data,"222222222")
    # dataSQL.update(sql)
if __name__=="__main__":
    host = '172.28.171.56'
    user = 'sa'
    password = 'chentian184616_'
    database = 'chentian'

    main()


