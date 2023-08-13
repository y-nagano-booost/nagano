import yfinance as yf
from pandas_datareader import data as pdr
import pandas as pd
import MySQLdb
import datetime

class MySQLConnect:
    '''
    MySQLのヘルパークラス
    #############################################################DBへの接続
       MySQLConnect()
    #############################################################１つのトランザクション内で複数SQLの実行
    #    db.execute_all(sqls)
    #############################################################指定テーブルのデータを取得
    # data = db.execute_query('select * from test')
    #############################################################Insert文の作成
    #  sqls = []
    #  for n in range(10):
    #    sqls.append("insert into test values('data{0}',{0})".format(n))
    #############################################################指定したテーブルが存在すれば、テーブルを削除
    #      db.drop('test2')
    #############################################################テーブルの新規作成
    #     db.create('test','name nvarchar(10),num numeric(5,1)','name,num',isdrop=True)
    #############################################################指定テーブルへの列追加
    # db.add_column('test',['add1 nvarchar(10)','add2 numeric(10,3)','add3 timestamp'])
    #############################################################テーブルの存在チェック
    # print(db.exists('test'))    print(db.exists('test9'))
    #############################################################テーブルとViewの一覧の取得
    # print(db.get_table_list()) 
    #############################################################View の一覧を取得
    #  print(db.get_table_list('view')) 
    #############################################################テーブルの一覧を取得
    #   print(db.get_table_list('table'))
    #############################################################指定したテーブルのカラム名の一覧を取得
    # print(db.get_column_list('test'))
    #############################################################指定したテーブルのカラム名とデータ型の取得
    # print(db.get_column_type('test'))
    #############################################################スカラ（結果が１つだけのSelect文）の実行
    #   print(db.execute_scalor("select sum(num) as num from test"))
    #############################################################データベース名の変更
    #   db.rename('test','test2')
    '''
    GET_TABLE_LIST_QUERY = "SELECT table_name,type FROM (SELECT table_name,table_schema,CASE table_type WHEN 'VIEW' THEN 'view' ELSE 'table' END AS type FROM information_schema.tables) t WHERE table_name LIKE '{0}' AND table_schema LIKE '{1}' AND type like '{2}'"
    GET_COLUMN_LIST_QUERY = "SELECT TABLE_NAME,COLUMN_NAME,DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS where TABLE_NAME like LOWER('{0}') and TABLE_SCHEMA like LOWER('{1}') ORDER BY ORDINAL_POSITION"
    GET_ALTER_TABLE_QUERY = "ALTER TABLE {0} ADD {1}"
    GET_RENAME_TABLE_QUERY = "RENAME TABLE {0} TO {1}"
    
    def __init__(self,host='127.0.0.1',dbname='kabudb',user='user1',password='jepco',port=3306):
        self.host = host
        self.dbname = dbname
        self.user = user
        self.password = password
        self.port = port
       
    def __connect(self):
        return MySQLdb.connect(host=self.host,port=self.port,db=self.dbname,user=self.user,passwd=self.password)
        
    def execute(self,sql):
        conn = self.__connect()
        cur = conn.cursor()
        cur.execute(sql)
        conn.commit()
        cur.close()
        conn.close()
    
    def execute_all(self,sqls):
        conn = self.__connect()
        cur = conn.cursor()
        try:
            for sql in sqls:
                cur.execute(sql)
            conn.commit()
        except MySQLdb.Error as e:
            conn.rollback()
        cur.close()
        conn.close()
    
    def execute_query(self,sql):
        conn = self.__connect()
        cur = conn.cursor()
        cur.execute(sql)
        res = cur.fetchall()
        cur.close()
        conn.close()
        return res
    
    def execute_scalor(self,sql):
        conn = self.__connect()
        cur = conn.cursor()
        cur.execute(sql)
        res = cur.fetchone()
        cur.close()
        conn.close()
        return res[0] if res != None else None
    
    def create(self,tablename,columns,primarykey = '',isdrop=False):
        if isdrop :
            self.drop(tablename)
        pkey = ',primary key({0})'.format(primarykey) if primarykey != '' else ''
        sql = 'create table {0}({1} {2})'.format(tablename,columns,pkey)
        self.execute(sql)
    
    def drop(self,tablename):
        res = self.execute_query(self.GET_TABLE_LIST_QUERY.format(tablename,self.dbname,'%%'))
        if len(res) > 0:
            self.execute("drop {0} {1}".format(res[0][1],tablename))
  
    def exists(self,tablename):
        res = self.execute_scalor(self.GET_TABLE_LIST_QUERY.format(tablename,self.dbname,'%%'))
        return False if res == None else True
    
    def rename(self,old_tablename,new_tablename):
        self.execute(self.GET_RENAME_TABLE_QUERY.format(old_tablename,new_tablename))
    
    def add_column(self,tablename,columns):
        for column in columns:
            self.execute(self.GET_ALTER_TABLE_QUERY.format(tablename,column))
    
    def get_table_list(self,table_type=""):
        res = self.execute_query(self.GET_TABLE_LIST_QUERY.format('%%',self.dbname,'%' + table_type + '%'))
        return [name[0] for name in res]
    
    def get_column_type(self,tablename):
        res = self.execute_query(self.GET_COLUMN_LIST_QUERY.format(tablename,self.dbname))
        return [(name[1],name[2]) for name in res]
    
    def get_column_list(self,tablename):
        res = self.execute_query(self.GET_COLUMN_LIST_QUERY.format(tablename,self.dbname))
        return [name[1] for name in res]

def ticker_list_update(mycon,ticker_name,ticker_last_date):
    sql="update tickers set last_date = '"+ticker_last_date+"' where name ='"+ticker_name +"';"
    mycon.execute(sql)
    return 0


def get_stock_data(mycon,ticker_code,ticker_name,ticker_last_date):
    try:
        # 株価情報の取得
        ticker_info  =  yf.Ticker(ticker_code)
        stock_data=ticker_info.history(period="max")
        #ticker_name1=ticker_info.info["longBusinessSummary"].split()[0]
        #ticker_name2=ticker_info.info["longBusinessSummary"].split()[1]
        ticker_sheet_name = ticker_code # + " " + ticker_name1 + " " + ticker_name2
        stock_data["Date"] = stock_data.index.strftime('%Y-%m-%d')
        stock_data["Roll_14"] = stock_data["Open"].rolling(14).mean()
        stock_data["Roll_25"] = stock_data["Open"].rolling(25).mean()
        stock_data["Roll_70"] = stock_data["Open"].rolling(70).mean()
        stock_data["Roll_14_70_diff"] = stock_data["Roll_70"] - stock_data["Roll_14"] 
        stock_data["Diff"] =stock_data["Open"].diff()
        stock_data["pct_chg"] = stock_data["Open"].pct_change()
        stock_data['sigma'] = stock_data["Open"].rolling(25).std()
        stock_data["BB_h1"] = stock_data["Roll_25"] + stock_data['sigma']
        stock_data["BB_h2"] = stock_data["Roll_25"] + stock_data['sigma']  * 2
        stock_data["BB_h3"] = stock_data["Roll_25"] + stock_data['sigma']  * 3
        stock_data["BB_h4"] = stock_data["Roll_25"] + stock_data['sigma']  * 4
        stock_data["BB_l1"] = stock_data["Roll_25"] - stock_data['sigma']
        stock_data["BB_l2"] = stock_data["Roll_25"] - stock_data['sigma']  * 2
        stock_data["BB_l3"] = stock_data["Roll_25"] - stock_data['sigma']  * 3
        stock_data["BB_l4"] = stock_data["Roll_25"] - stock_data['sigma']  * 4
        df_up, df_down = stock_data["Diff"].copy(), stock_data["Diff"].copy()
        df_up[df_up < 0] = 0
        df_down[df_down > 0] = 0
        df_up_14 = df_up.rolling(window = 14, center = False).mean()
        df_down_14 = abs(df_down.rolling(window = 14, center = False).mean())
        stock_data["RSI_14"]= (df_up_14 / (df_up_14 + df_down_14)) * 100
        df_up_25 = df_up.rolling(window = 25, center = False).mean()
        df_down_25 = abs(df_down.rolling(window = 25, center = False).mean())
        stock_data["RSI_25"]= (df_up_25 / (df_up_25 + df_down_25)) * 100
        stock_data["pct_kai_75"]  = (stock_data["Open"].diff(75) + stock_data["Open"] ) / stock_data["Open"]
        stock_data["pct_kai_40"]  = (stock_data["Open"].diff(40) + stock_data["Open"] ) / stock_data["Open"]
        stock_data["pct_kai_25"]  = (stock_data["Open"].diff(25) + stock_data["Open"] ) / stock_data["Open"]
        stock_data["pct_kai_14"]  = (stock_data["Open"].diff(14) + stock_data["Open"] ) / stock_data["Open"]
        stock_data=stock_data.reindex(columns=['Date','Open','Diff','Roll_14','Roll_25','Roll_70','Roll_14_70_diff','BB_h1','BB_h2','BB_h3','BB_h4','BB_l1','BB_l2','BB_l3','BB_l4','RSI_14','RSI_25','pct_kai_75','pct_kai_40','pct_kai_25','pct_kai_14','High','Low','Close','Volume','sigma','Dividends','Stock Splits'])
        stock_data=stock_data[stock_data["Date"] >= ticker_last_date.strftime('%Y-%m-%d')]
        sqls=[]
        for row in stock_data.iterrows():
            sql="REPLACE INTO stock_rates (ticker_cd,Date,Open,Diff,Roll_14,Roll_25,Roll_70,Roll_14_70_diff,BB_h1" \
                        +",BB_h2,BB_h3,BB_h4,BB_l1,BB_l2,BB_l3,BB_l4,RSI_14,RSI_25,pct_kai_75,pct_kai_40,pct_kai_14,High," \
                        +"Low,Close,Volume,sigma,Dividends,StockSplits) values ('"+ticker_code +"','" \
                        +row[1]["Date"]+"','"+str(row[1]["Open"])+"','"+str(row[1]["Diff"])+"','"+str(row[1]["Roll_14"])+"','"+str(row[1]["Roll_25"])+"','"+str(row[1]["Roll_70"]) \
                        +"','"+str(row[1]["Roll_14_70_diff"])+"','"+str(row[1]["BB_h1"])+"','"+str(row[1]["BB_h2"])+"','"+str(row[1]["BB_h3"])+"','"+str(row[1]["BB_h4"])+"','"+str(row[1]["BB_l1"]) \
                        +"','"+str(row[1]["BB_l2"])+"','"+str(row[1]["BB_l3"])+"','"+str(row[1]["BB_l4"])+"','"+str(row[1]["RSI_14"])+"','"+str(row[1]["RSI_25"])+"','"+str(row[1]["pct_kai_75"]) \
                        +"','"+str(row[1]["pct_kai_40"])+"','"+str(row[1]["pct_kai_14"])+"','"+str(row[1]["High"])+"','"+str(row[1]["Low"])+"','"+str(row[1]["Close"])+"','"+str(row[1]["Volume"]) \
                        +"','"+str(row[1]["sigma"])+"','"+str(row[1]["Dividends"])+"','"+str(row[1]["Stock Splits"])+"')"
            sql=sql.replace("'nan'","null")
            sqls.append(sql)
        
        mycon.execute_all(sqls)
        ticker_last_date=stock_data["Date"].max()
        ticker_list_update(mycon,ticker_name,ticker_last_date)
        #ファイル出力
        #filename = 'C:\\python\\' + ticker_code + '.xlsx'
        #stock_data.iloc[::-1].to_excel(filename, sheet_name=ticker_sheet_name, index=False, header=True)
        return stock_data
    except Exception as e:
        print("エラーが発生しました:", e)
        return None

def ticker_list(mycon):
    sql = "select name,ticker_cd,last_date from tickers;"
    ticker_list = mycon.execute_query(sql)
    return pd.DataFrame(ticker_list)


if __name__ == "__main__":
    # stock_data = get_stock_data("9831.T")
    mycon=MySQLConnect()
    ticker_list=ticker_list(mycon)
    for ticker in ticker_list.iterrows():
        ticker_inx=ticker[0]
        ticker_name=ticker[1][0]
        ticker_code=ticker[1][1]
        ticker_last_date=ticker[1][2]
        stock_data = get_stock_data(mycon,ticker_code,ticker_name,ticker_last_date)
        print(ticker_code)
        if stock_data is None:
            print("株価情報の取得に失敗しました。" + ticker_name)
