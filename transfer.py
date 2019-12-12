import pymysql
import traceback

#config = {
#    'host': 'localhost',
#    'port': 3306,
#    'user': 'root',
#    'database':'GasinfoProject',
#    'charset':'utf8mb4',
#    'passwd': 'Lawrence0613.',
#    'cursorclass':pymysql.cursors.DictCursor
#    }

conn = pymysql.connect(host='localhost',
                     user='root',
                     password='Lawrence0613.',
                     db='GasinfoProject')
#conn = pymysql.connect(**config)
conn.autocommit(1)
cursor = conn.cursor()

def main():
    index = 0
    try:
        sql = "select * from Priceinfo_google;"
        cursor.execute(sql)
        results=cursor.fetchall()
        for i in results:
            if len(i[2])!=1:
                pre = i[2].replace('$','')
            else:
                pre = 'NULL'

            if len(i[3])!=1:
                mid = i[3].replace('$','')
            else:
                mid = 'NULL'

            if len(i[4])!=1:
                reg = i[4].replace('$','')
            else:
                reg = 'NULL'

            if len(i[5])!=1:
                dis = i[5].replace('$','')
            else:
                dis = 'NULL'

            sql = """insert into Priceinfo_google_decimal(stationid, premium, midgrade, regular, diesel,fetchtime) \
                     values ('%s', %s, %s, %s, %s, '%s')"""%(i[1], pre, mid, reg, dis, i[6])
            cursor.execute(sql)
        conn.close()
    except:
        traceback.print_exc()
        conn.close()
if __name__ == '__main__':
    main()
