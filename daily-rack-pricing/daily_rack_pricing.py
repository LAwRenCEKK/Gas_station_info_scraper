import requests
#import MySQLdb
from bs4 import BeautifulSoup
import pymysql



def get_extra_info():

    # Retrive the HTML from URL
    f = requests.get('https://www.petro-canada.ca/en/rack-pricing/daily-rack-pricing.aspx')
    soup = BeautifulSoup(f.text, 'html.parser')
    # locate the table and extract information
    lis = soup.find_all('table', class_="rack-pricing__table")

    items = []
    for i in str(lis[0]).split('<tr>')[2:]:
        sub = (
            i.split('<td class="charttextwhite-s">')[0].replace('<td class=" tableheader-s" style="text-align:left;">',
                                                                ''))
        sub = sub.replace('<br/>', ' ')
        sub = sub.replace('</td>', '')
        sub = sub.replace('<th>', '')
        sub = sub.replace('</th>', '')
        sub = sub.replace('<td>', '')
        sub = sub.strip()
        items.append(sub)
    for i in range(len(items)):
        items[i] = items[i].split('\n')
    for i in range(len(items)):
        for j in range(len(items[i])):
            if items[i][j] == '':
                items[i][j] = '0'

    return items



    return items

def load_rack_price(items):
   # db = MySQLdb.connect("localhost","root","Lawrence0613.","gasinfo")
    db = pymysql.connect("localhost","root","Lawrence0613.","gasinfo")
    cursor = db.cursor()
    for i in range(len(items)):
        sql = """INSERT INTO Daily_rack_price (location, Reg_UL_Oct_87, Mid_UL_Oct_89, Sup_UL_Oct_91, Reg_UL_E_10, Mid_UL_E_5, ULS_Diesel, ULS_Diesel_No1, Seas_FFO, Stove_Oil) VALUES ('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')"""\
                 % (items[i][0], items[i][1], items[i][2], items[i][3], items[i][4], items[i][5], items[i][6], items[i][7], items[i][8], items[i][9])
        print sql
        try:
        # Execute the SQL command
            cursor.execute(sql)
        # Commit your changes in the database
            db.commit()
        except:
        # Rollback in case there is any error
            db.rollback()
        # disconnect from server
    db.close()




items = get_extra_info()
load_rack_price(items)
