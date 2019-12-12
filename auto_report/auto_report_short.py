import pymysql
import pickle
import smtplib

from email.MIMEMultipart import MIMEMultipart
from email.MIMEText import MIMEText
from email.MIMEBase import MIMEBase
from email import encoders

from datetime import datetime
from datetime import timedelta

report = open("./simplified_report.txt", "w+")
config = {
    'host': 'macquest2.cas.mcmaster.ca',
    'port': 3306,
    'user': 'Macquest',
    'database':'GasinfoProject',
    'charset':'utf8mb4',
    'passwd': 'Macquest2@',
    'cursorclass':pymysql.cursors.DictCursor
    }

conn = pymysql.connect(**config)
conn.autocommit(1)
cursor = conn.cursor()
current_time =  "Current time is %s"\
                 %datetime.now().strftime('%Y-%m-%d %H:%M:%S')
report.write(current_time + '\n')
for i in range(8):
    starting_point = (datetime.now()-timedelta(hours=(i+1))).strftime('%Y-%m-%d %H:00:00') 
    ending_point = (datetime.now()-timedelta(hours=i)).strftime('%Y-%m-%d %H:00:00')
    sql = "select count(*) from Priceinfo_google_decimal where fetchtime > '%s' and fetchtime < '%s';"\
           %(starting_point,ending_point)
    cursor.execute(sql)
    results=cursor.fetchall()
    num_station = results[0]['count(*)']
    content = "From %s to %s, there are %i stations providing price information"%\
           (starting_point, ending_point, num_station) 
    report.write(content + '\n')


sql = "select * from Priceinfo_google where fetchtime > '%s' \
       and fetchtime < '%s' and regular = '-';"\
       %(starting_point,datetime.now())
cursor.execute(sql)
exception = cursor.fetchall()
if len(exception) == 0:
    report.write( "There is no exception\n")
else:
    report.write( "There is an exception!!!!!!!\n")

#This the Popular time information table 
report.write("\n\nLive Popular time information:\n")
for i in range(8):
    starting_point = (datetime.now()-timedelta(hours=(i+1))).strftime('%Y-%m-%d %H:00:00') 
    ending_point = (datetime.now()-timedelta(hours=i)).strftime('%Y-%m-%d %H:00:00')
    sql = "select count(*) from Popular_time_info_live where fetchtime > \
           '%s' and fetchtime < '%s';"\
           %(starting_point,ending_point)
    cursor.execute(sql)
    results=cursor.fetchall()
    num_station = results[0]['count(*)']
    content = "From %s to %s, there are %i data items saved into the database table"\
               %(starting_point, ending_point, num_station) 
    report.write(content+ "\n")
report.close()
conn.close()


server = smtplib.SMTP('smtp.gmail.com',587 )
#server.connect("smtp.example.com",587)
server.ehlo()
server.starttls()
server.ehlo()
#Next, log in to the server
server.login("splashmonitoring7@gmail.com", "Liu433@m")
#Send the mail
msg = MIMEMultipart()
body = "Hi, This is the daily Report"
msg.attach(MIMEText(body, 'plain'))
msg['From'] = "splashmonitoring7@gmail.com"
msg['To'] = "jianpengliu14@gmail.com"
msg['Subject'] = "Check_point_Report %s"%(datetime.now().strftime("%Y-%m-%d %H%M"))

filename = 'report.txt'
attachment = open("./simplified_report.txt", "rb")
part = MIMEBase('application', 'octet-stream')
part.set_payload((attachment).read())
encoders.encode_base64(part)
part.add_header('Content-Disposition', "attachment; filename= %s" % filename)
msg.attach(part)
text = msg.as_string()
attachment.close()

server.sendmail("splashmonitoring7@gmail.com", "jianpengliu14@gmail.com", text)
server.quit()


