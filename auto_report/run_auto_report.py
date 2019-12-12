from time import sleep
import smtplib
import os
import datetime


from email.MIMEMultipart import MIMEMultipart
from email.MIMEText import MIMEText
from email.MIMEBase import MIMEBase
from email import encoders


# setup email server connection
server = smtplib.SMTP('smtp.gmail.com',587 )
server.ehlo()
server.starttls()
server.ehlo()
server.login("splashmonitoring7@gmail.com", "Liu433@m")


# email parameters
fromaddr = "splashmonitoring7@gmail.com"
toaddr = ["lidilee129@gmail.com","rzheng@mcmaster.ca","shahs77@mcmaster.ca","zhebettyji@gmail.com","weiy49@mcmaster.ca","liu433@mcmaster.ca","wuruhai@mcmaster.ca"]

msg = MIMEMultipart()
msg['From'] = fromaddr
msg['To'] = ", ".join(toaddr)
msg['Subject'] = "Daily Report %s"%(str(datetime.datetime.now()))
body = "Hi, This is the daily Report"
msg.attach(MIMEText(body, 'plain'))


# Report contents which are written into autoReport.txt
filename = 'report.txt'
attachment = open("/home/liu433/gasinfo/auto_report/report.txt", "rb")
part = MIMEBase('application', 'octet-stream')
part.set_payload((attachment).read())
encoders.encode_base64(part)
part.add_header('Content-Disposition', "attachment; filename= %s" % filename)
msg.attach(part) 
text = msg.as_string()
attachment.close()

# send
server.sendmail(fromaddr, toaddr, text)
server.quit()


