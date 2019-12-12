from time import sleep
import smtplib
import os
import datetime

def rename_log(current_name):
    now = datetime.datetime.now().strftime("%Y-%m-%d-%H:%M")
    try:
        new_name = current_name +'.' +now
        os.rename(current_name, new_name)
    except:
        file = open(current_name, "w")
        file.close()
    file = open(current_name, "w")
    file.close()
    return file

print (datetime.datetime.now())
os.system("sudo docker ps|grep splash > /home/liu433/gasinfo/splash_monitoring/splash_monitor.txt")

monit_splash = open('/home/liu433/gasinfo/splash_monitoring/splash_monitor.txt', 'r')
result1  =  monit_splash.read()
print result1
monit_splash.close()

monit_code = open('/home/liu433/gasinfo/loggings/code_break_issue.log', 'r')
result2  =  monit_code.read()
monit_code.close()

monit_html = open('/home/liu433/gasinfo/loggings/get_html_issue.log', 'r')
result3 = monit_html.read()
monit_html.close()

if len(result1) < 10:
    server = smtplib.SMTP('smtp.gmail.com',587 )
    #server.connect("smtp.example.com",587)
    server.ehlo()
    server.starttls()
    server.ehlo()
    #Next, log in to the server
    server.login("splashmonitoring7@gmail.com", "Liu433@m")

    #Send the mail
    msg = "Hello!Splash server is down"
    server.sendmail("splashmonitoring7@gmail.com", "liu433@mcmaster.ca", msg)
    server.close()
    #os.system("sudo systemctl start docker")
    sleep(5)
    #os.system("sudo docker run -p 8050:8050 -p 5023:5023 scrapinghub/splash")
    os.system("sudo docker start 6c4dd30f1311")
    sleep(5)
    os.system('sudo python /home/liu433/gasinfo/hourly_script.py')

if len(result2) > 5:
    server = smtplib.SMTP('smtp.gmail.com',587 )
    #server.connect("smtp.example.com",587)
    server.ehlo()
    server.starttls()
    server.ehlo()
    #Next, log in to the server
    server.login("splashmonitoring7@gmail.com", "Liu433@m")

    #Send the mail
    msg = "code break!!!"
    server.sendmail("splashmonitoring7@gmail.com", "liu433@mcmaster.ca", msg)
    server.close()
    print result2
    rename_log('/home/liu433/gasinfo/loggings/code_break_issue.log')
    
if len(result3) > 5:
    server = smtplib.SMTP('smtp.gmail.com',587 )
    #server.connect("smtp.example.com",587)
    server.ehlo()
    server.starttls()
    server.ehlo()
    #Next, log in to the server
    server.login("splashmonitoring7@gmail.com", "Liu433@m")

    #Send the mail
    msg = "Hello! HTML is not returned from splash"
    server.sendmail("splashmonitoring7@gmail.com", "liu433@mcmaster.ca", msg)
    server.close()
    print result3
    rename_log('/home/liu433/gasinfo/loggings/get_html_issue.log')

