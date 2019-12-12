# -*- coding: utf-8 -*-
import time
import pymysql
import logging
import os
import datetime

from prettytable import PrettyTable


def main():
    start = time.time()
    db = pymysql.connect(host='macquest2.cas.mcmaster.ca',
                         user='Macquest',
                         password='Macquest2@',
                         db='GasinfoProject')
    cursor = db.cursor()
    current_time = datetime.datetime.now()
    timedelta_12 = datetime.timedelta(hours = 12)
    timedelta_24 = datetime.timedelta(hours = 24)
    timedelta_36 = datetime.timedelta(hours = 36)
    timedelta_48 = datetime.timedelta(hours = 48)




    '''Price info___________________________________________________________________________'''
    price_report = report_Priceinfo_google(cursor,
                                           current_time,
                                           (current_time-timedelta_24))
    price_report1 = report_Priceinfo_google(cursor,
                                            (current_time-timedelta_24),
                                            (current_time-timedelta_48))
    print "Summary for the Price Information: " + datetime.datetime.now().strftime("%Y-%m-%d ")
    print "Average price of Reg: " + str(price_report['summary']['reg'])
    print "Average price of Mid: " + str(price_report['summary']['mid'])
    print "Average price of Pre: " + str(price_report['summary']['pre'])
    print "In total: %s stations have been updated" %str(price_report['summary']['total'])

    print "\nCompare with\n"

    print "Summary for the Price Information: " + (current_time-timedelta_24).strftime("%Y-%m-%d ")
    print "Average price of Reg: " + str(price_report1['summary']['reg'])
    print "Average price of Mid: " + str(price_report1['summary']['mid'])
    print "Average price of Pre: " + str(price_report1['summary']['pre'])
    print "In total: %s stations have been updated" % str(price_report['summary']['total'])


    '''Popular time information report'''
    popular_time = report_Popular_time_info(cursor,
                                            current_time,
                                            (current_time-timedelta_24),
                                            (current_time-timedelta_48))

    print "\n\nFollowing are the Popular time information:"
    print "In total: %s station provide the popular time information"%(popular_time['number'])
    print "Here is the peak popular time information"
    print popular_time['Plot']

    '''Popular time live information report'''
    popular_time_live = report_Popular_time_info_live(cursor)
    print "\n\nFollowing is the live popular time information\n"
    print popular_time_live

    '''Average number and average score and photo'''
    detail = report_Aveage_score_num_photos(cursor, current_time,
                                            (current_time-timedelta_24),
                                            (current_time-timedelta_48))
    print "\n\n Average Score, Rating and Total reviews\n"
    print detail['Plot']

    recommended_reviews= report_Recommended_reviews(cursor,(current_time-timedelta_24))
    print "\n\n Top review information:\n"
    print recommended_reviews

    wholesale = report_Wholesale_price(cursor,(current_time-timedelta_24))
    print "\n\nHere is the whole sale information:"
    print wholesale

    cursor.close()
    db.close()


def report_Aveage_score_num_photos(cursor, current_time, past_time, past_time2):
    graph = PrettyTable(['stationid', 'Rating', 'Photos','Total Reviews', 'link'])
    graph.align['Photos'] = 'r'
    graph.align['Total Reviews'] = 'r'
    graph.align['link'] = 'l'
    # print current_time
    # print past_time
    # print past_time2
    sql1 = "SELECT distinct(stationid), aveage_score, number_photo,total_review FROM Aveage_score_num_photos \
           where fetchtime <'%s' and fetchtime >'%s'" \
           % (current_time, past_time)
    sql2 = "SELECT distinct(stationid), aveage_score, number_photo,total_review FROM Aveage_score_num_photos \
           where fetchtime <'%s' and fetchtime >'%s'" \
           % (past_time, past_time2)

    cursor.execute(sql1)
    result1 = cursor.fetchall()
    cursor.execute(sql2)
    result2 = cursor.fetchall()

    #compare(result1,result2)
    for i in result1:
        i = list(i)
        i.append("https://maps.google.com/?cid=%s"%(str(i[0]).strip()))
        graph.add_row(i)
    result = {'Table Name':"Average_score_num_photos", 'Prograss': "%s data instance have been collected within last 24h"\
                                                                   %len(result1), 'Plot': graph}
    return result


def report_Recommended_reviews(cursor, timerange):
    graph = PrettyTable(['stationid','recommended reviews', 'link'])
    graph.align['stationid'] = 'l'
    graph.align['recommended reviews'] = 'l'
    graph.align['link'] = 'l'
    sql = "SELECT stationid, contents FROM Recommended_reviews WHERE fetchtime > '%s'" % (timerange)
    cursor.execute(sql)
    print sql
    result = cursor.fetchall()
    for i in result:
        i = list(i)
        i.append("https://maps.google.com/?cid=%s" % (str(i[0]).strip()))
        graph.add_row(i)
    #print {'Table Name':"Recommemded reviews", 'Prograss': "%s data instance have been collected within last 24h"%len(result)}
    return graph


def report_Popular_time_info(cursor,current_time, past_time, past_time2):
     graph = PrettyTable(['stationid','weekday', 'hour','peak', 'link'])
     graph.align['stationid'] = 'l'
     graph.align['weekday'] = 'l'
     graph.align['hour'] = 'l'
     graph.align['peak'] = 'l'
     graph.align['link'] = 'l'

     sql1 = "select distinct(stationid), weekday, hour, frequency from Popular_time_info where frequency = 1 and \
             fetchtime < '%s' and fetchtime > '%s'" \
            % (current_time, past_time)

     sql11 = "select distinct(stationid) from Popular_time_info where \
             fetchtime < '%s' and fetchtime > '%s'" \
            % (current_time, past_time)

     sql2 = "select distinct(stationid), weekday, hour, frequency from Popular_time_info where frequency = 1 and \
             fetchtime < '%s' and fetchtime > '%s'" \
            % (past_time, past_time2)
     cursor.execute(sql1)
     result1 = cursor.fetchall()
     cursor.execute(sql11)
     result11 = cursor.fetchall()

     for i in result1:
         i = list(i)
         i.append("https://maps.google.com/?cid=%s" % (str(i[0]).strip()))
         graph.add_row(i)
     result = {'Table Name':"Popular_time_info", 'number': str(len(result11)),'Plot':graph}
     return result


def report_Popular_time_info_live(cursor):
    graph = PrettyTable(["field"])
    graph.align['field'] = 'l'
    yesterday = datetime.datetime.today().day - 1 
    for i in range(23):
        sql = "SELECT distinct(stationid) FROM Popular_time_info_live WHERE fetchtime>'%s' and fetchtime<'%s'" \
              % (datetime.datetime.now().strftime("%Y-%m-") + str(yesterday) +" " + str(i),
                 datetime.datetime.now().strftime("%Y-%m-") + str(yesterday) +" " +  str(i + 1))
        cursor.execute(sql)
        all = cursor.fetchall()

        graph.add_row(["From %s:00 to %s:00 there are %s stations providing live information" %\
                       (str(i),str(i+1),str(len(all)))])
        for k in all:
            graph.add_row([(k[0])])
    return graph


def report_Priceinfo_google(cursor, current_time, past_time):
    '''This class is to report the Google Price Info'''
    sql = "SELECT * FROM Priceinfo_google WHERE fetchtime <'%s' and fetchtime >'%s'"\
          % (current_time, past_time)

    sql1 = "SELECT distinct(stationid) FROM Priceinfo_google WHERE fetchtime <'%s' and fetchtime >'%s'"\
           % (current_time, past_time)

    cursor.execute(sql)
    all = cursor.fetchall()
    cursor.execute(sql1)
    distinct = cursor.fetchall()

    regular = 0.0
    count_re = 0
    midgrade = 0.0
    count_mid = 0
    premium = 0.0
    count_pre = 0
    for i in all:
        try:
            regular = float(i[4].replace('$','')) + regular
            count_re = 1 + count_re
        except:
            continue
        try:
            midgrade = float(i[3].replace('$','')) + midgrade
            count_mid = 1 + count_mid
        except:
            continue
        try:
            premium = float(i[2].replace('$','')) + premium
            count_pre = 1 + count_pre
        except:
            continue
    regular = regular/count_re
    midgrade = midgrade/count_mid
    premium = premium/count_pre
    report = {'summary': {'reg': regular,
                          'mid': midgrade,
                          'pre': premium,
                          'total':str(len(distinct))}}
    return report


def report_Wholesale_price(cursor,timerange):
    graph = PrettyTable(['location','REG_87','MID_89','SUP_91','REG_E‑10','MID_E‑5', 'ULS_Diesel','ULS_Diesel1','Furnace_Oil','Stove_Oil','fetchtime'])
    graph.align['location'] = 'l'
    sql = "SELECT location, REG_87, MID_89, SUP_91, REG_E‑10, MID_E‑5, ULS_Diesel, ULS_Diesel1, Furnace_Oil,Stove_Oil, fetchtime FROM Wholesale_price WHERE fetchtime>'%s'"%(timerange)
    cursor.execute(sql)
    all = cursor.fetchall()
    for i in all:
        graph.add_row(i)
    return graph





#
# def compare(current, past):
#     print len(current)
#     print len(past)
#     for i in range(len(current)-1):
#         print current[i] + past[i]

if __name__ == '__main__':
    main()


