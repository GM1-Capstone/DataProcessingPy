import tkinter as tk
from tkinter import *
from tkinter import ttk
from tkinter import filedialog
from tkinter import messagebox
import springhill
import manila
import json
import os
from tkcalendar import Calendar, DateEntry
import calendar
import datetime
import mysql.connector
import math
import csv

master = Tk()
master.geometry("550x350")


def generateDataTable(site_name: str, start_epoch, path = ""):
    cnx = mysql.connector.connect(host='35.9.22.102', user='root', password='VRRocks', database='gm')
    cursor = cnx.cursor()

    site_name_flowsets = site_name.lower() + "Flowsets"
    site_name_flows = site_name.lower() + "Flows"

    startofday = start_epoch
    endofday = str(int(start_epoch) + 86400)

    query = (
            "SELECT max(frame_epoch) FROM " + site_name_flowsets + " where frame_epoch < (%s)"
            )   

    cursor.execute(query, (endofday,))
    maxx = cursor.fetchall()
    cnx.commit()

    query = (
            "select min(frame_epoch) from " + site_name_flowsets + " where frame_epoch > (%s)"
            )   

    cursor.execute(query, (startofday,))
    min = cursor.fetchall()
    cnx.commit()    

    min_seconds = math.floor(float(min[0][0]))
    max_seconds = math.ceil(float(maxx[0][0]))

    print(min_seconds, max_seconds)

    time_slices = []
    temp = min_seconds
    while temp <= max_seconds:
        time_slices.append(temp)
        temp += 6

    time_slices.append(max_seconds)    
    print(time_slices)

    start_id = -1

    priorities = ["Basic", "Basic +", "Enhanced", "Enhanced +", "Premium", "Premium +"]
    basicdata = []
    basicpdata = []
    enhanceddata = []
    enhancedpdata = []
    premiumdata = []
    premiumpdata = []
    for i in range(len(time_slices) - 1):

        start_time = time_slices[i]
        end_time = time_slices[i+1]        
        # print(start_time, end_time)

        query = (
                "select id from " + site_name_flowsets + " where frame_epoch >= %s and frame_epoch < %s"
                )  

        cursor.execute(query, (start_time, end_time))
        slice = cursor.fetchall()
        # print(slice[0][0])
        # print(slice[-1][0])
        start_id = slice[0][0]
        end_id = slice[-1][0]

        # print(len(slice))
        

        if start_id == -1:
            start_id = 0
            end_id = len(slice)
        else:
            start_id = end_id
            end_id = end_id + len(slice)
        print("start, end", start_id, end_id)

        for priority in priorities:
            total_flows = 0
            total_bytes = 0
            avg_bitrate = 0
            avg_packetrate = 0
            peak_bitrate = 0
            peak_packetrate = 0
            total_packets = 0
            time_sum = 6
            count = 1

            cur_flows = (
                    "select sum(num_flows), flowset_id, priority from springhillFlowsets inner join springhillFlows on springhillFlowsets.id = springhillFlows.flowset_id where flowset_id > %s and flowset_id <= %s and priority= %s and frame_epoch > %s and frame_epoch <= %s"
                    )
            
            cursor.execute(cur_flows, (start_id, end_id, priority, start_time, end_time))
            res = cursor.fetchall()[0][0]
            # print(res)

            if res is not None:
                total_flows += res

            cur_bytes = (
                    "select sum(bytes), flowset_id, priority from springhillFlowsets inner join springhillFlows on springhillFlowsets.id = springhillFlows.flowset_id where flowset_id > %s and flowset_id <= %s and priority= %s and frame_epoch > %s and frame_epoch <= %s"
                    )
            
            cursor.execute(cur_bytes, (start_id, end_id, priority, start_time, end_time))
            res = cursor.fetchall()[0][0]

            if res is not None:
                total_bytes += res

            avg_bitrate = total_bytes / time_sum
            avg_packetrate = total_packets / time_sum

            peak_bitrate = max(peak_bitrate, avg_bitrate)
            peak_packetrate = max(peak_packetrate, avg_packetrate)


            cur_packets = (
                    "select sum(packets), flowset_id, priority from springhillFlowsets inner join springhillFlows on springhillFlowsets.id = springhillFlows.flowset_id where flowset_id > %s and flowset_id <= %s and priority= %s and frame_epoch > %s and frame_epoch <= %s"
                    )
            
            cursor.execute(cur_packets, (start_id, end_id, priority, start_time, end_time))
            res = cursor.fetchall()[0][0]

            if res is not None:
                total_packets += res

            avg_bitrate = avg_bitrate / 1000000
            peak_bitrate = peak_bitrate / 1000000

            if priority == "Basic":
                basicdata.append([count, priority, total_flows, total_bytes, avg_bitrate, avg_packetrate, peak_bitrate, peak_packetrate, total_packets])
            if priority == "Basic +":
                basicpdata.append([count, priority, total_flows, total_bytes, avg_bitrate, avg_packetrate, peak_bitrate, peak_packetrate, total_packets])
            if priority == "Enhanced":
                enhanceddata.append([count, priority, total_flows, total_bytes, avg_bitrate, avg_packetrate, peak_bitrate, peak_packetrate, total_packets])
            if priority == "Enhanced +":
                enhancedpdata.append([count, priority, total_flows, total_bytes, avg_bitrate, avg_packetrate, peak_bitrate, peak_packetrate, total_packets])
            if priority == "Premium":
                premiumdata.append([count, priority, total_flows, total_bytes, avg_bitrate, avg_packetrate, peak_bitrate, peak_packetrate, total_packets])
            if priority == "Premium +":
                premiumpdata.append([count, priority, total_flows, total_bytes, avg_bitrate, avg_packetrate, peak_bitrate, peak_packetrate, total_packets])
            time_sum += 6
            count += 1
    
    # os.mkdir(path + "\\DataTableData")

    # bfn = path + "\\DataTableData\\" + site_name + "BasicDataTableData6SecondSlices.csv"
    with open("DataTableData/Basic.csv", "w", newline='') as f:
        writer = csv.writer(f)
        writer.writerow(["row_name","priority", "total_flows", "total_bytes", "avg_bitrate(Mb/second)", "avg_packetrate(packets/second)", "peak_bitrate(Mb/second)", "peak_packetrate(packets/second)", "total_packets"])
        
        for i in range(len(basicdata)):
            # writer.writerow([i,basicdata[i][0],basicdata[i][1],basicdata[i][2], basicdata[i][3], basicdata[i][4], basicdata[i][5], basicdata[i][6], basicdata[i][7], basicdata[i][8]])
            writer.writerow([i] + basicdata[i])
    f.close()   

    # bpfn = path + "\\DataTableData\\" + site_name + "Basic+DataTableData6SecondSlices.csv"
    with open("DataTableData/Basic+.csv", "w", newline='') as f:
        writer = csv.writer(f)
        writer.writerow(["row_name","priority", "total_flows", "total_bytes", "avg_bitrate(Mb/second)", "avg_packetrate(packets/second)", "peak_bitrate(Mb/second)", "peak_packetrate(packets/second)", "total_packets"])
        
        for i in range(len(basicdata)):
            # writer.writerow([i,basicdata[i][0],basicdata[i][1],basicdata[i][2], basicdata[i][3], basicdata[i][4], basicdata[i][5], basicdata[i][6], basicdata[i][7], basicdata[i][8]])
            writer.writerow([i] + basicpdata[i])

    f.close()   

    # efn = path + "\\DataTableData\\" + site_name + "EnhancedDataTableData6SecondSlices.csv"
    with open("DataTableData/Enhanced.csv", "w", newline='') as f:
        writer = csv.writer(f)
        writer.writerow(["row_name","priority", "total_flows", "total_bytes", "avg_bitrate(Mb/second)", "avg_packetrate(packets/second)", "peak_bitrate(Mb/second)", "peak_packetrate(packets/second)", "total_packets"])
        
        for i in range(len(basicdata)):
            # writer.writerow([i,basicdata[i][0],basicdata[i][1],basicdata[i][2], basicdata[i][3], basicdata[i][4], basicdata[i][5], basicdata[i][6], basicdata[i][7], basicdata[i][8]])
            writer.writerow([i] + enhanceddata[i])

    f.close()   

    # epfn = path + "\\DataTableData\\" + site_name + "Enhanced+DataTableData6SecondSlices.csv"
    with open("DataTableData/Enhanced+.csv", "w", newline='') as f:
        writer = csv.writer(f)
        writer.writerow(["row_name","priority", "total_flows", "total_bytes", "avg_bitrate(Mb/second)", "avg_packetrate(packets/second)", "peak_bitrate(Mb/second)", "peak_packetrate(packets/second)", "total_packets"])
        
        for i in range(len(basicdata)):
            # writer.writerow([i,basicdata[i][0],basicdata[i][1],basicdata[i][2], basicdata[i][3], basicdata[i][4], basicdata[i][5], basicdata[i][6], basicdata[i][7], basicdata[i][8]])
            writer.writerow([i] + enhancedpdata[i])

    f.close()  

    # pfn = path + "\\DataTableData\\" + site_name + "PremiumDataTableData6SecondSlices.csv"
    with open("DataTableData/Premium.csv", "w", newline='') as f:
        writer = csv.writer(f)
        writer.writerow(["row_name","priority", "total_flows", "total_bytes", "avg_bitrate(Mb/second)", "avg_packetrate(packets/second)", "peak_bitrate(Mb/second)", "peak_packetrate(packets/second)", "total_packets"])
        
        for i in range(len(basicdata)):
            # writer.writerow([i,basicdata[i][0],basicdata[i][1],basicdata[i][2], basicdata[i][3], basicdata[i][4], basicdata[i][5], basicdata[i][6], basicdata[i][7], basicdata[i][8]])
            writer.writerow([i] + premiumdata[i])

    f.close()   

    # ppfn = path + "\\DataTableData\\" + site_name + "Premium+DataTableData6SecondSlices.csv"
    with open("DataTableData/Premium+.csv", "w", newline='') as f:
        writer = csv.writer(f)
        writer.writerow(["row_name","priority", "total_flows", "total_bytes", "avg_bitrate(Mb/second)", "avg_packetrate(packets/second)", "peak_bitrate(Mb/second)", "peak_packetrate(packets/second)", "total_packets"])
        
        for i in range(len(basicdata)):
            # writer.writerow([i,basicdata[i][0],basicdata[i][1],basicdata[i][2], basicdata[i][3], basicdata[i][4], basicdata[i][5], basicdata[i][6], basicdata[i][7], basicdata[i][8]])
            writer.writerow([i] + premiumpdata[i])

    f.close()  


            

def generateGraphData(site_name: str, start_epoch, path):
    
    # Connect to the database
    cnx = mysql.connector.connect(host='35.9.22.102', user='root', password='VRRocks', database='gm')
    cursor = cnx.cursor()

    site_name_flowsets = site_name.lower() + "Flowsets"
    site_name_flows = site_name.lower() + "Flows"

    startofday = start_epoch
    endofday = str(int(start_epoch) + 86400)

    query = (
            "SELECT max(frame_epoch) FROM " + site_name_flowsets + " where frame_epoch < (%s)"
            )   

    cursor.execute(query, (endofday,))
    mmax = cursor.fetchall()
    cnx.commit()

    query = (
            "select min(frame_epoch) from " + site_name_flowsets + " where frame_epoch > (%s)"
            )   

    cursor.execute(query, (startofday,))
    min = cursor.fetchall()
    cnx.commit()    

    min_seconds = math.floor(float(min[0][0]))
    max_seconds = math.ceil(float(mmax[0][0]))

    print(min_seconds, max_seconds)

    time_slices = []
    temp = min_seconds
    while temp <= max_seconds:
        time_slices.append(temp)
        temp += 6

    time_slices.append(max_seconds)    
    print(time_slices)



    pp = []
    p = []
    ep = []
    e = []
    bp = []
    b = []

    dtpp = []
    dtp = []
    dtep = []
    dte = []
    dtbp = []
    dtb = []

    time_sum = 6

    pptotal_flows = 0
    pptotal_bytes = 0
    ppavg_bitrate = 0
    ppavg_packetrate = 0
    pppeak_bitrate = 0
    pppeak_packetrate = 0
    pptotal_packets = 0

    ptotal_flows = 0
    ptotal_bytes = 0
    pavg_bitrate = 0
    pavg_packetrate = 0
    ppeak_bitrate = 0
    ppeak_packetrate = 0
    ptotal_packets = 0

    eptotal_flows = 0
    eptotal_bytes = 0
    epavg_bitrate = 0
    epavg_packetrate = 0
    eppeak_bitrate = 0
    eppeak_packetrate = 0
    eptotal_packets = 0

    etotal_flows = 0
    etotal_bytes = 0
    eavg_bitrate = 0
    eavg_packetrate = 0
    epeak_bitrate = 0
    epeak_packetrate = 0
    etotal_packets = 0

    bptotal_flows = 0
    bptotal_bytes = 0
    bpavg_bitrate = 0
    bpavg_packetrate = 0
    bppeak_bitrate = 0
    bppeak_packetrate = 0
    bptotal_packets = 0

    btotal_flows = 0
    btotal_bytes = 0
    bavg_bitrate = 0
    bavg_packetrate = 0
    bpeak_bitrate = 0
    bpeak_packetrate = 0
    btotal_packets = 0

    start_id = -1

    for i in range(len(time_slices) - 1):
        start_time = time_slices[i]
        end_time = time_slices[i+1]        
        # print(start_time, end_time)

        query = (
                "select id from " + site_name_flowsets + " where frame_epoch >= %s and frame_epoch < %s"
                )  

        cursor.execute(query, (start_time, end_time))
        slice = cursor.fetchall()
        # print(len(slice))

        if start_id == -1:
            start_id = 0
            end_id = len(slice)
        else:
            start_id = end_id
            end_id = end_id + len(slice)
        print("start, end", start_id, end_id)


        ### PREMIUM + DATA ###

        ppbytes = (
                "select sum(bytes) from " + site_name_flows + " where flowset_id > %s and flowset_id <= %s and priority= %s"
                )
        
        cursor.execute(ppbytes, (start_id, end_id, "Premium +"))
        res = cursor.fetchall()[0][0]
        if res == None:
            res = 0
        ppbr = float((res * 8) / (end_time - start_time))

        pppackets = (
                "select sum(packets) from " + site_name_flows + " where flowset_id > %s and flowset_id <= %s and priority = %s"
                )
    
        cursor.execute(pppackets, (start_id, end_id, "Premium +"))
        res = cursor.fetchall()[0][0]
        if res == None:
            res = 0
        pppr = float(res / (end_time - start_time))



        # NEW STUFF
        cur_flows = (
                "select sum(num_flows), flowset_id, priority from springhillFlowsets inner join springhillFlows on springhillFlowsets.id = springhillFlows.flowset_id where flowset_id > %s and flowset_id <= %s and priority= %s and frame_epoch > %s and frame_epoch <= %s"
                )
        
        cursor.execute(cur_flows, (start_id, end_id, "Premium +", start_time, end_time))
        res = cursor.fetchall()[0][0]
        # print(res)

        if res is not None:
            pptotal_flows += res

        cur_bytes = (
                "select sum(bytes), flowset_id, priority from springhillFlowsets inner join springhillFlows on springhillFlowsets.id = springhillFlows.flowset_id where flowset_id > %s and flowset_id <= %s and priority= %s and frame_epoch > %s and frame_epoch <= %s"
                )
        
        cursor.execute(cur_bytes, (start_id, end_id, "Premium +", start_time, end_time))
        res = cursor.fetchall()[0][0]

        if res is not None:
            pptotal_bytes += res

        ppavg_bitrate = pptotal_bytes / time_sum
        ppavg_packetrate = pptotal_packets / time_sum

        pppeak_bitrate = max(pppeak_bitrate, ppavg_bitrate)
        pppeak_packetrate = max(pppeak_packetrate, ppavg_packetrate)


        cur_packets = (
                "select sum(packets), flowset_id, priority from springhillFlowsets inner join springhillFlows on springhillFlowsets.id = springhillFlows.flowset_id where flowset_id > %s and flowset_id <= %s and priority= %s and frame_epoch > %s and frame_epoch <= %s"
                )
        
        cursor.execute(cur_packets, (start_id, end_id, "Premium +", start_time, end_time))
        res = cursor.fetchall()[0][0]

        if res is not None:
            pptotal_packets += res

        ppavg_bitrate = ppavg_bitrate / 1000000
        pppeak_bitrate = pppeak_bitrate / 1000000

        dtpp.append(["Premium +", pptotal_flows, pptotal_bytes, ppavg_bitrate, ppavg_packetrate, pppeak_bitrate, pppeak_packetrate, pptotal_packets])









        #######################




        ### PREMIUM DATA ###

        pbytes = (
                "select sum(bytes) from " + site_name_flows + " where flowset_id > %s and flowset_id <= %s and priority= %s"
                )
        
        cursor.execute(pbytes, (start_id, end_id, "Premium"))
        res = cursor.fetchall()[0][0]
        if res == None:
            res = 0
        pbr = float((res * 8) / (end_time - start_time))

        ppackets = (
                "select sum(packets) from " + site_name_flows + " where flowset_id > %s and flowset_id <= %s and priority = %s"
                )
    
        cursor.execute(ppackets, (start_id, end_id, "Premium"))
        res = cursor.fetchall()[0][0]
        if res == None:
            res = 0
        ppr = float((res) / (end_time - start_time))


        # NEW STUFF
        cur_flows = (
                "select sum(num_flows), flowset_id, priority from springhillFlowsets inner join springhillFlows on springhillFlowsets.id = springhillFlows.flowset_id where flowset_id > %s and flowset_id <= %s and priority= %s and frame_epoch > %s and frame_epoch <= %s"
                )
        
        cursor.execute(cur_flows, (start_id, end_id, "Premium", start_time, end_time))
        res = cursor.fetchall()[0][0]
        # print(res)

        if res is not None:
            ptotal_flows += res

        cur_bytes = (
                "select sum(bytes), flowset_id, priority from springhillFlowsets inner join springhillFlows on springhillFlowsets.id = springhillFlows.flowset_id where flowset_id > %s and flowset_id <= %s and priority= %s and frame_epoch > %s and frame_epoch <= %s"
                )
        
        cursor.execute(cur_bytes, (start_id, end_id, "Premium", start_time, end_time))
        res = cursor.fetchall()[0][0]

        if res is not None:
            ptotal_bytes += res

        pavg_bitrate = ptotal_bytes / time_sum
        pavg_packetrate = ptotal_packets / time_sum

        ppeak_bitrate = max(ppeak_bitrate, pavg_bitrate)
        ppeak_packetrate = max(ppeak_packetrate, pavg_packetrate)


        cur_packets = (
                "select sum(packets), flowset_id, priority from springhillFlowsets inner join springhillFlows on springhillFlowsets.id = springhillFlows.flowset_id where flowset_id > %s and flowset_id <= %s and priority= %s and frame_epoch > %s and frame_epoch <= %s"
                )
        
        cursor.execute(cur_packets, (start_id, end_id, "Premium", start_time, end_time))
        res = cursor.fetchall()[0][0]

        if res is not None:
            ptotal_packets += res

        pavg_bitrate = pavg_bitrate / 1000000
        ppeak_bitrate = ppeak_bitrate / 1000000

        dtp.append(["Premium", ptotal_flows, ptotal_bytes, pavg_bitrate, pavg_packetrate, ppeak_bitrate, ppeak_packetrate, ptotal_packets])



        ######################





        ### ENHANCED + DATA ###

        epbytes = (
                "select sum(bytes) from " + site_name_flows + " where flowset_id > %s and flowset_id <= %s and priority= %s"
                )
        
        cursor.execute(epbytes, (start_id, end_id, "Enhanced +"))
        res = cursor.fetchall()[0][0]
        if res == None:
            res = 0
        epbr = float((res * 8) / (end_time - start_time))

        eppackets = (
                "select sum(packets) from " + site_name_flows + " where flowset_id > %s and flowset_id <= %s and priority = %s"
                )
    
        cursor.execute(eppackets, (start_id, end_id, "Enhanced +"))
        res = cursor.fetchall()[0][0]
        if res == None:
            res = 0
        eppr = float((res) / (end_time - start_time))

        # NEW STUFF
        cur_flows = (
                "select sum(num_flows), flowset_id, priority from springhillFlowsets inner join springhillFlows on springhillFlowsets.id = springhillFlows.flowset_id where flowset_id > %s and flowset_id <= %s and priority= %s and frame_epoch > %s and frame_epoch <= %s"
                )
        
        cursor.execute(cur_flows, (start_id, end_id, "Enhanced +", start_time, end_time))
        res = cursor.fetchall()[0][0]
        # print(res)

        if res is not None:
            eptotal_flows += res

        cur_bytes = (
                "select sum(bytes), flowset_id, priority from springhillFlowsets inner join springhillFlows on springhillFlowsets.id = springhillFlows.flowset_id where flowset_id > %s and flowset_id <= %s and priority= %s and frame_epoch > %s and frame_epoch <= %s"
                )
        
        cursor.execute(cur_bytes, (start_id, end_id, "Enhanced +", start_time, end_time))
        res = cursor.fetchall()[0][0]

        if res is not None:
            eptotal_bytes += res

        epavg_bitrate = eptotal_bytes / time_sum
        epavg_packetrate = eptotal_packets / time_sum

        eppeak_bitrate = max(eppeak_bitrate, epavg_bitrate)
        eppeak_packetrate = max(eppeak_packetrate, epavg_packetrate)


        cur_packets = (
                "select sum(packets), flowset_id, priority from springhillFlowsets inner join springhillFlows on springhillFlowsets.id = springhillFlows.flowset_id where flowset_id > %s and flowset_id <= %s and priority= %s and frame_epoch > %s and frame_epoch <= %s"
                )
        
        cursor.execute(cur_packets, (start_id, end_id, "Enhanced +", start_time, end_time))
        res = cursor.fetchall()[0][0]

        if res is not None:
            eptotal_packets += res

        epavg_bitrate = epavg_bitrate / 1000000
        eppeak_bitrate = eppeak_bitrate / 1000000

        dtep.append(["Enhanced +", eptotal_flows, eptotal_bytes, epavg_bitrate, epavg_packetrate, eppeak_bitrate, eppeak_packetrate, eptotal_packets])
        
        ########################



        ### ENHANCED DATA ###

        ebytes = (
                "select sum(bytes) from " + site_name_flows + " where flowset_id > %s and flowset_id <= %s and priority= %s"
                )
        
        cursor.execute(ebytes, (start_id, end_id, "Enhanced"))
        res = cursor.fetchall()[0][0]
        if res == None:
            res = 0
        ebr = float((res * 8) / (end_time - start_time))

        epackets = (
                "select sum(packets) from " + site_name_flows + " where flowset_id > %s and flowset_id <= %s and priority = %s"
                )
    
        cursor.execute(epackets, (start_id, end_id, "Enhanced"))
        res = cursor.fetchall()[0][0]
        if res == None:
            res = 0
        epr = float((res) / (end_time - start_time))


        # NEW STUFF
        cur_flows = (
                "select sum(num_flows), flowset_id, priority from springhillFlowsets inner join springhillFlows on springhillFlowsets.id = springhillFlows.flowset_id where flowset_id > %s and flowset_id <= %s and priority= %s and frame_epoch > %s and frame_epoch <= %s"
                )
        
        cursor.execute(cur_flows, (start_id, end_id, "Enhanced", start_time, end_time))
        res = cursor.fetchall()[0][0]
        # print(res)

        if res is not None:
            etotal_flows += res

        cur_bytes = (
                "select sum(bytes), flowset_id, priority from springhillFlowsets inner join springhillFlows on springhillFlowsets.id = springhillFlows.flowset_id where flowset_id > %s and flowset_id <= %s and priority= %s and frame_epoch > %s and frame_epoch <= %s"
                )
        
        cursor.execute(cur_bytes, (start_id, end_id, "Enhanced", start_time, end_time))
        res = cursor.fetchall()[0][0]

        if res is not None:
            etotal_bytes += res

        eavg_bitrate = etotal_bytes / time_sum
        eavg_packetrate = etotal_packets / time_sum

        epeak_bitrate = max(epeak_bitrate, eavg_bitrate)
        epeak_packetrate = max(epeak_packetrate, eavg_packetrate)


        cur_packets = (
                "select sum(packets), flowset_id, priority from springhillFlowsets inner join springhillFlows on springhillFlowsets.id = springhillFlows.flowset_id where flowset_id > %s and flowset_id <= %s and priority= %s and frame_epoch > %s and frame_epoch <= %s"
                )
        
        cursor.execute(cur_packets, (start_id, end_id, "Enhanced", start_time, end_time))
        res = cursor.fetchall()[0][0]

        if res is not None:
            etotal_packets += res

        eavg_bitrate = eavg_bitrate / 1000000
        epeak_bitrate = epeak_bitrate / 1000000

        dte.append(["Enhanced", etotal_flows, etotal_bytes, eavg_bitrate, eavg_packetrate, epeak_bitrate, epeak_packetrate, etotal_packets])


        ######################



        ### BASIC + DATA ###

        bpbytes = (
                "select sum(bytes) from " + site_name_flows + " where flowset_id > %s and flowset_id <= %s and priority= %s"
                )
        
        cursor.execute(bpbytes, (start_id, end_id, "Basic +"))
        res = cursor.fetchall()[0][0]
        if res == None:
            res = 0
        bpbr = float((res * 8) / (end_time - start_time))

        bppackets = (
                "select sum(packets) from " + site_name_flows + " where flowset_id > %s and flowset_id <= %s and priority = %s"
                )
    
        cursor.execute(bppackets, (start_id, end_id, "Basic +"))
        res = cursor.fetchall()[0][0]
        if res == None:
            res = 0
        bppr = float((res) / (end_time - start_time))


        # NEW STUFF
        cur_flows = (
                "select sum(num_flows), flowset_id, priority from springhillFlowsets inner join springhillFlows on springhillFlowsets.id = springhillFlows.flowset_id where flowset_id > %s and flowset_id <= %s and priority= %s and frame_epoch > %s and frame_epoch <= %s"
                )
        
        cursor.execute(cur_flows, (start_id, end_id, "Basic +", start_time, end_time))
        res = cursor.fetchall()[0][0]
        # print(res)

        if res is not None:
            bptotal_flows += res

        cur_bytes = (
                "select sum(bytes), flowset_id, priority from springhillFlowsets inner join springhillFlows on springhillFlowsets.id = springhillFlows.flowset_id where flowset_id > %s and flowset_id <= %s and priority= %s and frame_epoch > %s and frame_epoch <= %s"
                )
        
        cursor.execute(cur_bytes, (start_id, end_id, "Basic +", start_time, end_time))
        res = cursor.fetchall()[0][0]

        if res is not None:
            bptotal_bytes += res

        bpavg_bitrate = bptotal_bytes / time_sum
        bpavg_packetrate = bptotal_packets / time_sum

        bppeak_bitrate = max(bppeak_bitrate, bpavg_bitrate)
        bppeak_packetrate = max(bppeak_packetrate, bpavg_packetrate)


        cur_packets = (
                "select sum(packets), flowset_id, priority from springhillFlowsets inner join springhillFlows on springhillFlowsets.id = springhillFlows.flowset_id where flowset_id > %s and flowset_id <= %s and priority= %s and frame_epoch > %s and frame_epoch <= %s"
                )
        
        cursor.execute(cur_packets, (start_id, end_id, "Basic +", start_time, end_time))
        res = cursor.fetchall()[0][0]

        if res is not None:
            bptotal_packets += res

        bpavg_bitrate = bpavg_bitrate / 1000000
        bppeak_bitrate = bppeak_bitrate / 1000000

        dtbp.append(["Basic +", bptotal_flows, bptotal_bytes, bpavg_bitrate, bpavg_packetrate, bppeak_bitrate, bppeak_packetrate, bptotal_packets])



        ####################


        ### BASIC DATA ###

        bbytes = (
                "select sum(bytes) from " + site_name_flows + " where flowset_id > %s and flowset_id <= %s and priority= %s"
                )
        
        cursor.execute(bbytes, (start_id, end_id, "Basic"))
        res = cursor.fetchall()[0][0]
        if res == None:
            res = 0
        bbr = float((res * 8) / (end_time - start_time))

        bpackets = (
                "select sum(packets) from " + site_name_flows + " where flowset_id > %s and flowset_id <= %s and priority = %s"
                )
    
        cursor.execute(bpackets, (start_id, end_id, "Basic"))
        res = cursor.fetchall()[0][0]
        if res == None:
            res = 0
        bpr = float((res) / (end_time - start_time))    


        # NEW STUFF
        cur_flows = (
                "select sum(num_flows), flowset_id, priority from springhillFlowsets inner join springhillFlows on springhillFlowsets.id = springhillFlows.flowset_id where flowset_id > %s and flowset_id <= %s and priority= %s and frame_epoch > %s and frame_epoch <= %s"
                )
        
        cursor.execute(cur_flows, (start_id, end_id, "Basic", start_time, end_time))
        res = cursor.fetchall()[0][0]
        # print(res)

        if res is not None:
            btotal_flows += res

        cur_bytes = (
                "select sum(bytes), flowset_id, priority from springhillFlowsets inner join springhillFlows on springhillFlowsets.id = springhillFlows.flowset_id where flowset_id > %s and flowset_id <= %s and priority= %s and frame_epoch > %s and frame_epoch <= %s"
                )
        
        cursor.execute(cur_bytes, (start_id, end_id, "Basic", start_time, end_time))
        res = cursor.fetchall()[0][0]

        if res is not None:
            btotal_bytes += res

        bavg_bitrate = btotal_bytes / time_sum
        bavg_packetrate = btotal_packets / time_sum

        bpeak_bitrate = max(bpeak_bitrate, bavg_bitrate)
        bpeak_packetrate = max(bpeak_packetrate, bavg_packetrate)


        cur_packets = (
                "select sum(packets), flowset_id, priority from springhillFlowsets inner join springhillFlows on springhillFlowsets.id = springhillFlows.flowset_id where flowset_id > %s and flowset_id <= %s and priority= %s and frame_epoch > %s and frame_epoch <= %s"
                )
        
        cursor.execute(cur_packets, (start_id, end_id, "Basic", start_time, end_time))
        res = cursor.fetchall()[0][0]

        if res is not None:
            btotal_packets += res

        bavg_bitrate = bavg_bitrate / 1000000
        bpeak_bitrate = bpeak_bitrate / 1000000

        dtb.append(["Basic", btotal_flows, btotal_bytes, bavg_bitrate, bavg_packetrate, bpeak_bitrate, bpeak_packetrate, btotal_packets])

        ####################  



        pp.append(["Premium +", start_time, end_time, ppbr, pppr])
        p.append(["Premium",start_time, end_time, pbr, ppr])

        ep.append(["Enhanced +", start_time, end_time, epbr, eppr])
        e.append(["Enhanced",start_time, end_time, ebr, epr])

        bp.append(["Basic +", start_time, end_time, bpbr, bppr])
        b.append(["Basic", start_time, end_time, bbr, bpr])

        time_sum += 6


    maxbrpp = -1
    maxprpp = -1

    maxbrp = -1
    maxprp = -1    
    
    maxbrep = -1
    maxprep = -1

    maxbre = -1
    maxpre = -1

    maxbrbp = -1
    maxprbp = -1

    maxbrb = -1
    maxprb = -1
    
    ppbrs = []
    pbrs = []
    epbrs = []
    ebrs = []
    bpbrs = []
    bbrs = []
    ppprs = []
    pprs = []
    epprs = []
    eprs = []
    bpprs = []
    bprs = []    

    for i in range(len(pp)):
        brpp = pp[i][3]
        ppbrs.append(brpp)
        if brpp > maxbrpp:
            maxbrpp = brpp

        prpp = pp[i][4]
        ppprs.append(prpp)
        if prpp > maxprpp:
            maxprpp = prpp


        brp = p[i][3]
        pbrs.append(brp)
        if brp > maxbrp:
            maxbrp = brp

        prp = p[i][4]
        pprs.append(prp)
        if prp > maxprp:
            maxprp = prp


        brep = ep[i][3]
        epbrs.append(brep)
        if brep > maxbrep:
            maxbrep = brep

        prep = ep[i][4]
        epprs.append(prep)
        if prep > maxprep:
            maxprep = prep


        bre = e[i][3]
        ebrs.append(bre)
        if bre > maxbre:
            maxbre = bre

        pre = e[i][4]
        eprs.append(pre)
        if pre > maxpre:
            maxpre = pre



        brbp = bp[i][3]
        bpbrs.append(brbp)
        if brbp > maxbrbp:
            maxbrbp = brbp

        prbp = bp[i][4]
        bpprs.append(prbp)
        if prbp > maxprbp:
            maxprbp = prbp


        brb = b[i][3]
        bbrs.append(brb)
        if brb > maxbrb:
            maxbrb = brb

        prb = b[i][4]
        bprs.append(prb)
        if prb > maxprb:
            maxprb = prb


    
    os.mkdir(path + "\\GraphData")

    bfn = path + "\\GraphData\\" + site_name + "BasicGraphData6SecondSlices.csv"
    with open(bfn, "w", newline='') as f:
        writer = csv.writer(f)
        writer.writerow(["row_name","priority", "start_time", "end_time", "bitrate(bits/second)", "packetrate(packets/second)"])
        
        for i in range(len(b)):
            # writer.writerow(b[i])
            # print(b[3])
            writer.writerow([i,b[i][0],b[i][1],b[i][2],(float(b[i][3])/1000000), b[i][4]])

    f.close()   

    bpfn = path + "\\GraphData\\" + site_name + "Basic+GraphData6SecondSlices.csv"
    with open(bpfn, "w", newline='') as f:
        writer = csv.writer(f)
        writer.writerow(["row_name","priority", "start_time", "end_time", "bitrate(bits/second)", "packetrate(packets/second)"])
        
        for i in range(len(bp)):
            # writer.writerow(bp[i])
            writer.writerow([i,bp[i][0],bp[i][1],bp[i][2],(float(bp[i][3])/1000000), bp[i][4]])

    f.close()   

    efn = path + "\\GraphData\\" + site_name + "EnhancedGraphData6SecondSlices.csv"
    with open(efn, "w", newline='') as f:
        writer = csv.writer(f)
        writer.writerow(["row_name","priority", "start_time", "end_time", "bitrate(bits/second)", "packetrate(packets/second)"])
        
        for i in range(len(e)):
            # writer.writerow(e[i])
            writer.writerow([i,e[i][0],e[i][1],e[i][2],(float(e[i][3])/1000000), e[i][4]])

    f.close()   

    epfn = path + "\\GraphData\\" + site_name + "Enhanced+GraphData6SecondSlices.csv"
    with open(epfn, "w", newline='') as f:
        writer = csv.writer(f)
        writer.writerow(["row_name","priority", "start_time", "end_time", "bitrate(bits/second)", "packetrate(packets/second)"])
        
        for i in range(len(ep)):
            # writer.writerow(ep[i])
            writer.writerow([i,ep[i][0],ep[i][1],ep[i][2],(float(ep[i][3])/1000000), ep[i][4]])

    f.close()  

    pfn = path + "\\GraphData\\" + site_name + "PremiumGraphData6SecondSlices.csv"
    with open(pfn, "w", newline='') as f:
        writer = csv.writer(f)
        writer.writerow(["row_name","priority", "start_time", "end_time", "bitrate(bits/second)", "packetrate(packets/second)"])
        
        for i in range(len(p)):
            # writer.writerow(p[i])
            writer.writerow([i,p[i][0],p[i][1],p[i][2],(float(p[i][3])/1000000), p[i][4]])

    f.close()   

    ppfn = path + "\\GraphData\\" + site_name + "Premium+GraphData6SecondSlices.csv"
    with open(ppfn, "w", newline='') as f:
        writer = csv.writer(f)
        writer.writerow(["row_name","priority", "start_time", "end_time", "bitrate(bits/second)", "packetrate(packets/second)"])
        
        for i in range(len(pp)):
            # writer.writerow(pp[i])
            writer.writerow([i,pp[i][0],pp[i][1],pp[i][2],(float(pp[i][3])/1000000), pp[i][4]])

    f.close()  


    tabledatafn = path + "\\GraphData\\" + site_name + "TableData.csv"

    # averages

    ppavgbr = (sum(ppbrs)/len(ppbrs)) / 1000000
    pavgbr = (sum(pbrs)/len(pbrs)) / 1000000

    epavgbr = (sum(epbrs)/len(epbrs)) / 1000000
    eavgbr = (sum(ebrs)/len(ebrs)) / 1000000

    bpavgbr = (sum(bpbrs)/len(bpbrs)) / 1000000
    bavgbr = (sum(bbrs)/len(bbrs)) / 1000000


    ppavgpr = (sum(ppprs)/len(ppprs)) / 1000000
    pavgpr = (sum(pprs)/len(pprs)) / 1000000

    epavgpr = (sum(epprs)/len(epprs)) / 1000000
    eavgpr = (sum(eprs)/len(eprs)) / 1000000

    bpavgpr = (sum(bpprs)/len(bpprs)) / 1000000
    bavgpr = (sum(bprs)/len(bprs)) / 1000000

    # maxes/peaks
    print("prem +")
    print(maxbrpp, maxprpp)

    print("prem")
    print(maxbrp, maxprp  )
    
    print("enhanced plus")
    print(maxbrep, maxprep)

    print("enhanced")
    print(maxbre, maxpre)

    print("basic plsu")
    print(maxbrbp, maxprbp)

    print("basic")
    print(maxbrb, maxprb)

    # totals
    print(min_seconds, max_seconds)
    
    min_seconds = str(min_seconds)
    max_seconds = str(max_seconds)

    query = ("select sum(" + site_name_flows + ".packets) from " + site_name_flowsets + " inner join " + site_name_flows + " on " + site_name_flowsets +".id=" + site_name_flows + ".flowset_id where " + site_name_flows + ".priority=\"Basic\" and " + site_name_flowsets + ".frame_epoch > " + min_seconds + " and " + site_name_flowsets + ".frame_epoch < " + max_seconds + " ")

    cursor.execute(query)
    basictotalpackets = cursor.fetchall()[0][0]

    query = ("select sum(" + site_name_flows + ".packets) from " + site_name_flowsets + " inner join " + site_name_flows + " on " + site_name_flowsets +".id=" + site_name_flows + ".flowset_id where " + site_name_flows + ".priority=\"Basic +\" and " + site_name_flowsets + ".frame_epoch > " + min_seconds + " and " + site_name_flowsets + ".frame_epoch < " + max_seconds + " ")

    cursor.execute(query)
    basicplustotalpackets = cursor.fetchall()[0][0]

    query = ("select sum(" + site_name_flows + ".packets) from " + site_name_flowsets + " inner join " + site_name_flows + " on " + site_name_flowsets +".id=" + site_name_flows + ".flowset_id where " + site_name_flows + ".priority=\"Enhanced\" and " + site_name_flowsets + ".frame_epoch > " + min_seconds + " and " + site_name_flowsets + ".frame_epoch < " + max_seconds + " ")

    cursor.execute(query)
    enhancedtotalpackets = cursor.fetchall()[0][0]

    query = ("select sum(" + site_name_flows + ".packets) from " + site_name_flowsets + " inner join " + site_name_flows + " on " + site_name_flowsets +".id=" + site_name_flows + ".flowset_id where " + site_name_flows + ".priority=\"Enhanced +\" and " + site_name_flowsets + ".frame_epoch > " + min_seconds + " and " + site_name_flowsets + ".frame_epoch < " + max_seconds + " ")

    cursor.execute(query)
    enhancedplustotalpackets = cursor.fetchall()[0][0]

    query = ("select sum(" + site_name_flows + ".packets) from " + site_name_flowsets + " inner join " + site_name_flows + " on " + site_name_flowsets +".id=" + site_name_flows + ".flowset_id where " + site_name_flows + ".priority=\"Premium\" and " + site_name_flowsets + ".frame_epoch > " + min_seconds + " and " + site_name_flowsets + ".frame_epoch < " + max_seconds + " ")

    cursor.execute(query)
    premiumtotalpackets = cursor.fetchall()[0][0]

    query = ("select sum(" + site_name_flows + ".packets) from " + site_name_flowsets + " inner join " + site_name_flows + " on " + site_name_flowsets +".id=" + site_name_flows + ".flowset_id where " + site_name_flows + ".priority=\"Premium +\" and " + site_name_flowsets + ".frame_epoch > " + min_seconds + " and " + site_name_flowsets + ".frame_epoch < " + max_seconds + " ")

    cursor.execute(query)
    premiumplustotalpackets = cursor.fetchall()[0][0]




    query = ("select sum(" + site_name_flows + ".bytes) from " + site_name_flowsets + " inner join " + site_name_flows + " on " + site_name_flowsets +".id=" + site_name_flows + ".flowset_id where " + site_name_flows + ".priority=\"Basic\" and " + site_name_flowsets + ".frame_epoch > " + min_seconds + " and " + site_name_flowsets + ".frame_epoch < " + max_seconds + " ")

    cursor.execute(query)
    basictotalbytes = float(int(cursor.fetchall()[0][0])/1000000)

    query = ("select sum(" + site_name_flows + ".bytes) from " + site_name_flowsets + " inner join " + site_name_flows + " on " + site_name_flowsets +".id=" + site_name_flows + ".flowset_id where " + site_name_flows + ".priority=\"Basic +\" and " + site_name_flowsets + ".frame_epoch > " + min_seconds + " and " + site_name_flowsets + ".frame_epoch < " + max_seconds + " ")

    cursor.execute(query)
    basicplustotalbytes = float(int(cursor.fetchall()[0][0])/1000000)

    query = ("select sum(" + site_name_flows + ".bytes) from " + site_name_flowsets + " inner join " + site_name_flows + " on " + site_name_flowsets +".id=" + site_name_flows + ".flowset_id where " + site_name_flows + ".priority=\"Enhanced\" and " + site_name_flowsets + ".frame_epoch > " + min_seconds + " and " + site_name_flowsets + ".frame_epoch < " + max_seconds + " ")

    cursor.execute(query)
    enhancedtotalbytes = float(int(cursor.fetchall()[0][0])/1000000)

    query = ("select sum(" + site_name_flows + ".bytes) from " + site_name_flowsets + " inner join " + site_name_flows + " on " + site_name_flowsets +".id=" + site_name_flows + ".flowset_id where " + site_name_flows + ".priority=\"Enhanced +\" and " + site_name_flowsets + ".frame_epoch > " + min_seconds + " and " + site_name_flowsets + ".frame_epoch < " + max_seconds + " ")

    cursor.execute(query)
    enhancedplustotalbytes = float(int(cursor.fetchall()[0][0])/1000000)

    query = ("select sum(" + site_name_flows + ".bytes) from " + site_name_flowsets + " inner join " + site_name_flows + " on " + site_name_flowsets +".id=" + site_name_flows + ".flowset_id where " + site_name_flows + ".priority=\"Premium\" and " + site_name_flowsets + ".frame_epoch > " + min_seconds + " and " + site_name_flowsets + ".frame_epoch < " + max_seconds + " ")

    # cursor.execute(query)
    # if cursor.fetchall()[0][0] == None:
    #     val = 0
    # else:
    #     val = cursor.fetchall()[0][0]
    # premiumtotalbytes = float(int(val)/1000000)

    query = ("select sum(" + site_name_flows + ".bytes) from " + site_name_flowsets + " inner join " + site_name_flows + " on " + site_name_flowsets +".id=" + site_name_flows + ".flowset_id where " + site_name_flows + ".priority=\"Premium +\" and " + site_name_flowsets + ".frame_epoch > " + min_seconds + " and " + site_name_flowsets + ".frame_epoch < " + max_seconds + " ")

    cursor.execute(query)
    premiumplustotalbytes = float(int(cursor.fetchall()[0][0])/1000000) 





    query = ("select count(*) from " + site_name_flowsets + " inner join " + site_name_flows + " on " + site_name_flowsets +".id=" + site_name_flows + ".flowset_id where " + site_name_flows + ".priority=\"Basic\" and " + site_name_flowsets + ".frame_epoch > " + min_seconds + " and " + site_name_flowsets + ".frame_epoch < " + max_seconds + " ")

    cursor.execute(query)
    basictotalflows = cursor.fetchall()[0][0]

    query = ("select count(*) from " + site_name_flowsets + " inner join " + site_name_flows + " on " + site_name_flowsets +".id=" + site_name_flows + ".flowset_id where " + site_name_flows + ".priority=\"Basic +\" and " + site_name_flowsets + ".frame_epoch > " + min_seconds + " and " + site_name_flowsets + ".frame_epoch < " + max_seconds + " ")

    cursor.execute(query)
    basicplustotalflows = cursor.fetchall()[0][0]

    query = ("select count(*) from " + site_name_flowsets + " inner join " + site_name_flows + " on " + site_name_flowsets +".id=" + site_name_flows + ".flowset_id where " + site_name_flows + ".priority=\"Enhanced\" and " + site_name_flowsets + ".frame_epoch > " + min_seconds + " and " + site_name_flowsets + ".frame_epoch < " + max_seconds + " ")

    cursor.execute(query)
    enhancedtotalflows = cursor.fetchall()[0][0]

    query = ("select count(*) from " + site_name_flowsets + " inner join " + site_name_flows + " on " + site_name_flowsets +".id=" + site_name_flows + ".flowset_id where " + site_name_flows + ".priority=\"Enhanced +\" and " + site_name_flowsets + ".frame_epoch > " + min_seconds + " and " + site_name_flowsets + ".frame_epoch < " + max_seconds + " ")

    cursor.execute(query)
    enhancedplustotalflows = cursor.fetchall()[0][0]

    query = ("select count(*) from " + site_name_flowsets + " inner join " + site_name_flows + " on " + site_name_flowsets +".id=" + site_name_flows + ".flowset_id where " + site_name_flows + ".priority=\"Premium\" and " + site_name_flowsets + ".frame_epoch > " + min_seconds + " and " + site_name_flowsets + ".frame_epoch < " + max_seconds + " ")

    cursor.execute(query)
    premiumtotalflows = cursor.fetchall()[0][0]

    query = ("select count(*) from " + site_name_flowsets + " inner join " + site_name_flows + " on " + site_name_flowsets +".id=" + site_name_flows + ".flowset_id where " + site_name_flows + ".priority=\"Premium +\" and " + site_name_flowsets + ".frame_epoch > " + min_seconds + " and " + site_name_flowsets + ".frame_epoch < " + max_seconds + " ")

    cursor.execute(query)
    premiumplustotalflows = cursor.fetchall()[0][0]




    with open(tabledatafn, "w", newline='') as f:
        writer = csv.writer(f)
        writer.writerow(["row_name", "priority","total_flows","total_bytes","average_bitrate","average_packetrate","peak_bitrate","peak_packetrate","total_packets"])

        writer.writerow([1,"premium+",premiumplustotalflows,premiumplustotalbytes,ppavgbr,ppavgpr,maxbrpp,maxprpp,premiumplustotalpackets])    
        # writer.writerow([2,"premium",premiumtotalflows,premiumtotalbytes,pavgbr,pavgpr,maxbrp,maxprp,premiumtotalpackets])    
        writer.writerow([3,"enhanced+",enhancedplustotalflows,enhancedplustotalbytes,epavgbr,epavgpr,maxbrep,maxprep,enhancedplustotalpackets])    
        writer.writerow([4,"enhanced",enhancedtotalflows,enhancedtotalbytes,eavgbr,eavgpr,maxbre,maxpre,enhancedtotalpackets])    
        writer.writerow([5,"basic+",basicplustotalflows,basicplustotalbytes,bpavgbr,bpavgpr,maxbrbp,maxprbp,basicplustotalpackets])    
        writer.writerow([6,"basic",basictotalflows,basictotalbytes,bavgbr,bavgpr,maxbrb,maxprb,basictotalpackets])    


    f.close() 








    # os.mkdir(path + "\\DataTableData")

    # bfn = path + "\\DataTableData\\" + site_name + "BasicDataTableData6SecondSlices.csv"
    with open("DataTableData/Basic.csv", "w", newline='') as f:
        writer = csv.writer(f)
        writer.writerow(["row_name","priority", "total_flows", "total_bytes", "avg_bitrate(Mb/second)", "avg_packetrate(packets/second)", "peak_bitrate(Mb/second)", "peak_packetrate(packets/second)", "total_packets"])
        
        for i in range(len(dtb)):
            writer.writerow([i] + dtb[i])
    f.close()   

    # bpfn = path + "\\DataTableData\\" + site_name + "Basic+DataTableData6SecondSlices.csv"
    with open("DataTableData/Basic+.csv", "w", newline='') as f:
        writer = csv.writer(f)
        writer.writerow(["row_name","priority", "total_flows", "total_bytes", "avg_bitrate(Mb/second)", "avg_packetrate(packets/second)", "peak_bitrate(Mb/second)", "peak_packetrate(packets/second)", "total_packets"])
        
        for i in range(len(dtbp)):
            writer.writerow([i] + dtbp[i])

    f.close()   

    # efn = path + "\\DataTableData\\" + site_name + "EnhancedDataTableData6SecondSlices.csv"
    with open("DataTableData/Enhanced.csv", "w", newline='') as f:
        writer = csv.writer(f)
        writer.writerow(["row_name","priority", "total_flows", "total_bytes", "avg_bitrate(Mb/second)", "avg_packetrate(packets/second)", "peak_bitrate(Mb/second)", "peak_packetrate(packets/second)", "total_packets"])
        
        for i in range(len(dte)):
            writer.writerow([i] + dte[i])

    f.close()   

    # epfn = path + "\\DataTableData\\" + site_name + "Enhanced+DataTableData6SecondSlices.csv"
    with open("DataTableData/Enhanced+.csv", "w", newline='') as f:
        writer = csv.writer(f)
        writer.writerow(["row_name","priority", "total_flows", "total_bytes", "avg_bitrate(Mb/second)", "avg_packetrate(packets/second)", "peak_bitrate(Mb/second)", "peak_packetrate(packets/second)", "total_packets"])
        
        for i in range(len(dtep)):
            writer.writerow([i] + dtep[i])

    f.close()  

    # pfn = path + "\\DataTableData\\" + site_name + "PremiumDataTableData6SecondSlices.csv"
    with open("DataTableData/Premium.csv", "w", newline='') as f:
        writer = csv.writer(f)
        writer.writerow(["row_name","priority", "total_flows", "total_bytes", "avg_bitrate(Mb/second)", "avg_packetrate(packets/second)", "peak_bitrate(Mb/second)", "peak_packetrate(packets/second)", "total_packets"])
        
        for i in range(len(dtp)):
            writer.writerow([i] + dtp[i])

    f.close()   

    # ppfn = path + "\\DataTableData\\" + site_name + "Premium+DataTableData6SecondSlices.csv"
    with open("DataTableData/Premium+.csv", "w", newline='') as f:
        writer = csv.writer(f)
        writer.writerow(["row_name","priority", "total_flows", "total_bytes", "avg_bitrate(Mb/second)", "avg_packetrate(packets/second)", "peak_bitrate(Mb/second)", "peak_packetrate(packets/second)", "total_packets"])
        
        for i in range(len(dtpp)):
            writer.writerow([i] + dtpp[i])

    f.close()  















    # with open("SpringhillGraphData3SecondSlicesBySlice.csv", "w", newline='') as f:
    #     writer = csv.writer(f)
    #     writer.writerow(["priority", "start_time", "end_time", "bitrate(bits/second)", "packetrate(packets/second)"])
        
    #     for i in range(len(pp)):
    #         writer.writerow(pp[i])
    #         writer.writerow(p[i])
    #         writer.writerow(ep[i])
    #         writer.writerow(e[i])
    #         writer.writerow(bp[i])
    #         writer.writerow(b[i])

    # f.close()  


    # cursor.execute(query)
    # slice1 = cursor.fetchall()
    # cnx.commit()   
    # print(len(slice1))
        # Get data between start and end
        # Get bitrate, get packetrate, append to list for each priority



    # getFullFlowSetData Example Call
    # data = getFullFlowSetData("PCAP Data/springhill.json", "Application")
    # for k,v in data.items():
    #     print(k, ':', v)

    # getUniqueIPs Example Call
    # srcIPs, destIPs = getUniqueIPs("PCAP Data/initialPCAPdata.json", True)

    # getFlowsets Example Call
    # flowsetData, flowsData = getFlowsets("PCAP Data/springhill.json", 1)

    # writeFlowsetsAndFlowsToCSV Example Call
    # writeFlowsetsAndFlowsToCSV("PCAP Data/springhill.json", "CSV Data/springhillFlowsets.csv", "CSV Data/springhillFlows.csv", 1)
    
    # insertFlows Example Call
    # insertFlows("CSV Data/springhillFlows.csv")

    # insertFlowsets Example Call
    # insertFlowsets("CSV Data/springhillFlowsets.csv")

    return

def getUniqueIPs(JSONfilename: str, pathToNewFile: str):
    """
    Get all flowset data from a file.
    
    params:
        JSONfilename : str
            The name of the json file to extract data from
        writeToText : bool
            Whether or not to write the IPs to text files in this directory

    return:
        uniqueSourceIPs : set (unordered list)
            A list of unique source IPs
        uniqueDestinationIPs : set (unordered list)
            A list of unique destination IPs
    """

    # Initialize return data
    srcips = []
    destips = []
    
    # Read in file
    with open(str(JSONfilename)) as f:
        data = json.loads(f.read())
        
    # Iterate through each flowset -> flow, and add the ips to the lists
    for FullFlowSetData in data:
        flowSet = FullFlowSetData["_source"]["layers"]["cflow"]
        for key in flowSet.keys():
            if key[0] == 'S' or key[0] == 'F' and key[-1] != ')':
                flowSetKey = key

        flows = flowSet[flowSetKey]
        for flowKey, flowVals in flows.items():
            if flowKey[0] == 'F':
                srcips.append(flowVals["cflow.srcaddr"])
                destips.append(flowVals["cflow.dstaddr"])   
    
    # Remove duplicate IPs
    uniqueSourceIPs = set(srcips)
    uniqueDestinationIPs = set(destips)
    
    # Write unique IPs to text files as well
    newSourceFilePath = str(pathToNewFile) + '\\uniqueSourceIPs.txt'
    newDestinationFilePath = str(pathToNewFile) + '\\uniqueDestinationIPs.txt'

    with open(newSourceFilePath, 'w') as src:
        for ip in uniqueSourceIPs:
            src.write(ip)
            src.write("\n")

    with open(newDestinationFilePath, 'w') as dst:
        for ip in uniqueDestinationIPs:
            dst.write(ip)
            dst.write("\n")

    return uniqueSourceIPs, uniqueDestinationIPs

def main():

    generateGraphData("Springhill", 1674018001, "C:\\Users\\nwang\\Desktop")
    return

    v = StringVar(master)
    introText = Label(master, text="Choose what you'd like to do from the dropdown menu below", font=("Helvetica bold", 12))
    introText.pack(ipady=10)
    
    combobox = ttk.Combobox(master, textvariable=v, state="readonly")
    combobox['values'] = ("Generate the files necessary for displaying graph data","Insert all flows and flowsets from a PCAP json file into the database",  "Get a list of unique source and destination IPs from a PCAP json file")
    combobox.pack(fill=tk.X, padx=50, pady=5)

    def comboboxCB(event):
        
        choice = combobox.get()

        # Graph data generation
        if choice == combobox['values'][0]:
            # Generate csvs for graphing
            lab = Label(master, text="Make sure you're connected to the vpn before doing this\nWhich site do you want to get the data from?")
            lab.pack(ipady=5)

            def graphWrapper():

                messagebox.showinfo(message="Select the place you want the files to be stored")
                destination = filedialog.askdirectory()
                datetime1 = cal.get_date()
                dt1 = datetime.datetime.combine(datetime1, datetime.datetime.strptime('00:00', '%H:%M').time())
                date_epochtime = calendar.timegm(dt1.timetuple())
                print("you selected " + str(date_epochtime))

                if destination:
                    destination_path = str(os.path.abspath(destination))        
                    dst_entry = tk.Label(master, text="JSON File Path: " + destination_path)
                    dst_entry.pack()  

                if var.get() == "Springhill":
                    generateGraphData("Springhill", date_epochtime, destination_path)
                if var.get() == "Manila":
                    generateGraphData("Manila", date_epochtime, destination_path)

   

            var = StringVar()

            springhillRadio = Radiobutton(master, text = "Springhill", variable = var, value = "Springhill", command=graphWrapper)
            manilaRadio = Radiobutton(master, text="Manila", variable = var, value = "Manila", command=graphWrapper)
            springhillRadio.pack()
            manilaRadio.pack()
            lab2 = Label(master, text="Choose a date to get the graph data from\n(Currently only 1/18/23 for Springhill, and 2/2/23 for Manila )")
            lab2.pack()
            date = StringVar()

            cal = DateEntry(master, width=16, variable=date, command=graphWrapper)
            cal.pack()
            pass

        # Insert flows and flowsets
        if choice == combobox['values'][1]:
            
            lab = Label(master, text="Make sure you're connected to the vpn before doing this\nWhich site do you want to insert into?")
            lab.pack(ipady=5)

            def insertionWrapper():

                messagebox.showinfo(message="Select the exported PCAP file in .json format")

                source = filedialog.askopenfile(mode='r', filetypes=[('Json Files','*.json')])
                print("you selected" + var.get())
                if source:
                    source_path = str(os.path.abspath(source.name))        
                    src_entry = tk.Label(master, text="JSON File Path: " + source_path)
                    src_entry.pack()  

                if var.get() == "Springhill":
                    springhill.writeFlowsetsAndFlowsToCSV(source_path, siteId=1)
                    springhill.insertFlowsets('flowsets.csv')
                    springhill.insertFlows('flows.csv')
                    os.remove("flows.csv")
                    os.remove("flowsets.csv")
                if var.get() == "Manila":
                    manila.writeFlowsetsAndFlowsToCSV(source_path, siteId=2)
                    # manila.insertFlows('flows.csv')
                    # manila.insertFlowsets('flowsets.csv')
                    # os.remove("flows.csv")
                    # os.remove("flowsets.csv")        
                if var.get() == "unknown":
                    # unknown.writeFlowsetsAndFlowsToCSV(source_path, siteId=-1)
                    pass
                    # manila.insertFlows('flows.csv')
                    # manila.insertFlowsets('flowsets.csv')
                    # os.remove("flows.csv")
                    # os.remove("flowsets.csv")     

            var = StringVar()

            springhillRadio = Radiobutton(master, text = "Springhill", variable = var, value = "Springhill", command=insertionWrapper)
            manilaRadio = Radiobutton(master, text="Manila", variable = var, value = "Manila", command=insertionWrapper)
            unknownRadio = Radiobutton(master, text="Unknown", variable = var, value = "Unknown", command=insertionWrapper)

            springhillRadio.pack()
            manilaRadio.pack()


        # Get source/dst IPs
        if choice == combobox['values'][2]:
            # get path to exported PCAP JSON file
            messagebox.showinfo(message="Select the exported PCAP file in .json format")
            source = filedialog.askopenfile(mode='r', filetypes=[('Json Files','*.json')])
            if source:
                source_path = str(os.path.abspath(source.name))        
                src_entry = tk.Label(master, text="JSON File Path: " + source_path)
                src_entry.pack()

            # get path to desired folder
            messagebox.showinfo(message="Select the directory to save the IPs to")
            destination = filedialog.askdirectory()
            if destination:
                destination_path = str(os.path.abspath(destination))
                destination_label = tk.Label(master, text="Output Destination: " + destination_path)
                destination_label.pack()
            # call getUniqueIPs(json path, newpath)
            start = Button(master, text="Go", command=getUniqueIPs(source_path, destination_path))
            start.pack()
               
    
    combobox.bind('<<ComboboxSelected>>', comboboxCB)

    mainloop()


if __name__ == "__main__":
    main()
