import json
import time
import csv
import mysql.connector
import math

def getFlowsetDataCalculations(JSONfilename: str, view: str):
    """
    OUTDATED
    Get all flowset data from a file.
    
    params:
        JSONfilename : str
            The name of the json file to extract data from
        view : str
            The type of view data to return | "Application", "etc..."

    return:
        data : dict
            All flowset data in dictionary form
    """
    
    # Initialize all data variables
    applications = []
    totalFlows = 0
    totalBytes = 0
    totalPackets = 0
    totalTimeElapsed = 0
    averageBitRate = 0
    averagePacketRate = 0
    peakBitRate = "WIP"
    peakPacketRate = "WIP"
    
    # Get the time the data processing started
    start = time.time()

    # Read in data from the file
    with open(JSONfilename) as f:
        data = json.loads(f.read())

    # Iterate through each flowset -> flow, and calculate all the necessary values
    for FullFlowSetData in data:
        flowSet = FullFlowSetData["_source"]["layers"]["cflow"]
        for key in flowSet.keys():
            if key[0] == 'F':
                flowSetKey = key
                
        flows = flowSet[flowSetKey]
        numFlows = len(flows) - 4
        totalFlows += numFlows

        for flowKey, flowVals in flows.items():
            if flowKey[0] == 'F':
                totalBytes += int(flowVals["cflow.octets"])
                totalPackets += int(flowVals["cflow.packets"])
                totalTimeElapsed += float(flowVals["cflow.timedelta"])


    # Bytes per second
    averageBitRate = (totalBytes / ((totalTimeElapsed / 60) * .0075)) / 2

    # Packets per second
    averagePacketRate = (totalPackets / ((totalTimeElapsed))) / 2

    # Get the real time elapsed during processing
    end = time.time()
    realTime = end-start

    # Return dictionary with all values calculated
    allData = {
        "View": view,
        "Applications": applications,
        "Total Flows": totalFlows,
        "Total Bytes:": totalBytes,
        "Total Packets": totalPackets,
        "Average BitRate": averageBitRate,
        "Average Packet Rate": averagePacketRate,
        "Peak BitRate": peakBitRate,
        "Peak Packet Rate": peakPacketRate,
        "Total Time Elapsed": totalTimeElapsed,
        "Real World Time Elapsed": realTime
    }

    return allData


def getUniqueIPs(JSONfilename: str, writeToText: bool):
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
    with open(JSONfilename) as f:
        data = json.loads(f.read())

    # Iterate through each flowset -> flow, and add the ips to the lists
    for FullFlowSetData in data:
        flowSet = FullFlowSetData["_source"]["layers"]["cflow"]
        for key in flowSet.keys():
            if key[0] == 'F':
                flowSetKey = key
                
        flows = flowSet[flowSetKey]

        for flowKey, flowVals in flows.items():
            if flowKey[0] == 'F':
                srcips.append(flowVals["cflow.srcaddr"])
                destips.append(flowVals["cflow.dstaddr"])   
    
    # Remove duplicate IPs
    uniqueSourceIPs = set(srcips)
    uniqueDestinationIPs = set(destips)
    
    # If desired, write unique IPs to text files as well
    if writeToText:
        with open('uniqueSourceIPs.txt', 'w') as src:
            for ip in uniqueSourceIPs:
                src.write(ip)
                src.write("\n")

        with open('uniqueDestinationIPs.txt', 'w') as dst:
            for ip in uniqueDestinationIPs:
                dst.write(ip)
                dst.write("\n")

    return uniqueSourceIPs, uniqueDestinationIPs


def getFlowsetsAndFlows(JSONfilename: str, siteId: int):
    """
    Get all flows and flowsets from a JSON file
    
    params:
        JSONfilename : str
            The name of the json file to extract data from
        siteId : int
            The id of the site PCAP data

    return:
        data : list[dict]
            An array of all flowsets' data for use in filling tables
    """
    
    # Initialize return variable
    flowsetsTableData = []
    flowsTableData = []

    # Mapping dscp_marking to priority info
    ip_dscp_dict = {
        46: ["Unknown", "Unknown"],
        47: ["Voice","Premium +"],
        48: ["Voice","Premium +"],
        34: ["Interactive Video", "Premium"],
        26: ["Streaming Control", "Enhanced +"],
        24: ["Streaming Control", "Enhanced +"],
        18: ["Normal Business", "Enhanced"],
        0: ["Default", "Basic +"],
        10: ["Scavenger", "Basic"],
        12: ["Scavenger", "Basic"],
    }

    # Establish server connection
    cnxFlows = mysql.connector.connect(host='35.9.22.102', user='root', password='VRRocks', database='gm')
    cursor = cnxFlows.cursor()

    query = (
        "select flowset_id from springhillFlows where flowset_id=(select max(flowset_id) from springhillFlows) limit 1"
    )

    cursor.execute(query)
    starting_flowset_id = cursor.fetchall()[0][0] + 1

    # Read in data from the file
    with open(JSONfilename) as f:
        data = json.loads(f.read())

    flowSetUniqueIdCounter = 1

    # Iterate through each flowset, getting all values for the table
    for FullFlowSetData in data:
        index = FullFlowSetData["_index"]
        
        frameTime = FullFlowSetData["_source"]["layers"]["frame"]["frame.time"]
        frameEpoch = FullFlowSetData["_source"]["layers"]["frame"]["frame.time_epoch"]
        frameLen = FullFlowSetData["_source"]["layers"]["frame"]["frame.len"]
        
        ethDstOuiResolve = FullFlowSetData["_source"]["layers"]["eth"]["eth.dst_tree"]["eth.dst.oui_resolved"]
        ethDstAddrOuiResolve = FullFlowSetData["_source"]["layers"]["eth"]["eth.dst_tree"]["eth.addr.oui_resolved"]
        ethSrcOuiResolve = FullFlowSetData["_source"]["layers"]["eth"]["eth.src_tree"]["eth.src.oui_resolved"]
        ethSrcAddrOuiResolve = FullFlowSetData["_source"]["layers"]["eth"]["eth.src_tree"]["eth.addr.oui_resolved"]
        
        ipSrc = FullFlowSetData["_source"]["layers"]["ip"]["ip.src"]
        ipSrcHost = FullFlowSetData["_source"]["layers"]["ip"]["ip.src_host"]
        ipDst = FullFlowSetData["_source"]["layers"]["ip"]["ip.dst"]
        ipDstHost = FullFlowSetData["_source"]["layers"]["ip"]["ip.dst_host"]

        flowSet = FullFlowSetData["_source"]["layers"]["cflow"]
        for key in flowSet.keys():
            if key[0] != 'c':
                flowsetNumber = key
        flowsetLength = FullFlowSetData["_source"]["layers"]["cflow"][flowsetNumber]["cflow.flowset_length"]
        
        flows = flowSet[flowsetNumber]
        numFlows = len(flowSet[flowsetNumber]) - 3
        
        oneFlowsetData = {
            "siteId": siteId,
            "index": index,
            "frameTime": frameTime,
            "frameEpoch": frameEpoch,
            "frameLen": frameLen,
            "ethDstOuiResolve":ethDstOuiResolve,
            "ethDstAddrOuiResolve": ethDstAddrOuiResolve,
            "ethSrcOuiResolve": ethSrcOuiResolve,
            "ethSrcAddrOuiResolve": ethSrcAddrOuiResolve,
            "ipSrc": ipSrc,
            "ipSrcHost": ipSrcHost,
            "ipDst": ipDst,
            "ipDstHost": ipDstHost,
            "flowsetName": flowsetNumber,
            "numFlows": numFlows,
            "flowsetLength": flowsetLength,
            "flowsetUniqueId": flowSetUniqueIdCounter
        }
        flowsetsTableData.append(oneFlowsetData)

        # Iterate through each flow, appending the data to the return list
        for flowKey, flowVals in flows.items():
            if flowKey[0] != 'c' and flowKey[0] != 'T':

                ip_dscp = int(flowVals["cflow.ip_dscp"])
                type_dscp = ip_dscp_dict[ip_dscp][0]
                priority = ip_dscp_dict[ip_dscp][1]

                oneFlow = {
                    "uniqueFlowsetId": flowSetUniqueIdCounter,
                    "flowNumber": int(flowKey[5:]),
                    "srcAddr": (flowVals["cflow.srcaddr"]),
                    "dstAddr": (flowVals["cflow.dstaddr"]),
                    "srcPort": (flowVals["cflow.srcport"]),
                    "dstPort": (flowVals["cflow.dstport"]),
                    "forwardingStatus": 'null',
                    "forwardingStatusCode": 'null',
                    "nextHop": (flowVals["cflow.nexthop"]),
                    "outputInt": (flowVals["cflow.outputint"]),
                    "outputDirection": 'null',
                    "bytes": int(flowVals["cflow.octets"]),
                    "packets": int(flowVals["cflow.packets"]),
                    "timeDelta": float(flowVals["cflow.timedelta"]),
                    "ip_dscp": ip_dscp,
                    "type_dscp": type_dscp,
                    "priority": priority
                }

                flowsTableData.append(oneFlow)

        flowSetUniqueIdCounter += 1

    return flowsetsTableData, flowsTableData


def writeFlowsetsAndFlowsToCSV(JSONfilename: str, siteId: int, flowsetsOutputFileName="flowsets.csv", flowsOutputFileName="flows.csv"):
    """
    Write the flowset and flows data to CSV for insertion into DB tables
    
    params:
        JSONfilename : str
            The name of the json file to extract data from
        flowsetsOutputFileName : str
            The name of the file to write flowsets to
        flowsOutputFileName : str
            The name of the file to write flows to
        siteId : int
            The id of the site, in this case springhill is "1"
    """    

    # Get flowset/flows data    
    flowsetsTableData, flowsTableData = getFlowsetsAndFlows(JSONfilename, siteId)

    with open(flowsetsOutputFileName, "w", newline='') as f:
        writer = csv.writer(f)
        writer.writerow(["flowsetUniqueId", "siteId", "flowsetName", "index", "frameTime", "frameEpoch", "frameLen", "ethDstOuiResolve", "ethDstAddrOuiResolve", "ethSrcOuiResolve", "ethSrcAddrOuiResolve", "ipSrc", "ipSrcHost", "ipDst", "ipDstHost", "numFlows", "flowsetLength"])
        
        for i in range(len(flowsetsTableData)):
            flowset = flowsetsTableData[i]
            writer.writerow([flowset["flowsetUniqueId"], flowset["siteId"], flowset["flowsetName"], flowset["index"], flowset["frameTime"], flowset["frameEpoch"], flowset["frameLen"], flowset["ethDstOuiResolve"], 
                             flowset["ethDstAddrOuiResolve"], flowset["ethSrcOuiResolve"], flowset["ethSrcAddrOuiResolve"], flowset["ipSrc"], flowset["ipSrcHost"], flowset["ipDst"], 
                             flowset["ipDstHost"], flowset["numFlows"], flowset["flowsetLength"]])

    f.close()    

    # Write the data to flows CSV file
    with open(flowsOutputFileName, "w", newline='') as f:
        writer = csv.writer(f)
        writer.writerow(["uniqueFlowsetId","flowNumber", "srcAddr", "dstAddr", "srcPort", "dstPort", "forwardingStatus", "forwardingStatusCode", "nextHop", "outputInt", "outputDirection", "bytes", "packets", "timeDelta", "ip_dscp", "type_dscp", "priority"])
        
        for i in range(len(flowsTableData)):
            flow = flowsTableData[i]
            writer.writerow([flow["uniqueFlowsetId"], flow["flowNumber"], flow["srcAddr"], flow["dstAddr"], flow["srcPort"], flow["dstPort"], 
                             flow["forwardingStatus"], flow["forwardingStatusCode"], flow["nextHop"], flow["outputInt"], flow["outputDirection"], flow["bytes"], 
                             flow["packets"], flow["timeDelta"], flow["ip_dscp"], flow["type_dscp"], flow["priority"]])

    f.close()   


def insertFlows(pathToFlowsAsCSV : str):
    """
    Insert flows into the table from a CSV file
    
    params:
        pathToFlowsAsCSV : str
            the file path to the file to insert
    """

    # Establish server connection
    cnxFlows = mysql.connector.connect(host='35.9.22.102', user='root', password='VRRocks', database='gm')
    cursor = cnxFlows.cursor()

    # Open and read the file line by line
    with open(pathToFlowsAsCSV) as f:
        reader = csv.reader(f)

        first = True

        # Iterate through each line
        for row in reader:
            if not first:
                row[0] = int(row[0])

                query = (
                    "INSERT INTO manilaFlows (flowset_id, flow_number, src_addr, dst_addr, src_port, dst_port, forwarding_status, forwarding_status_code, next_hop, output_int, output_direction, bytes, packets, time_delta, ip_dscp, type_dscp, priority) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                )   

                cursor.execute(query, row)
                cnxFlows.commit()
            first = False

    f.close()
    return


def insertFlowsets(pathToFlowsetsAsCSV : str):
    """
    Insert flowsets into the table from a CSV file
    
    params:
        pathToFlowsetsAsCSV : str
            the file path to the file to insert
    """   

    # Establish server connection
    cnxFlows = mysql.connector.connect(host='35.9.22.102', user='root', password='VRRocks', database='gm')
    cursor = cnxFlows.cursor()

    # Open and read the file line by line
    with open(pathToFlowsetsAsCSV) as f:
        reader = csv.reader(f)

        first = True

        # Iterate through each line
        for row in reader:
            if not first:
                row[0] = int(row[0])

                query = (
                    "INSERT INTO manilaFlows (site_id, flowset_number, indx, frame_time, frame_epoch, frame_len, eth_dst_oui_resolve, eth_src_oui_resolve, ip_src, ip_src_host, ip_dst, ip_dst_host, num_flows, flowset_length) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                )   

                cursor.execute(query, row)
                cnxFlows.commit()

            first = False

    f.close()
    return


def main():

    # Connect to the database example
    # cnx = mysql.connector.connect(host='35.9.22.102', user='root', password='VRRocks', database='gm')
    # cursor = cnx.cursor()

    # getFullFlowSetData Example Call
    # data = getFullFlowSetData("PCAP Data/manila.json", "Application")
    # for k,v in data.items():
    #     print(k, ':', v)

    # getUniqueIPs Example Call
    # srcIPs, destIPs = getUniqueIPs("PCAP Data/manila.json", True)

    # getFlowsets Example Call
    # flowsetData, flowsData = getFlowsets("PCAP Data/manila.json", 1)

    # writeFlowsetsAndFlowsToCSV Example Call
    # writeFlowsetsAndFlowsToCSV("PCAP Data/manila.json", "CSV Data/manilaFlowsets.csv", "CSV Data/manilaFlows.csv", 1)
    
    # insertFlows Example Call
    # insertFlows("CSV Data/manilaFlows.csv")

    # insertFlowsets Example Call
    # insertFlowsets("CSV Data/manilaFlowsets.csv")

    # Connect to the database example
    cnx = mysql.connector.connect(host='35.9.22.102', user='root', password='VRRocks', database='gm')
    cursor = cnx.cursor()

    query = (
            "select min(frame_epoch) from manilaFlowsets"
            )   

    cursor.execute(query)
    min = cursor.fetchall()
    cnx.commit()

    query = (
            "select max(frame_epoch) from manilaFlowsets"
            )   

    cursor.execute(query)
    max = cursor.fetchall()
    cnx.commit()    

    min_seconds = math.floor(float(min[0][0]))
    max_seconds = math.ceil(float(max[0][0]))

    time_slices = []
    temp = min_seconds
    while temp <= max_seconds:
        time_slices.append(temp)
        temp += 60

    time_slices.append(max_seconds)

    pp = []
    p = []
    ep = []
    e = []
    bp = []
    b = []

    start_id = -1

    for i in range(len(time_slices) - 1):
        start_time = time_slices[i]
        end_time = time_slices[i+1]        
        # print(start_time, end_time)

        query = (
                "select id from manilaFlowsets where frame_epoch >= %s and frame_epoch < %s"
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
                "select sum(bytes) from manilaFlows where flowset_id > %s and flowset_id <= %s and priority= %s"
                )
        
        cursor.execute(ppbytes, (start_id, end_id, "Premium +"))
        res = cursor.fetchall()[0][0]
        if res == None:
            res = 0
        ppbr = float((res * 8) / (end_time - start_time))

        pppackets = (
                "select sum(packets) from manilaFlows where flowset_id > %s and flowset_id <= %s and priority = %s"
                )
    
        cursor.execute(pppackets, (start_id, end_id, "Premium +"))
        res = cursor.fetchall()[0][0]
        if res == None:
            res = 0
        pppr = float(res / (end_time - start_time))

        #######################




        ### PREMIUM DATA ###

        pbytes = (
                "select sum(bytes) from manilaFlows where flowset_id > %s and flowset_id <= %s and priority= %s"
                )
        
        cursor.execute(pbytes, (start_id, end_id, "Premium"))
        res = cursor.fetchall()[0][0]
        if res == None:
            res = 0
        pbr = float((res * 8) / (end_time - start_time))

        ppackets = (
                "select sum(packets) from manilaFlows where flowset_id > %s and flowset_id <= %s and priority = %s"
                )
    
        cursor.execute(ppackets, (start_id, end_id, "Premium"))
        res = cursor.fetchall()[0][0]
        if res == None:
            res = 0
        ppr = float((res) / (end_time - start_time))

        ######################





        ### ENHANCED + DATA ###

        epbytes = (
                "select sum(bytes) from manilaFlows where flowset_id > %s and flowset_id <= %s and priority= %s"
                )
        
        cursor.execute(epbytes, (start_id, end_id, "Enhanced +"))
        res = cursor.fetchall()[0][0]
        if res == None:
            res = 0
        epbr = float((res * 8) / (end_time - start_time))

        eppackets = (
                "select sum(packets) from manilaFlows where flowset_id > %s and flowset_id <= %s and priority = %s"
                )
    
        cursor.execute(eppackets, (start_id, end_id, "Enhanced +"))
        res = cursor.fetchall()[0][0]
        if res == None:
            res = 0
        eppr = float((res) / (end_time - start_time))

        ########################



        ### ENHANCED DATA ###

        ebytes = (
                "select sum(bytes) from manilaFlows where flowset_id > %s and flowset_id <= %s and priority= %s"
                )
        
        cursor.execute(ebytes, (start_id, end_id, "Enhanced"))
        res = cursor.fetchall()[0][0]
        if res == None:
            res = 0
        ebr = float((res * 8) / (end_time - start_time))

        epackets = (
                "select sum(packets) from manilaFlows where flowset_id > %s and flowset_id <= %s and priority = %s"
                )
    
        cursor.execute(epackets, (start_id, end_id, "Enhanced"))
        res = cursor.fetchall()[0][0]
        if res == None:
            res = 0
        epr = float((res) / (end_time - start_time))

        ######################



        ### BASIC + DATA ###

        bpbytes = (
                "select sum(bytes) from manilaFlows where flowset_id > %s and flowset_id <= %s and priority= %s"
                )
        
        cursor.execute(bpbytes, (start_id, end_id, "Basic +"))
        res = cursor.fetchall()[0][0]
        if res == None:
            res = 0
        bpbr = float((res * 8) / (end_time - start_time))

        bppackets = (
                "select sum(packets) from manilaFlows where flowset_id > %s and flowset_id <= %s and priority = %s"
                )
    
        cursor.execute(bppackets, (start_id, end_id, "Basic +"))
        res = cursor.fetchall()[0][0]
        if res == None:
            res = 0
        bppr = float((res) / (end_time - start_time))

        ####################


        ### BASIC DATA ###

        bbytes = (
                "select sum(bytes) from manilaFlows where flowset_id > %s and flowset_id <= %s and priority= %s"
                )
        
        cursor.execute(bbytes, (start_id, end_id, "Basic"))
        res = cursor.fetchall()[0][0]
        if res == None:
            res = 0
        bbr = float((res * 8) / (end_time - start_time))

        bpackets = (
                "select sum(packets) from manilaFlows where flowset_id > %s and flowset_id <= %s and priority = %s"
                )
    
        cursor.execute(bpackets, (start_id, end_id, "Basic"))
        res = cursor.fetchall()[0][0]
        if res == None:
            res = 0
        bpr = float((res) / (end_time - start_time))    

        ####################  



        pp.append(["Premium +", start_time, end_time, ppbr, pppr])
        p.append(["Premium",start_time, end_time, pbr, ppr])

        ep.append(["Enhanced +", start_time, end_time, epbr, eppr])
        e.append(["Enhanced",start_time, end_time, ebr, epr])

        bp.append(["Basic +", start_time, end_time, bpbr, bppr])
        b.append(["Basic", start_time, end_time, bbr, bpr])


    all_info_by_priority = pp + p + ep + e + bp + b
    for ele in all_info_by_priority:
        #print(ele)
        pass

    all_info_by_slice = []
    for i in range(len(pp)):
        all_info_by_slice.append(pp[i])
        all_info_by_slice.append(p[i])
        all_info_by_slice.append(ep[i])
        all_info_by_slice.append(e[i])
        all_info_by_slice.append(bp[i])
        all_info_by_slice.append(b[i])
    

    for ele in all_info_by_slice:
        print(ele)

    with open("SpringhillGraphData60SecondSlicesByPriority.csv", "w", newline='') as f:
        writer = csv.writer(f)
        writer.writerow(["priority", "start_time", "end_time", "bitrate(bits/second)", "packetrate(packets/second)"])
        
        for i in range(len(all_info_by_priority)):
            writer.writerow(all_info_by_priority[i])

    f.close()   

    with open("SpringhillGraphData60SecondSlicesBySlice.csv", "w", newline='') as f:
        writer = csv.writer(f)
        writer.writerow(["priority", "start_time", "end_time", "bitrate(bits/second)", "packetrate(packets/second)"])
        
        for i in range(len(pp)):
            writer.writerow(pp[i])
            writer.writerow(p[i])
            writer.writerow(ep[i])
            writer.writerow(e[i])
            writer.writerow(bp[i])
            writer.writerow(b[i])

    f.close()  

    # maxbrpp = -1
    # maxprpp = -1

    # maxbrp = -1
    # maxprp = -1    
    
    # maxbrep = -1
    # maxprep = -1

    # maxbre = -1
    # maxpre = -1

    # maxbrbp = -1
    # maxprbp = -1

    # maxbrb = -1
    # maxprb = -1

    # for i in range(len(pp)):
    #     brpp = pp[i][3]
    #     if brpp > maxbrpp:
    #         maxbrpp = brpp

    #     prpp = pp[i][4]
    #     if prpp > maxprpp:
    #         maxprpp = prpp


    #     brp = p[i][3]
    #     if brp > maxbrp:
    #         maxbrp = brp

    #     prp = p[i][4]
    #     if prp > maxprp:
    #         maxprp = prp


    #     brep = ep[i][3]
    #     if brep > maxbrep:
    #         maxbrep = brep

    #     prep = ep[i][4]
    #     if prep > maxprep:
    #         maxprep = prep


    #     bre = e[i][3]
    #     if bre > maxbre:
    #         maxbre = bre

    #     pre = e[i][4]
    #     if pre > maxpre:
    #         maxpre = pre



    #     brbp = bp[i][3]
    #     if brbp > maxbrbp:
    #         maxbrbp = brbp

    #     prbp = bp[i][4]
    #     if prbp > maxprbp:
    #         maxprbp = prbp


    #     brb = b[i][3]
    #     if brb > maxbrb:
    #         maxbrb = brb

    #     prb = b[i][4]
    #     if prb > maxprb:
    #         maxprb = prb

    # print("prem +")
    # print(maxbrpp, maxprpp)

    # print("prem")
    # print(maxbrp, maxprp  )
    
    # print("enhanced plus")
    # print(maxbrep, maxprep)

    # print("enhanced")
    # print(maxbre, maxpre)

    # print("basic plsu")
    # print(maxbrbp, maxprbp)

    # print("basic")
    # print(maxbrb, maxprb)
    
    print(pp)
    return


if __name__ == "__main__":
   main()