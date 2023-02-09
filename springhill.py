import json
import time
import csv

def getFlowsetDataCalculations(JSONfilename: str, view: str):
    """
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
    Get all flowset data from a file.
    
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

        numFlows = FullFlowSetData["_source"]["layers"]["cflow"]["cflow.count"]        
        flowSet = FullFlowSetData["_source"]["layers"]["cflow"]
        for key in flowSet.keys():
            if key[0] != 'c':
                flowsetNumber = key
        flowsetLength = FullFlowSetData["_source"]["layers"]["cflow"][flowsetNumber]["cflow.flowset_length"]

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

        # Iterate through each flow, appending 
        flows = flowSet[flowsetNumber]
        numFlows = len(flows) - 4

        for flowKey, flowVals in flows.items():
            if flowKey[0] != 'c':
                oneFlow = {
                    "uniqueFlowsetId": flowSetUniqueIdCounter,
                    "flowNumber": int(flowKey[5:]),
                    "srcAddr": (flowVals["cflow.srcaddr"]),
                    "dstAddr": (flowVals["cflow.dstaddr"]),
                    "srcPort": (flowVals["cflow.srcport"]),
                    "dstPort": (flowVals["cflow.dstport"]),
                    "forwardingStatus": (flowVals["Forwarding Status"]["cflow.forwarding_status"]),
                    "forwardingStatusCode": (flowVals["Forwarding Status"]["cflow.forwarding_status_forward_code"]),
                    "nextHop": (flowVals["cflow.nexthop"]),
                    "outputInt": (flowVals["cflow.outputint"]),
                    "outputDirection": (flowVals["cflow.direction"]),
                    "bytes": int(flowVals["cflow.octets"]),
                    "packets": int(flowVals["cflow.packets"]),
                    "timeDelta": float(flowVals["cflow.timedelta"])
                }
                flowsTableData.append(oneFlow)

        flowSetUniqueIdCounter += 1
    return flowsetsTableData, flowsTableData


def writeFlowsetsAndFlowsToCSV(JSONfilename: str, flowsetsOutputFileName: str, flowsOutputFileName: str, siteId: int):
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

    with open(flowsOutputFileName, "w", newline='') as f:
        writer = csv.writer(f)
        writer.writerow(["uniqueFlowsetId","flowNumber", "srcAddr", "dstAddr", "srcPort", "dstPort", "forwardingStatus", "forwardingStatusCode", "nextHop", "outputInt", "outputDirection", "bytes", "packets", "timeDelta"])
        
        for i in range(len(flowsTableData)):
            flow = flowsTableData[i]
            writer.writerow([flow["uniqueFlowsetId"], flow["flowNumber"], flow["srcAddr"], flow["dstAddr"], flow["srcPort"], flow["dstPort"], 
                             flow["forwardingStatus"], flow["forwardingStatusCode"], flow["nextHop"], flow["outputInt"], flow["outputDirection"], flow["bytes"], 
                             flow["packets"], flow["timeDelta"]])

    f.close()   


def main():

    # getFullFlowSetData Example Call
    # data = getFullFlowSetData('initialPCAPdata.json', 'Application')
    # for k,v in data.items():
    #     print(k, ':', v)

    # getUniqueIPs Example Call
    # srcIPs, destIPs = getUniqueIPs('initialPCAPdata.json', True)

    # getFlowsets Example Call
    # flowsetTableData, flowsTableData = getFlowsets("oneFlowset.json", 1)

    # writeFlowsetsToCSV Example Call
    # writeFlowsetsToCSV("oneFlowset.json", "oneFlowsetFlowsets.csv", "oneFlowsetFlows.csv", 1)

    
    start = time.time()
    writeFlowsetsAndFlowsToCSV("nf2_maktai.json", "nf2_maktaiFlowsets.csv", "nf2_maktaiFlows.csv", 1)
    print("Total time elapsed: ", time.time() - start)



if __name__ == "__main__":
   main()