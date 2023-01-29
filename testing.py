import json
import time

def getFullFlowSetData(JSONfilename: str, view: str):
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


def main():

    # getFullFlowSetData Example Call
    data = getFullFlowSetData('initialPCAPdata.json', 'Application')
    for k,v in data.items():
        print(k, ':', v)

    # getUniqueIPs Example Call
    srcIPs, destIPs = getUniqueIPs('initialPCAPdata.json', True)
    

if __name__ == "__main__":
   main()