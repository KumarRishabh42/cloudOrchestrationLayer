import sys
import pymongo
from pymongo import MongoClient
client = MongoClient()
db = client['cloudDB']
import os

def parseVMTypeFile(VMfile):
    file = open(VMfile,"r")
    VMdict = eval(''.join(file.read().split('\n')))
    print VMdict
    return VMdict



def parseMachineFile(machineFile):
    file = open(machineFile,"r")
    machineFile = file.read().split('\n')
    machineFile = filter(bool , machineFile)
    return machineFile
    pass


def parseImagesFile(imageFile):
    file = open(imageFile,"r")
    imageFile = file.read().split('\n')
    imageFile = filter(bool , imageFile)
    return imageFile
    
    pass



if __name__ == '__main__':
    listOfFiles = sys.argv
    pm_file = listOfFiles[1]
    image_file = listOfFiles[2]
    types_file = listOfFiles[3]
    instanceTypeDict = parseVMTypeFile(types_file)
    machineList = parseMachineFile(pm_file)
    [ db.machines.insert({'id':i+1,'pmName':machine}) for i,machine in enumerate(machineList) ]
    db.instance.insert(instanceTypeDict)
    imageFiles = parseImagesFile(image_file)
    List = [ i.split('/') for i in imageFiles]
    [ db.images.insert({'id':i+1,'imageLocation':image,'imageFile':List[i][-1] })  for i,image in enumerate(imageFiles)]
    print "start"
    print imageFiles
    print str(List)
    print "done"
    for i in imageFiles:
           command = "scp    "+i+"    ."
           print command
           os.system(command)
    print imageFiles
    print listOfFiles




















