from flask import Flask,request,jsonify
from reference import referenceXML,referencAlternateXML,VM_Type,reference_volume_xml
from flask.ext.pymongo import PyMongo
import config
import uuid
import libvirt
idx = 0
from bson.objectid import ObjectId
import json
import random
from bs4 import BeautifulSoup
soup = BeautifulSoup(referenceXML.XML_doc)
import os
import string
import subprocess
app = Flask(__name__)
app.config.from_object(config)
mongo = PyMongo(app)
import subprocess      
from reference.randomChoice import randFunc
#mongo.drop_database()

import rados,rbd
from bson import objectid




def id_generator(size=6, chars=string.ascii_uppercase + string.digits):
    return ''.join(random.choice(chars) for x in range(size))


@app.route("/")
def hello():
    return "Cloud Orchestration Layer v1.0 " + " Author : Kumar Rishabh :: 201102103"


@app.route('/vm/create', methods = ['GET','POST'])
def VMcreate():
    global idx
    idx = idx+1
    #return str(idx)
    if(idx == 1):
	for i in mongo.db.machines.find():
		ipDetailx = i['pmName']
		idNew = i['id']
		connector1 ="qemu+ssh://"+ipDetailx+"/system"
		conn1 = libvirt.open(connector1)
		machineDetails = {}
		ListNew = conn1.getInfo()
		machineDetails['id'] = idNew
		machineDetails['ip'] = ipDetailx
		machineDetails['ram'] = ListNew[1]
		machineDetails['vcpu'] = ListNew[2]
		mongo.db.machine_info.insert(machineDetails)
 
    test_vm = request.args.get('name')
    VMtype = request.args.get('instance_type')
    image_id = request.args.get('image_id')
    imageToBoot = mongo.db.images.find_one({'id':int(image_id)})
    imageName = imageToBoot['imageFile']
    #return str(imageToBoot)
    #return str(machineCount)
    ######implementing a basic algorithm the image is booted up in \
    ######idx%machineCount numbered machine
    instance_id = int(VMtype)
    Listx = mongo.db.instance.find_one()
    for i in Listx["types"]:
        if(i['tid'] == instance_id):
            memoryx = i["ram"]
            vcpux =  i["cpu"]
   

    machineCount = mongo.db.machines.find().count()
    
    machineToBoot = (idx%machineCount)+1
    machineEmerCount = machineToBoot
    machineSpecDetails = mongo.db.machine_info.find_one({'id':machineToBoot})
    #return str(machineSpecDetails)
    while(machineSpecDetails['ram']<memoryx or machineSpecDetails['vcpu']<vcpux):
            idx=idx+1
    	    machineToBoot = (idx%machineCount)+1 
	      
            if(machineToBoot  == machineEmerCount):
		dictTo = {}
                dictTo['vmid'] = 0
		return jsonify(dictTo)
	    machineSpecDetails = mongo.db.machine_info.find_one({'id':machineToBoot})

    memory = machineSpecDetails['ram'] - memoryx
    cpu = machineSpecDetails['vcpu'] - vcpux
    #return str(memory)+str(" ")+str(cpu)+" "+str(machineToBoot)
    mongo.db.machine_info.update(  {'id':machineToBoot},{'$set':{'ram': memory,'vcpu': cpu }})    

    machineToBootDetails = mongo.db.machines.find_one({'id':machineToBoot})
    ipDetails = machineToBootDetails['pmName']
    print "the"+str(machineToBootDetails)+"dude"
    #return str(ipDetails)
    nameOfFile = id_generator()
    command ="scp  "+imageName+"  "+ipDetails+":/"+nameOfFile+".img"
    os.system(command)
    #return command
    connector ="qemu+ssh://"+ipDetails+"/system"
    conn = libvirt.open(connector)
    #return str(conn.getInfo())
    #conn=libvirt.open("qemu:///system")
    stdout, stderr = subprocess.Popen(['ssh',ipDetails, 'arch'],stdout=subprocess.PIPE).communicate()
    stdout = stdout.strip()
    if(stdout == "x86_64"):
        string = ''.join(referenceXML.XML_doc.split('\n'))
    else:
        string = ''.join(referencAlternateXML.XML_doc.split('\n'))
    #return string 
    #####string manipulation
    #return str(test_vm)+str(VMtype)
    #List = VM_Type.types["types"]
    
    vmName = test_vm
    randomUUID = uuid.uuid4()
    instance_type = int(VMtype)


    source = "/"+nameOfFile+".img"
    List = mongo.db.instance.find_one()
    for i in List["types"]:
        if(i['tid'] == instance_type):
            memory = i["ram"]*1024
            vcpu =  i["cpu"]
    VMDetails = {}
    VMDetails["name"] = vmName
    VMDetails["instance_type"] = instance_type
    #someRandomNumber = int(random.random()*10000)
    #(randomUUID)
    VMDetails["vmid"] = str(randomUUID)
    ############Change this afterwards
    VMDetails["pmid"] = machineToBoot
    mongo.db.VMDetails.insert(VMDetails)
    toReturnDetail = mongo.db.VMDetails.find_one({"instance_type":instance_type,"name":vmName,"pmid":machineToBoot})
    
    
    XML_desc = string%(idx,test_vm,randomUUID,memory,memory,vcpu,source)
    #return XML_desc
    try:
	conn.createXML(XML_desc,0)
    except:
	dictTo = {}
        dictTo['vmid'] = 0
        conn.close()
	return jsonify(dictTo)
	
    toReturnActual = {}
    
    toReturnActual["vmid"] = str(randomUUID)
    conn.close()
    return jsonify(toReturnActual)
    return json.dumps(toReturnActual)




@app.route('/vm/query', methods = ['GET','POST'])
def VMquery():
    vmid = request.args.get('vmid')
    #document = mongo.db.VMDetails.find_one({'_id': ObjectId(vmid)})
    try:
        for i in mongo.db.VMDetails.find():
            print i
        document = mongo.db.VMDetails.find_one({'vmid':vmid})
        document.pop('_id',None)
    except:
        dictx = {}
        dictx['status'] = "failure Entry Not Found"
        return jsonify(dictx)
        return json.dumps(dictx)
    return jsonify(document)
    return json.dumps(document)
     


@app.route('/vm/destroy', methods = ['GET','POST'])
def VMdestroy():
    vmid = request.args.get('vmid')
    statusDict={}
    VMtoDel = mongo.db.VMDetails.find_one({'vmid':vmid})
    try:
        pmIdx = VMtoDel['pmid']
        instanceId = VMtoDel['instance_type']
    except TypeError:
        statusDict['status']=0
        return jsonify(statusDict)
        return json.dumps(statusDict)
 
    Listx = mongo.db.instance.find_one()
    for i in Listx["types"]:
        if(i['tid'] == instanceId):
            memoryx = i["ram"]
            vcpux =  i["cpu"]
   
    machineSpecDetails = mongo.db.machine_info.find_one({'id':pmIdx})
 
    memory = machineSpecDetails['ram'] + memoryx
    cpu = machineSpecDetails['vcpu'] + vcpux
    #return str(memory)+str(" ")+str(cpu)+" "+str(machineToBoot)
    mongo.db.machine_info.update(  {'id':pmIdx},{'$set':{'ram': memory,'vcpu': cpu }})    


    #return str(pmIdx)
    IPdetails = mongo.db.machines.find_one({'id':pmIdx})
    #return str(IPdetails)
    connector ="qemu+ssh://"+IPdetails['pmName']+"/system"
    #return connector
    conn = libvirt.open(connector)
    #return conn.getHostname()   
    try:
        VMToDestroy = conn.lookupByUUIDString(vmid)
    except:
        statusDict['status']=0
        conn.close()
        return jsonify(statusDict)
        return json.dumps(statusDict)
 

    if(VMToDestroy.isActive()==1):
        if(VMToDestroy.destroy()==0):
            mongo.db.VMDetails.remove({'vmid': vmid})
            statusDict['status']=1
        else:
           statusDict['status']=0
    conn.close()
    return jsonify(statusDict)
    return json.dumps(statusDict)
 






@app.route('/vm/types', methods = ['GET','POST'])
def VMtypes():
    instanceDetails = list(mongo.db.instance.find())
    dictToReturn = {}
    dictToReturn["types"] = instanceDetails[0]["types"]
    return jsonify(dictToReturn)
    return json.dumps(dictToReturn)
 
@app.route('/image/list', methods = ['GET','POST'])
def imageList():
    imageList = []
    for i in mongo.db.images.find():
        individualDict = {}
        individualDict['id'] = i['id']
        individualDict['name'] = i['imageFile']
        imageList.append(individualDict)
    dictx = {}
    dictx['images'] = imageList
    return jsonify(dictx)
    #return json.dumps(dictx)



@app.route('/volume/create', methods = ['GET','POST'])
def volumeCreate():
    volumeName = request.args.get('name')
    volumeSize = request.args.get('size')
    actualSize = int(float(volumeSize) * (1024**3))
    #return volumeName
    #return POOL_NAME
    rbd_inst.create(ioctx,str(volumeName),actualSize)
    dictx={}
    dictx['name'] = volumeName 
    dictx['size'] = volumeSize 
    dictx['status'] = "available"
    dictx['vmid'] = 0
    dictx['device'] = 0
    os.system("sudo rbd map %s --pool %s --name client.admin"%(str(volumeName),str(POOL_NAME)))
    volumeId = str(mongo.db.volumeDetails.insert(dictx))
    return jsonify(volumeid = volumeId)
 
   
@app.route('/volume/query', methods = ['GET','POST'])
def volumeQuery():
    volumeid = request.args.get('volumeid')#todo check the size of the objectId in the request
    volume = mongo.db.volumeDetails.find_one({"_id": objectid.ObjectId(volumeid)})
    stringError = "volumeid : "+volumeid+" does not exist"
    if volume == None: return jsonify(error=stringError)

    if volume['status'] == "attached":
         return jsonify(volumeid = str(volume["_id"]),
                 name = volume['name'],
                 size = volume['size'],
                 status = volume['status'],
                 vmid = volume['vmid']
                 )

    elif volume['status'] == "available":
        return jsonify(volumeid = str(volume["_id"]),
                 name = volume['name'],
                 size = volume['size'],
                 status = volume['status'],
                 )
    else:
        return jsonify(error = "volumeid : %s does not exist"%(volumeid))

 
@app.route('/volume/destroy', methods = ['GET','POST'])
def volumeDestroy():
    volumeid = request.args.get('volumeid')
    objectId = objectid.ObjectId(volumeid)
    block = mongo.db.volumeDetails.find_one({"_id":objectId})
    #return str(block)
    if block == None:
        return jsonify(status=0)
    if block['status']=="attached":
        return jsonify(status=0)
    imageName = str(block['name'])
    os.system('sudo rbd unmap /dev/rbd/%s/%s'%(POOL_NAME,imageName))
    rbd_inst.remove(ioctx,imageName)
    mongo.db.volumeDetails.remove(block)
    return jsonify(status=1)


@app.route('/volume/attach', methods = ['GET','POST'])
def volumeAttach():
    vmid = request.args.get('vmid')
    volumeid = request.args.get('volumeid')
    volumeObjectId = objectid.ObjectId(volumeid)
    block = mongo.db.volumeDetails.find_one({"_id":volumeObjectId})
    instance = mongo.db.VMDetails.find_one({"vmid":str(vmid)})
    if block == None:                                                          
        return jsonify(status=0) 
    if instance == None:                                                          
        return jsonify(status=0) 
    if block['status']!="available" :
        return jsonify(status=0) 
    
    pmid = instance['pmid']
    machineDetails = mongo.db.machines.find_one({'id':pmid})
    connector ="qemu+ssh://"+machineDetails['pmName']+"/system"
    #return str(machineDetails['pmName'])
    conn = libvirt.open(connector)   
    #return str(vmid)
    domain = conn.lookupByUUIDString(str(vmid))
    deviceName = randFunc()
    VOLUME_LOCAL_XML = reference_volume_xml.VOLUME_XML%(POOL_NAME,block["name"],HOSTNAME,deviceName)
    print VOLUME_LOCAL_XML
    try:
        domain.attachDevice(VOLUME_LOCAL_XML)
    except:
        conn.close()
        return jsonify(status=0)
    
    conn.close()
    mongo.db.volumeDetails.update({"_id":volumeObjectId},{'$set':{'status': "attached",'vmid':str(vmid),'device':str(deviceName)}})
    return jsonify(status=1)
        
    



@app.route('/volume/detach', methods = ['GET','POST'])
def volumeDetach():
    volumeId = request.args.get('volumeid','')
    objectId = objectid.ObjectId(volumeId)
    block = mongo.db.volumeDetails.find_one({'_id':objectId})
    if block == None:
        return jsonify(status=0)
    if block['status'] == 'available':
        return jsonify(status=0)
    vmId = block['vmid']
    instance = mongo.db.VMDetails.find_one({'vmid':vmId})
    pmid = instance['pmid']
    machine = mongo.db.machines.find_one({'id':pmid})
    connector ="qemu+ssh://"+machine['pmName']+"/system"
    conn = libvirt.open(connector)   
   
    domain = conn.lookupByUUIDString(str(vmId))
    VOLUME_LOCAL_XML = reference_volume_xml.VOLUME_XML%(POOL_NAME,block["name"],HOSTNAME,block["device"])
    try:
        
        mongo.db.volumeDetails.update({"_id":objectId},{'$set':{'status': "available",'vmid':0,'device':0,}})
        domain.detachDevice(VOLUME_LOCAL_XML)
        conn.close()
        return jsonify(status=1)
    except:
        conn.close()
        return jsonify(status=0)
   
    

  


if __name__ == "__main__":
    #global conn
    connx = rados.Rados(conffile='/etc/ceph/ceph.conf')
    connx.connect()
    #import os 
    monProc = subprocess.Popen("ceph mon_status", shell=True, bufsize=0, stdout=subprocess.PIPE, universal_newlines=True) 
    monDict = eval(monProc.stdout.read())
    HOSTNAME = monDict['monmap']['mons'][0]['name']
    #os.system('sleep 4')

    #print conn.state
    if "new_cluster" not in connx.list_pools():
        connx.create_pool('new_cluster')
    ioctx = connx.open_ioctx('new_cluster')
    POOL_NAME = "new_cluster"
    rbd_inst = rbd.RBD()
    available = 1
    attached = 0
    deleted = -1



    app.run(host='0.0.0.0', debug=True)
