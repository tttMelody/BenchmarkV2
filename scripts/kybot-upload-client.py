#! /usr/bin/python
import optparse
import os
import requests
import uuid
import time
import sys
from BeautifulSoup import BeautifulSoup

parser = optparse.OptionParser(description='Kybot upload client')

parser.add_option('-s', action='store',
    dest='server', default='https://kybot.io',
    help='Kybot server used to upload')
parser.add_option('-u', action='store',
    dest='username', help='Kybot username')
parser.add_option('-p', action='store',
    dest='password', help='Kybot password')
parser.add_option('-f', action='store',
    dest='file', help='Kybot log file')

# 2MB
CHUNK_SIZE = 2 * 1024 * 1024

def initHttpSession():
    global s
    s = requests.Session()

def login(config):
    print(config.server)
    r = s.get("%s%s" % (config.server, '/saml/login'), verify=False, timeout=5)
    if r.status_code != requests.codes.ok:
        print("retry")
        r = s.get("%s%s" % (config.server, '/saml/login'), verify=False, timeout=5)
        #system.exit(1)    

    print(str(r.text.encode('utf-8')))
    print("------------0----------------")
    
    soup = BeautifulSoup(r.text)
    payload = {}
    payload["SAMLRequest"] = soup.form.input['value']
    r = s.post("%s" % soup.form['action'], data=payload)
    
#    print(r.text)
    print("------------1---------------")

    soup = BeautifulSoup(r.text)
    payload = {"shib_idp_ls_supported" : "false", "_eventId_proceed": ""}
    #action = "%s%s" % ("https://sso.kyligence.com", soup.form['action'])
    r = s.post("%s%s" % ("https://sso.kyligence.com", soup.form['action']), data=payload)

#    print(r.text)
    print("------------2---------------")

    soup = BeautifulSoup(r.text)
    payload = {}
    payload["j_password"] = config.password
    payload["j_username"] = config.username
    payload["donotcache"] = '1'
    payload["_eventId_proceed"] =''

    action = "%s%s" % ("https://sso.kyligence.com", soup.form['action'])
    r = s.post("%s%s" % ("https://sso.kyligence.com", soup.form['action']), data=payload)

    #print(str(r.text.encode('utf-8')))
    print("------------3-------------")

    soup = BeautifulSoup(r.text)
    payload = {}
#    payload["SAMLRequest"] = soup.form.input['value']
    payload["SAMLResponse"] = soup.form.input['value']
    r = s.post("%s" % soup.form['action'], data=payload, verify=False)

    print(str(r.text.encode('utf-8')))
    print("------------4-------------")

    if r.status_code != requests.codes.ok:
        print("Login failed!")
        sys.exit(1)

def upload_file_multi(config):
    statinfo = os.stat(config.file)
    chunks = (int)(statinfo.st_size / CHUNK_SIZE) + 1;
    print(chunks)
    guid = uuid.uuid4()
    payload = {'guid': guid, 'chunks': chunks, 'type': 'noused', 'lastModifiedDate': 'notused'}
    index = 1
    for chunk in read_chunks(config.file):
        #print(chunk)
        files = {'file': chunk}
        payload['name'] = uuid.uuid4()
        payload['id'] = index
        payload['size'] = len(chunk)
        payload['chunk'] = index 

        r = s.post(("%s/api/monitor/uploadSlice" % config.server), files=files, data=payload)
        if r.text != 'true':
            print "upload failed."
        
        index += 1

    #merge files
    payload = {'guid': guid, 'fileName': 'demosite-%s.zip' % time.strftime("%Y-%m-%d")}
    r = s.post(("%s/api/monitor/uploadedResult" % config.server), data=payload)
    print(str(r.text.encode('utf-8')))

def read_chunks(filepath):
    with open(filepath,'rb') as f:
        while True:
            chunk = f.read(CHUNK_SIZE)
            if chunk:
                yield chunk
            else:
                break

if __name__ == "__main__":
    (config, args) = parser.parse_args()
    initHttpSession()
    login(config)
    upload_file_multi(config)
