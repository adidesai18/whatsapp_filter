from flask import Flask, jsonify
import firebase_admin
from firebase_admin import credentials, db
import pandas as pd
import time
import logging
import requests
from requests.exceptions import Timeout
import os
from concurrent.futures import ThreadPoolExecutor
from dotenv import load_dotenv
import json

logging.basicConfig(level=logging.INFO)

executor = ThreadPoolExecutor(max_workers=2)

load_dotenv()
url = os.getenv('database_url')

# Explanation:
# .indexOn: This specifies that you want to index the values of the number node.
# .value: This tells Firebase to create an index on the values stored under number.
# results = db.reference('man_khatav/contacts').order_by_value().equal_to(number_to_check).get()
cred = credentials.Certificate('service_account_key.json')
firebase_admin.initialize_app(cred, {'databaseURL': url})

session = requests.Session()
session.headers.update({
    'Content-Type': 'application/json',
    'Connection': 'keep-alive',
})

with open('output.json', 'r') as file:
    data = json.load(file)

fail_contacts=data['man_khatav']['fail_contacts']
whatsapp_contact_list=data['man_khatav']['whatsapp_contacts']
non_whatsapp_contact_list = data['man_khatav']['non_whatsapp_contacts']

def exist_in_firestore(contact):
    if contact in whatsapp_contact_list or contact in non_whatsapp_contact_list:
        return True
    else:
        return False 

def whatsapp_compatibility(contact):
    url = "https://api.ultramsg.com/instance92619/contacts/check"
    querystring = {
        "token": "gosy2elgi0h502my",
        "chatId": f"{contact}@c.us",
    }
    try:
        response = session.get(url,params=querystring)
        logging.info(response.text)
        result=response.json()
        if result['status']=='valid':
            return True
        else:
            return False
    except Timeout:
        fail_contacts.append(contact)
        logging.error("Timeout occurred")
        time.sleep(15)
        return 
    except requests.exceptions.JSONDecodeError:
        fail_contacts.append(contact)
        logging.error(f"Failed to decode JSON for contact: {contact}")
        time.sleep(15)
        return 
    except requests.exceptions.RequestException as e:
        fail_contacts.append(contact)
        logging.error(f"Request error: {e}")
        time.sleep(15)
        return 

counter=0
def check_contact(row):
    contact = str(row['numbers']).replace('\xa0', '').replace('_x000D_', '').replace(' ', '').replace('-', '').replace('+', '').replace('\n','')
    if len(contact)==12 and contact.startswith('91'):
        if exist_in_firestore(contact):
            return
        else:
            compatible=whatsapp_compatibility(contact)
            if compatible:
                whatsapp_contact_list.append(contact)
            elif compatible==False:
                non_whatsapp_contact_list.append(contact)
            else:
                return
    elif len(contact)==10:
        cc_contact=f"91{contact}"
        if exist_in_firestore(cc_contact):
            return
        else:
            compatible=whatsapp_compatibility(cc_contact)
            if compatible:
                whatsapp_contact_list.append(cc_contact)
            elif compatible==False:
                non_whatsapp_contact_list.append(cc_contact)
            else:
                return
    else:
        return
    global counter
    counter+=1
    if counter%100==0:
        whatsapp=list(set(whatsapp_contact_list))
        non_whatsapp=list(set(non_whatsapp_contact_list))
        data = {
            'man_khatav': {
                'fail_contacts':list(set(fail_contacts)),
                'valid_contacts': len(whatsapp),
                'invalid_contacts': len(non_whatsapp),
                'whatsapp_contacts':whatsapp,
                'non_whatsapp_contacts':non_whatsapp,
                }
        }
        with open('output.json', 'w') as file:
            json.dump(data, file,indent=4)

task_live=False
path = 'sajgane.xlsx' 
df = pd.read_excel(path)

def flask_background_task():
    global task_live
    task_live=True
    df.apply(check_contact, axis=1)
    logging.info("Task Completed")
    task_live=False

app = Flask(__name__)

@app.route('/', methods=['GET'])
def root():
    # whatsapp=list(set(db.reference('man_khatav/whatsapp_contacts').get()))
    # non_whatsapp=list(set(db.reference('man_khatav/non_whatsapp_contacts').get()))
    # data = {
    #     'man_khatav': {
    #         'fail_contacts':[],
    #         'valid_contacts': len(whatsapp),
    #         'invalid_contacts': len(non_whatsapp),
    #         'whatsapp_contacts':whatsapp,
    #         'non_whatsapp_contacts':non_whatsapp,
    #         }
    # }
    # with open('output.json', 'w') as file:
    #     json.dump(data, file,indent=4)
    return jsonify({'message': 'ok'}), 200

@app.route('/health', methods=['GET'])
def health():
    return jsonify({'message': 'ok'}), 200
    
@app.route('/check_number', methods=['GET'])
def check_number():
    try:
        if not task_live:
            executor.submit(flask_background_task)
            return jsonify({'message': 'task progess started'}), 200
        else:
            return jsonify({'message': 'task in progess'}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000, debug=True)