import time
import json;
import io;
from kafka import KafkaProducer
import csv

data=json.load(open('cred.json'))
bootstrap_servers=data['bootstrap_servers'];
sasl_plain_username=data['Api key'];
sasl_plain_password=data['Api secret'];
khv_path = "D:\Year 4\Cloud Computing\Milestone 4\SOFE4630U-tut4\mnist\data\csvs\kvh.csv"

producer = KafkaProducer(bootstrap_servers=bootstrap_servers,security_protocol='SASL_SSL',sasl_mechanism='PLAIN',\
    sasl_plain_username=sasl_plain_username,sasl_plain_password=sasl_plain_password,\
    value_serializer=lambda m: json.dumps(m).encode('utf-8'))
    
with open(khv_path) as csv_file:
    csv_reader = csv.reader(csv_file, delimiter=',')
    line_count = 0
    for row in csv_reader:
        value = {'key':int(row[0]), 'image':row[1]};
        producer.send('mnist_image', value);
        time.sleep(0.1);
        print("Image with key "+row[0]+" is sent with data " + str(row[1]))
producer.close();
