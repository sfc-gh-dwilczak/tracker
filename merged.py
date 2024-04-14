from confluent_kafka import Producer
import serial
import time
import re
import json


TOPIC = 'stackoverflow'

ser = serial.Serial("/dev/ttyUSB2",115200)
rec_buff = ''

kafka_producer = Producer({'bootstrap.servers': 'pkc-n98pk.us-west-2.aws.confluent.cloud:9092',
                     'security.protocol': 'SASL_SSL',
                     'sasl.mechanisms': 'PLAIN',
                     'sasl.username': 'XAJOC4VIDC4KST6U',
                     'sasl.password': '',
                     'client.id': 'device'})

def parse_gps_data(gps_data):
    # Regular expression to extract latitude and longitude in the format (ddmm.mmmm,N/S,dddmm.mmmm,E/W)
    pattern = r"\+CGPSINFO: (\d{4}\.\d+),(N|S),(\d{5}\.\d+),(W|E)"
    matches = re.search(pattern, gps_data)

    if not matches:
        return "No valid GPS data found in the string."

    lat_ddmm, lat_ns, lon_ddmm, lon_ew = matches.groups()

    # Function to convert from DDMM.MMMM format to decimal degrees
    def ddmm_to_decimal(ddmm, direction):
        degrees = int(float(ddmm) // 100)
        minutes = float(ddmm) % 100
        decimal_degrees = degrees + (minutes / 60)
        if direction in ['S', 'W']:  # Adjust for southern and western hemispheres
            decimal_degrees = -decimal_degrees
        return decimal_degrees

    # Convert latitude and longitude to decimal degrees
    latitude = ddmm_to_decimal(lat_ddmm, lat_ns)
    longitude = ddmm_to_decimal(lon_ddmm, lon_ew)

    return latitude, longitude

def send_at(command,back,timeout):
        rec_buff = ''
        ser.write((command+'\r\n').encode())
        time.sleep(timeout)
        if ser.inWaiting():
                time.sleep(0.01 )
                rec_buff = ser.read(ser.inWaiting())
        if back not in rec_buff.decode():
                print(command + ' ERROR')
                print(command + ' back:\t' + rec_buff.decode())
                return 0
        else:
                GPSDATA = str(rec_buff.decode())
                try:
                    latitude, longitude = parse_gps_data(GPSDATA)

                    location_data = {
                        "latitude": latitude,
                        "longitude": longitude
                    }

                    location_json = json.dumps(location_data)
                    kafka_producer.poll(0)
                    kafka_producer.produce(TOPIC, location_json)
                    #print(latitude, ",", longitude)
                except:
                    print("Didn't parse.")
                
                return 1

def get_gps_position():
        rec_null = True
        answer = 0
        print('Start GPS session...')
        rec_buff = ''
        send_at('AT+CGPS=0','OK',1)
        send_at('AT+CGPS=1','OK',1)
        time.sleep(2)
        while rec_null:
                answer = send_at('AT+CGPSINFO','+CGPSINFO: ',1)
                if 1 == answer:
                        answer = 0
                        if ',,,,,,' in rec_buff:
                                print('GPS is not ready')
                                rec_null = False
                                time.sleep(1)
                else:
                        print('error %d'%answer)
                        rec_buff = ''
                        send_at('AT+CGPS=0','OK',1)
                        return False
                time.sleep(1.5)

try:
        get_gps_position()

except :
        ser.close()




