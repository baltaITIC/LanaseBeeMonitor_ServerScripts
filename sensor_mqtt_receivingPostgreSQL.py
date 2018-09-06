#
import paho.mqtt.client as mqtt

#
#import pymysql
import psycopg2
import sys
from EMailSender import sendMail as sender
import _thread

print(sys.version_info[0]);

#disconnect checking
def on_disconnect(client, userdata, rc):
    print("Disconnected with result code "+str(rc))
#Subscring verification
def on_subscribe(client, userdata, mid, granted_qos):
    print("sub ack " + str(mid) + "qos " +str(granted_qos))

#On_connect definition used for subscribe to edison topics
# This is the Subscriber
def on_connect(client, userdata, flags, rc):
    #
    print("Connected with result code "+str(rc))

    #
    client.subscribe("/id_site_1/light")
    client.subscribe("/id_site_1/loudness")
    client.subscribe("/id_site_1/vibration")
    client.subscribe("/id_site_1/accelerometer")
    client.subscribe("/id_site_1/temperature")
    client.subscribe("/id_site_1/humidity")
    client.subscribe("/id_site_1/rfid")
    client.subscribe("/id_site_1/weight")

    #
    client.subscribe("/id_site_1/alert/light")
    client.subscribe("/id_site_1/alert/loudness")
    client.subscribe("/id_site_1/alert/vibration")
    client.subscribe("/id_site_1/alert/accelerometer")
    client.subscribe("/id_site_1/alert/temperature")
    client.subscribe("/id_site_1/alert/humidity")
    client.subscribe("/id_site_1/alert/weight")

#On_connect definition used for receive messages from publisher
def on_message(client, userdata, msg):
    topic = str(msg.topic)
    message = str(msg.payload)

    message = message[2:(len(message)-1)]

    values = message.split()

    print(message)
    print (topic)  
    #print (msg.payload, msg.topic)
    
    #
    #conn = pymysql.connect(host='localhost', port=3306, user='root', password='', db='iot_sensors')
    conn_string = "host='localhost' dbname='iot_sensors' user='postgres' password='postgres'"
    conn = psycopg2.connect(conn_string)

    #
    #cur = conn.cursor()
    cur = conn.cursor()

    

    if (topic == "/id_site_1/light"):
        ID_Agent = int(values[0])
        ID_Sensor = values[1]
        value = str(values[2])
        datetime = str(values[3])+" "+str(values[4])
          
        cur.execute("""INSERT INTO observation(observation_value,observation_date,fk_id_agent,fk_id_sensor) VALUES(%s,%s,%s,%s)""", (value,datetime, ID_Agent, ID_Sensor))
        
    elif (topic == "/id_site_1/loudness"):
        ID_Agent = int(values[0])
        ID_Sensor = values[1]
        value = str(values[2])
        datetime = str(values[3])+" "+str(values[4])
        
        cur.execute("""INSERT INTO observation(observation_value,observation_date,fk_id_agent,fk_id_sensor) VALUES(%s,%s,%s,%s)""",(value,datetime, ID_Agent, ID_Sensor))
       
    elif (topic == "/id_site_1/vibration"):
        ID_Agent = int(values[0])
        ID_Sensor = values[1]
        value = str(values[2])
        datetime = str(values[3])+" "+str(values[4])
        
        cur.execute("""INSERT INTO observation(observation_value,observation_date,fk_id_agent,fk_id_sensor) VALUES(%s,%s,%s,%s)""",(value,datetime, ID_Agent, ID_Sensor))
       
    elif (topic == "/id_site_1/temperature"):
        ID_Agent = int(values[0])
        ID_Sensor = values[1]
        value = str(values[2])
        datetime = str(values[3])+" "+str(values[4])
        
        cur.execute("""INSERT INTO observation(observation_value,observation_date,fk_id_agent,fk_id_sensor) VALUES(%s,%s,%s,%s)""",(value,datetime, ID_Agent, ID_Sensor))
       

    elif (topic == "/id_site_1/accelerometer"):
        ID_Agent = int(values[0])
        ID_Sensor = values[1]
        value = str(values[2])
        datetime = str(values[3])+" "+str(values[4])
        
        cur.execute("""INSERT INTO observation(observation_value,observation_date,fk_id_agent,fk_id_sensor) VALUES(%s,%s,%s,%s)""",(value,datetime, ID_Agent, ID_Sensor))
                
    elif (topic == "/id_site_1/weight"):
        ID_Agent = int(values[0])
        ID_Sensor = values[1]
        value = str(values[2])
        datetime = str(values[3])+" "+str(values[4])
        
        cur.execute("""INSERT INTO observation(observation_value,observation_date,fk_id_agent,fk_id_sensor) VALUES(%s,%s,%s,%s)""",(value,datetime, ID_Agent, ID_Sensor))
       
    #Pendiente actualizar la consulta para el sensor RFID
    elif (topic == "/id_site_1/rfid"):
        value_sensor = values[1]
        
        datetime = str(values[0])
        datetime = datetime[0:4]+"-"+datetime[4:6]+"-"+datetime[6:8]+" "+datetime[9:11]+":"+datetime[11:13]+":"+datetime[13:15]
        #datetime = datetime.replace("T"," ").replace("Z","")
        value = str(value_sensor)
        #sensor= str(value_sensor[1])
        #serial = values[2]
        targetaId = str(values[2])
        print(datetime+" "+value+" "+targetaId)

        #Find rfid id by agent id
        cur.execute('select pk_id_sensor_rfid from sensor_rfid where fk_id_agent=%s;',(targetaId))
        idRfid = str(cur.fetchone())
        idRfid = idRfid[1:(len(idRfid)-2)]


        cur.execute("""INSERT INTO bee_control(control_date, fk_id_sensor_rfid, fk_id_bee) VALUES(%s,%s,%s)""",(datetime, idRfid, value))
       
    

    #-----Comienzan los alerts---------

    elif (topic == "/id_site_1/alert/light"):
        
        ID_Agent = int(values[0])
        ID_Sensor = values[1]
        value = str(values[2])
        datetime = str(values[3])+" "+str(values[4])   
        priority = str(values[5])
        state = int(values[6])

        cur.execute("""INSERT INTO observation(observation_value,observation_date,fk_id_agent,fk_id_sensor) VALUES(%s,%s,%s,%s)""", (value,datetime, ID_Agent, ID_Sensor))
        conn.commit()
        
        cur.execute('select pk_id_observation from observation order by pk_id_observation DESC LIMIT 1')
        resultados = cur.fetchone()
        for registro in resultados:
            print (registro)

        cur.execute("""INSERT INTO alert(priority, alert_state, fk_id_agent, fk_id_observation) VALUES(%s, %s, %s, %s)""",(priority, state, ID_Agent,registro))
        
        cur.execute('SELECT description, address FROM site, agent, ubication WHERE agent.pk_id_agent=%s',(str(ID_Agent)))
        resultados = cur.fetchall()
        
        lDesc = resultados[0][0]
        addr= resultados[0][1]
        #print(text)
        sensor=topic.split("/")[3]
        #print(text,sensor)
        cur.execute('SELECT unit, symbol FROM sensor, sensor_type, units WHERE sensor.pk_id_sensor=%s and sensor.fk_id_sensor_type=sensor_type.pk_id_sensor_type and sensor_type.fk_id_unit = units.pk_id_unit;',(str(ID_Sensor)))
        resultados = cur.fetchall()
        print(resultados)
        unit = ""
        symbol = ""
        try:
            unit = resultados[0][0]
            symbol= resultados[0][1]
        except:
            unit = "-"
            symbol = "---"
        try:
            _thread.start_new_thread(sender,(lDesc,addr,sensor,ID_Agent,value,datetime,unit,symbol,))
        except:
            print ("Error: Can't send mail")
    elif (topic == "/id_site_1/alert/loudness"):
        
        ID_Agent = int(values[0])
        ID_Sensor = values[1]
        value = str(values[2])
        datetime = str(values[3])+" "+str(values[4])   
        priority = str(values[5])
        state = int(values[6])

        cur.execute("""INSERT INTO observation(observation_value,observation_date,fk_id_agent,fk_id_sensor) VALUES(%s,%s,%s,%s)""", (value,datetime, ID_Agent, ID_Sensor))
        conn.commit()
        
        cur.execute('select pk_id_observation from observation order by pk_id_observation DESC LIMIT 1')
        resultados = cur.fetchone()
        for registro in resultados:
            print (registro)

        cur.execute("""INSERT INTO alert(priority, alert_state, fk_id_agent, fk_id_observation) VALUES(%s, %s, %s, %s)""",(priority, state, ID_Agent,registro))
        
        cur.execute('SELECT description, address FROM site, agent, ubication WHERE agent.pk_id_agent=%s',(str(ID_Agent)))
        resultados = cur.fetchall()
        
        lDesc = resultados[0][0]
        addr= resultados[0][1]
        #print(text)
        sensor=topic.split("/")[3]
        #print(text,sensor)
        cur.execute('SELECT unit, symbol FROM sensor, sensor_type, units WHERE sensor.pk_id_sensor=%s and sensor.fk_id_sensor_type=sensor_type.pk_id_sensor_type and sensor_type.fk_id_unit = units.pk_id_unit;',(str(ID_Sensor)))
        resultados = cur.fetchall()
        print(resultados)
        unit = ""
        symbol = ""
        try:
            unit = resultados[0][0]
            symbol= resultados[0][1]
        except:
            unit = "-"
            symbol = "---"
        try:
            _thread.start_new_thread(sender,(lDesc,addr,sensor,ID_Agent,value,datetime,unit,symbol,))
        except:
            print ("Error: Can't send mail")
    elif (topic == "/id_site_1/alert/vibration"):
        
        ID_Agent = int(values[0])
        ID_Sensor = values[1]
        value = str(values[2])
        datetime = str(values[3])+" "+str(values[4])   
        priority = str(values[5])
        state = int(values[6])

        cur.execute("""INSERT INTO observation(observation_value,observation_date,fk_id_agent,fk_id_sensor) VALUES(%s,%s,%s,%s)""", (value,datetime, ID_Agent, ID_Sensor))
        conn.commit()
        
        cur.execute('select pk_id_observation from observation order by pk_id_observation DESC LIMIT 1')
        resultados = cur.fetchone()
        for registro in resultados:
            print (registro)

        cur.execute("""INSERT INTO alert(priority, alert_state, fk_id_agent, fk_id_observation) VALUES(%s, %s, %s, %s)""",(priority, state, ID_Agent,registro))
        
        cur.execute('SELECT description, address FROM site, agent, ubication WHERE agent.pk_id_agent=%s',(str(ID_Agent)))
        resultados = cur.fetchall()
        
        lDesc = resultados[0][0]
        addr= resultados[0][1]
        #print(text)
        sensor=topic.split("/")[3]
        #print(text,sensor)
        cur.execute('SELECT unit, symbol FROM sensor, sensor_type, units WHERE sensor.pk_id_sensor=%s and sensor.fk_id_sensor_type=sensor_type.pk_id_sensor_type and sensor_type.fk_id_unit = units.pk_id_unit;',(str(ID_Sensor)))
        resultados = cur.fetchall()
        print(resultados)
        unit = ""
        symbol = ""
        try:
            unit = resultados[0][0]
            symbol= resultados[0][1]
        except:
            unit = "-"
            symbol = "---"
        try:
            _thread.start_new_thread(sender,(lDesc,addr,sensor,ID_Agent,value,datetime,unit,symbol,))
        except:
            print ("Error: Can't send mail")

    elif (topic == "/id_site_1/alert/temperature"):
        
        ID_Agent = int(values[0])
        ID_Sensor = values[1]
        value = str(values[2])
        datetime = str(values[3])+" "+str(values[4])   
        priority = str(values[5])
        state = int(values[6])

        cur.execute("""INSERT INTO observation(observation_value,observation_date,fk_id_agent,fk_id_sensor) VALUES(%s,%s,%s,%s)""", (value,datetime, ID_Agent, ID_Sensor))
        conn.commit()
        
        cur.execute('select pk_id_observation from observation order by pk_id_observation DESC LIMIT 1')
        resultados = cur.fetchone()
        for registro in resultados:
            print (registro)

        cur.execute("""INSERT INTO alert(priority, alert_state, fk_id_agent, fk_id_observation) VALUES(%s, %s, %s, %s)""",(priority, state, ID_Agent,registro))
        
        cur.execute('SELECT description, address FROM site, agent, ubication WHERE agent.pk_id_agent=%s',(str(ID_Agent)))
        resultados = cur.fetchall()
        
        lDesc = resultados[0][0]
        addr= resultados[0][1]
        #print(text)
        sensor=topic.split("/")[3]
        #print(text,sensor)
        cur.execute('SELECT unit, symbol FROM sensor, sensor_type, units WHERE sensor.pk_id_sensor=%s and sensor.fk_id_sensor_type=sensor_type.pk_id_sensor_type and sensor_type.fk_id_unit = units.pk_id_unit;',(str(ID_Sensor)))
        resultados = cur.fetchall()
        print(resultados)
        unit = ""
        symbol = ""
        try:
            unit = resultados[0][0]
            symbol= resultados[0][1]
        except:
            unit = "-"
            symbol = "---"
        try:
            _thread.start_new_thread(sender,(lDesc,addr,sensor,ID_Agent,value,datetime,unit,symbol,))
        except:
            print ("Error: Can't send mail")
    elif (topic == "/id_site_1/alert/accelerometer"):
        
        ID_Agent = int(values[0])
        ID_Sensor = values[1]
        value = str(values[2])
        datetime = str(values[3])+" "+str(values[4])   
        priority = str(values[5])
        state = int(values[6])

        cur.execute("""INSERT INTO observation(observation_value,observation_date,fk_id_agent,fk_id_sensor) VALUES(%s,%s,%s,%s)""", (value,datetime, ID_Agent, ID_Sensor))
        conn.commit()
        
        cur.execute('select pk_id_observation from observation order by pk_id_observation DESC LIMIT 1')
        resultados = cur.fetchone()
        for registro in resultados:
            print (registro)

        cur.execute("""INSERT INTO alert(priority, alert_state, fk_id_agent, fk_id_observation) VALUES(%s, %s, %s, %s)""",(priority, state, ID_Agent,registro))
        
        cur.execute('SELECT description, address FROM site, agent, ubication WHERE agent.pk_id_agent=%s',(str(ID_Agent)))
        resultados = cur.fetchall()
        
        lDesc = resultados[0][0]
        addr= resultados[0][1]
        #print(text)
        sensor=topic.split("/")[3]
        #print(text,sensor)
        cur.execute('SELECT unit, symbol FROM sensor, sensor_type, units WHERE sensor.pk_id_sensor=%s and sensor.fk_id_sensor_type=sensor_type.pk_id_sensor_type and sensor_type.fk_id_unit = units.pk_id_unit;',(str(ID_Sensor)))
        resultados = cur.fetchall()
        print(resultados)
        unit = ""
        symbol = ""
        try:
            unit = resultados[0][0]
            symbol= resultados[0][1]
        except:
            unit = "-"
            symbol = "---"
        try:
            _thread.start_new_thread(sender,(lDesc,addr,sensor,ID_Agent,value,datetime,unit,symbol,))
        except:
            print ("Error: Can't send mail")

    elif (topic == "/id_site_1/alert/weight"):
        
        ID_Agent = int(values[0])
        ID_Sensor = values[1]
        value = str(values[2])
        datetime = str(values[3])+" "+str(values[4])   
        priority = str(values[5])
        state = int(values[6])

        cur.execute("""INSERT INTO observation(observation_value,observation_date,fk_id_agent,fk_id_sensor) VALUES(%s,%s,%s,%s)""", (value,datetime, ID_Agent, ID_Sensor))
        conn.commit()
        
        cur.execute('select pk_id_observation from observation order by pk_id_observation DESC LIMIT 1')
        resultados = cur.fetchone()
        for registro in resultados:
            print (registro)

        cur.execute("""INSERT INTO alert(priority, alert_state, fk_id_agent, fk_id_observation) VALUES(%s, %s, %s, %s)""",(priority, state, ID_Agent,registro))
        
        cur.execute('SELECT description, address FROM site, agent, ubication WHERE agent.pk_id_agent=%s',(str(ID_Agent)))
        resultados = cur.fetchall()
        
        lDesc = resultados[0][0]
        addr= resultados[0][1]
        #print(text)
        sensor=topic.split("/")[3]
        #print(text,sensor)
        cur.execute('SELECT unit, symbol FROM sensor, sensor_type, units WHERE sensor.pk_id_sensor=%s and sensor.fk_id_sensor_type=sensor_type.pk_id_sensor_type and sensor_type.fk_id_unit = units.pk_id_unit;',(str(ID_Sensor)))
        resultados = cur.fetchall()
        print(resultados)
        unit = ""
        symbol = ""
        try:
            unit = resultados[0][0]
            symbol= resultados[0][1]
        except:
            unit = "-"
            symbol = "---"
        try:
            _thread.start_new_thread(sender,(lDesc,addr,sensor,ID_Agent,value,datetime,unit,symbol,))
        except:
            print ("Error: Can't send mail")


    conn.commit()
    
    cur.close()
    conn.close()
#
client = mqtt.Client("site1")
#ip provicional
client.connect("localhost",1883,60)

#
client.on_connect = on_connect
#
client.on_message = on_message
#
client.on_disconnect = on_disconnect

#client.on_subscribe = on_subscribe

#
client.loop_forever()