#
import paho.mqtt.client as mqtt
import json
import serial
import time
import sys
import minimalmodbus

hostname = "demo.ontheshalloweb.com"
port = 1883
keep_alive = 60

def set_exit_handler(func):
    signal.signal(signal.SIGTERM, func)
  
def on_exit(sig, func=None):
    log.error("exit handler triggered")
    powermeter.loop_stop()
    powermeter.disconnect()
    sys.exit(1)
def powermeter_client_on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Powermeter Client connected. (" + str(rc) +")")  # 0: Connection successful


def powermeter_client_on_message(client, userdata, msg):
    print("Powermeter Client message received from " + msg.topic + " " + str(msg.qos) + " " + str(msg.payload))


def powermeter_client_on_publish(client, obj, mid):
    print("Powermeter Client message has been published.")


def powermeter_client_on_disconnect(client, userdata, rc):
    print("Powermeter Client disconnected. (" + str(rc) + ")")

class PowerMeter:
    def __init__(self):
        self.Volts = ""
        self.Current = ""       
        self.Active_Power = ""
        self.Apparent_Power = ""
        self.Reactive_Power = ""
        self.Power_Factor = ""
        self.Phase_Angle = ""
        self.Frequency = ""
        self.Import_Active_Energy = ""
        self.Export_Active_Energy = ""
        self.Import_Reactive_Energy = ""
        self.Export_Reactive_Energy = ""
        self.Total_Active_Energy = ""
        self.Total_Reactive_Energy = ""
        self.Current_Yield = ""
    def toJSON(self):
        return dict(Volts=self.Volts,
                    Current=self.Current, 
                    Active_Power=self.Active_Power,
                    Apparent_Power=self.Apparent_Power,
                    Reactive_Power=self.Reactive_Power,
                    Power_Factor=self.Power_Factor,
                    Phase_Angle=self.Phase_Angle,
                    Frequency=self.Frequency,
                    Import_Active_Energy=self.Import_Active_Energy,
                    Export_Active_Energy=self.Export_Active_Energy,
                    Import_Reactive_Energy=self.Import_Reactive_Energy,
                    Export_Reactive_Energy=self.Export_Reactive_Energy,
                    Total_Active_Energy=self.Total_Active_Energy,
                    Total_Reactive_Energy=self.Total_Reactive_Energy,
                    Current_Yield=self.Current_Yield)

class ComplexEncoder(json.JSONEncoder):
    def default(self, obj):      
        return obj.toJSON()

class PowerMeterParser:
    @staticmethod
    def Deserialize(content):
        '''
        message = PowerMeter()
        message.timestamp = content[TIMESTAMP]
        message.topic = content[TOPIC]
        message.payload = content[PAYLOAD]
        '''
        return message

    @staticmethod
    def Serialize(content):
        return json.dumps(content, cls=ComplexEncoder)

def read_meter():
    Volts = rs485.read_float(0, functioncode=4, numberOfRegisters=2)
    Current = rs485.read_float(6, functioncode=4, numberOfRegisters=2)
    Active_Power = rs485.read_float(12, functioncode=4, numberOfRegisters=2)
    Apparent_Power = rs485.read_float(18, functioncode=4, numberOfRegisters=2)
    Reactive_Power = rs485.read_float(24, functioncode=4, numberOfRegisters=2)
    Power_Factor = rs485.read_float(30, functioncode=4, numberOfRegisters=2)
    Phase_Angle = rs485.read_float(36, functioncode=4, numberOfRegisters=2)
    Frequency = rs485.read_float(70, functioncode=4, numberOfRegisters=2)
    Import_Active_Energy = rs485.read_float(72, functioncode=4, numberOfRegisters=2) 
    Export_Active_Energy = rs485.read_float(74, functioncode=4, numberOfRegisters=2)
    Import_Reactive_Energy = rs485.read_float(76, functioncode=4, numberOfRegisters=2)
    Export_Reactive_Energy = rs485.read_float(78, functioncode=4, numberOfRegisters=2)
    Total_Active_Energy = rs485.read_float(342, functioncode=4, numberOfRegisters=2)
    Total_Reactive_Energy = rs485.read_float(344, functioncode=4, numberOfRegisters=2)

    print 'Voltage: {0:.1f} Volts'.format(Volts)
    print 'Current: {0:.1f} Amps'.format(Current)
    print 'Active power: {0:.1f} Watts'.format(Active_Power)
    print 'Apparent power: {0:.1f} VoltAmps'.format(Apparent_Power)
    print 'Reactive power: {0:.1f} VAr'.format(Reactive_Power)
    print 'Power factor: {0:.1f}'.format(Power_Factor)
    print 'Phase angle: {0:.1f} Degree'.format(Phase_Angle)
    print 'Frequency: {0:.1f} Hz'.format(Frequency)
    print 'Import active energy: {0:.3f} Kwh'.format(Import_Active_Energy)
    print 'Export active energy: {0:.3f} kwh'.format(Export_Active_Energy)
    print 'Import reactive energy: {0:.3f} kvarh'.format(Import_Reactive_Energy)
    print 'Export reactive energy: {0:.3f} kvarh'.format(Export_Reactive_Energy)
    print 'Total active energy: {0:.3f} kwh'.format(Total_Active_Energy)
    print 'Total reactive energy: {0:.3f} kvarh'.format(Total_Reactive_Energy)
    print 'Current Yield (V*A): {0:.1f} Watt'.format(Volts * Current)
    data = PowerMeter()
    data.Volts = '{0:.1f}'.format(Volts)
    data.Current = '{0:.1f}'.format(Current)
    data.Active_Power = '{0:.1f}'.format(Active_Power)
    data.Apparent_Power = '{0:.1f}'.format(Apparent_Power)
    data.Reactive_Power = '{0:.1f}'.format(Reactive_Power)
    data.Power_Factor = '{0:.1f}'.format(Power_Factor)
    data.Phase_Angle = '{0:.1f}'.format(Phase_Angle)
    data.Frequency = '{0:.1f}'.format(Frequency)
    data.Import_Active_Energy = '{0:.3f}'.format(Import_Active_Energy)
    data.Export_Active_Energy = '{0:.3f}'.format(Export_Active_Energy)
    data.Import_Reactive_Energy = '{0:.3f}'.format(Import_Reactive_Energy)
    data.Export_Reactive_Energy = '{0:.3f}'.format(Export_Reactive_Energy)
    data.Total_Active_Energy = '{0:.3f}'.format(Total_Active_Energy)
    data.Total_Reactive_Energy = '{0:.3f}'.format(Total_Reactive_Energy)
    data.Current_Yield = '{0:.1f}'.format(Volts * Current)

    powermeter.publish("/powermeter",PowerMeterParser.Serialize(data))
    print PowerMeterParser.Serialize(data)

powermeter = mqtt.Client()
powermeter.on_connect = powermeter_client_on_connect
powermeter.on_message = powermeter_client_on_message
powermeter.on_publish = powermeter_client_on_publish
powermeter.on_disconnect = powermeter_client_on_disconnect

powermeter.loop_start()
powermeter.connect(hostname, port, keep_alive)

rs485 = minimalmodbus.Instrument('/dev/ttyUSB0', 1)
rs485.serial.baudrate = 2400
rs485.serial.bytesize = 8
rs485.serial.parity = minimalmodbus.serial.PARITY_NONE
rs485.serial.stopbits = 1
rs485.serial.timeout = 1
rs485.debug = False
rs485.mode = minimalmodbus.MODE_RTU
print rs485

read_meter()

LOOP_WAIT_TIME = 1
while True:
    try:
        time.sleep(LOOP_WAIT_TIME)
        read_meter()
    except KeyboardInterrupt:
        print('KeyboardInterrupt')
        powermeter.loop_stop()
        powermeter.disconnect()
        sys.exit(0)