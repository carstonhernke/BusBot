def getBusLocations(conn,duration=52200):
    import requests, time, psycopg2
    import xml.etree.ElementTree as ET
    
    class route:
        def __init__(self, routeName):
            self.agency = agency_tag
            self.routeName = routeName
            self.lastUpdate = 0
            
    class vehiclelog:
        def __init__(self,vehicleId,route,lat,lon,heading,speed,passengerCount,timeSinceReport):
            self.vehicleId = vehicleId
            self.route = route
            self.lat = lat
            self.lon = lon
            self.heading = heading
            self.speed = speed
            self.passengerCount = passengerCount
            self.timeSinceReport = timeSinceReport
            self.pullTimeEpoch = time.time()
            self.pullTime = time.strftime('%Y-%m-%d %H:%M:%S %Z',time.localtime(time.time()))
            self.reportTimeEpoch = time.time()-int(timeSinceReport)
            self.reportTime = time.strftime('%Y-%m-%d %H:%M:%S %Z',time.localtime(time.time()-int(timeSinceReport)))
            self.timeZone = time.strftime('%Z',time.localtime(time.time()))
     
    cur = conn.cursor()       
    agency_tag = "umn-twin"
    
    campusConnector = route("connector")
    stPaulCirculator = route("stpaul")
    stadiumCirculator = route("stadium")
    fourthStCirculator = route("4thst")
    universityCirculator = route("university")
    routes= [campusConnector,stPaulCirculator,stadiumCirculator,fourthStCirculator,universityCirculator]
    
    startTime = time.time()
    
    #headers = ["vehicleId","route","lat","lon","heading","speed","passengerCount","timeSinceReport","pullTimeEpoch","pullTime","reportTimeEpoch","reportTime","timeZone"]
    while (time.time()-startTime)<duration:
        for r in routes:
            while("TRUE"):
                try:
                    get1 = requests.get("http://webservices.nextbus.com/service/publicXMLFeed?command=vehicleLocations&a="+r.agency+"&r="+r.routeName+"&t="+str(r.lastUpdate))
                except requests.ConnectionError as e:
                    print(e)
                    time.sleep(10)
                    continue
                else:
                    root = ET.fromstring(get1.text)   
                    for vehicles in root.findall('vehicle'):
                        bus = vehiclelog(vehicles.get('id'),r.routeName,vehicles.get('lat'),vehicles.get('lon'),vehicles.get('heading'),vehicles.get('speed'),vehicles.get('passengerCount'),vehicles.get('secsSinceReport'))
                        print(bus.vehicleId,bus.route,bus.lat,bus.lon,bus.heading,bus.speed,bus.passengerCount,bus.timeSinceReport,bus.pullTimeEpoch,bus.pullTime,bus.reportTimeEpoch,bus.reportTime,bus.timeZone)
                        #data = [bus.vehicleId,bus.route,bus.lat,bus.lon,bus.heading,bus.speed,bus.passengerCount,bus.timeSinceReport,bus.pullTimeEpoch,bus.pullTime,bus.reportTimeEpoch,bus.reportTime,bus.timeZone]
                        cur.execute("INSERT INTO collected_data (vehicleId,route,observed_lat,observed_lon,heading,speed,passengerCount,timeSinceReport,pullTimeEpoch,pullTime,reportTimeEpoch,reportTime,timeZone) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,)",bus.vehicleId,bus.route,bus.lat,bus.lon,bus.heading,bus.speed,bus.passengerCount,bus.timeSinceReport,bus.pullTimeEpoch,bus.pullTime,bus.reportTimeEpoch,bus.reportTime,bus.timeZone)
                    for t in root.findall('lastTime'):
                        r.lastUpdate = t.get('time')                   
    exit()
