def getBusLocations(conn):
    import requests, time, psycopg2, math
    import datetime
    import xml.etree.ElementTree as ET
    import pandas as pd
    import numpy as np
    import pandas.io.sql as psql
        
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
    class velocity:
        def __init__(self, speed, direction):
            self.speed = speed
            self.dir = direction
    
    class cartesian:
        def __init__(self, x, y, z): 
            self.x = x
            self.y = y
            self.z = z
    
    class cartTime(cartesian): #time should be relative to epoch in s
        def __init__(self, x, y, z, t, b=0):
            cartesian.__init__(self, x, y, z)
            self.time = t
            self.bearing = b
            
    class geo(cartTime):
        def __init__(self, lat, lon, time):
            cartesian.__init__(self, geo2cart(lat,lon).x, geo2cart(lat,lon).y, geo2cart(lat,lon).z)
            self.latitude = lat
            self.longitude = lon
    
    class cartTimeInd(cartTime):
        def __init__(self, x, y, z, time, cid):
            cartTime.__init__(self, x, y, z, time)
            self.cid = cid
            
    def dist(p1,p2): #returns distance between two cartesian coordinates in meters
        return abs(math.sqrt(((p2.x-p1.x)**2)+((p2.y-p1.y)**2)+((p2.z-p1.z)**2)))*1000 #basic euclidean distance formula (including z-coordinates)
        
    def geo2cart(lat,lon): #convert geodisic coordinates to cartesian coordinates. takes two arguments-(latitude,longitude)
        eradius = 6391.645929 #calculated for UMN location (44.9N) using Ligas & Banasic, 2011
        rlat = np.radians(lat)
        rlon = np.radians(lon)
        x = eradius * np.cos(rlat) * np.cos(rlon)
        y = eradius * np.cos(rlat) * np.sin(rlon)
        z = (eradius*0.99330) * np.sin(rlat)
        return cartesian(x,y,z)
    
    def adjustObservation(c1,c2,p): #takes 2 checkpoints (ch1,ch2)(geo) and one point (p)(carttime), and returns the point on the line between checkpoints closest to the point
        t=((p.x-c1.x)*(c2.x-c1.x)+(p.y-c1.y)*(c2.y-c1.y))/((c2.x-c1.x)**2+(c2.y-c1.y)**2)
        new_x = t*(c2.x-c1.x)+c1.x
        new_y = t*(c2.y-c1.y)+c1.y
        return cartTime(new_x,new_y,p.z,p.time) #passes through z and time
    
    def betweenPoints(p1,p2,p3): #takes three cartesian points. Checks whether the third one lies between the first two (within 20 meters). Returns a bool
        p3_adj = adjustObservation(p1,p2,p3)
        if (dist(p1,p3_adj)+dist(p3_adj,p2)) - dist(p1,p2) < 10 : #is between points, doesn't check distance
            on_line = True
        else:
            on_line = False
        if dist(p3,p3_adj) < 30: #less than x meters from line connecting points
            close_to_line = True
        else:
            close_to_line = False
        return on_line and close_to_line
     
    def checkBearing(p1,p2,o):
        c_bear = 90-(180/math.pi)*math.atan2(p2.y-p1.y,p2.x-p1.x) #finds bearing of two points
        o_bear = o.bearing
        if (180-abs(abs(c_bear-o_bear)-180))<30:
            return True
        else:
            return True
    
    def speed(p1,p2): #takes two cartTimes, gives speed in m/s
        return dist(p1,p2)/(p2.time-p1.time)
    
    def importCheckpoints(route):
        if route =='university':
            df_c = psql.read_sql("SELECT * FROM checkpoints WHERE route='university' ORDER BY cid",conn)
        if route =='university_alt':
            df_c = psql.read_sql("SELECT * FROM checkpoints WHERE route='university_alt' ORDER BY cid",conn)
        if route =='4thst':
            df_c = psql.read_sql("SELECT * FROM checkpoints WHERE route='4thst' ORDER BY cid",conn)
        if route =='stadium':
            df_c = psql.read_sql("SELECT * FROM checkpoints WHERE route='stadium' ORDER BY cid",conn)
        if route =='stpaul':
            df_c = psql.read_sql("SELECT * FROM checkpoints WHERE route='stpaul' ORDER BY cid",conn)
        if route =='connector':
            df_c = psql.read_sql("SELECT * FROM checkpoints WHERE route='connector' ORDER BY cid",conn)
        if route =='connector_alt':
            df_c = psql.read_sql("SELECT * FROM checkpoints WHERE route='connector_alt' ORDER BY cid",conn)
            
        dfx = pd.DataFrame(geo2cart(df_c.lat,df_c.lon).x)
        dfy = pd.DataFrame(geo2cart(df_c.lat,df_c.lon).y)
        dfz = pd.DataFrame(geo2cart(df_c.lat,df_c.lon).z)
        df_c["x"]=dfx
        df_c["y"]=dfy
        df_c["z"]=dfz
        return df_c
        
    def importObservations(filename):
        df_o = pd.read_csv("collected_data/"+filename,usecols=[0,1,2,3,4,10])
        return df_o
        
    def betweenCheckpoints(p): #checks which two checkpoints a point lies between, and returns the ID of the first of the pair   
        for i in range(0,len(c.index)-2):
            if betweenPoints(df2c(c,i),df2c(c,i+1),p) and checkBearing(df2c(c,i),df2c(c,i+1),p):
                return i
        if betweenPoints(df2c(c,len(c.index)-1),df2c(c,0),p) and checkBearing(df2c(c,len(c.index)-1),df2c(c,0),p):
                return len(c.index)-1
        else:
            return -1
        
    def df2c(df,index):
        x = df['x'][index]
        y = df['y'][index]
        z = df['z'][index]
        return cartTime(x,y,z,0)
        
    def dfl2c(df,index):
        l = geo2cart(df['lat'][index],df['lon'][index])
        return cartTime(l.x,l.y,l.z,df['reportTimeEpoch'][index],df['heading'][index])
        
    def checkpointsBetween(df,o1,o2): #checks which checkpoints lie between two points, and appends them to a list, then returns the list
        checkpoints = []
        for i in range(0,len(df.index)-1):
            if betweenPoints(o1,o2,df2c(df,i)) == True:
                checkpoints.append(i)
        return checkpoints
        
    def firstAdjustedObservation(p):
        b = betweenCheckpoints(p)
        checkpoint1 = b[0]
        checkpoint2 = b[1]
        return adjustObservation(checkpoint1,checkpoint2,p)
    
    def calculateCheckpointTime(cid,p1,p2):
        c_time = p1.time+dist(p1,df2c(c,cid))/speed(p1,p2) #may need to modify so distance is on checkpoint path
        return c_time 
        
    def alternateTime():
        now = datetime.datetime.now()
        day = datetime.datetime.today().weekday()
        
        if day == 5 or day == 6:
            return True
        else:
            if now.hour == 18 and now.minute >= 30:
                return True
            if now.hour > 18:
                return True
            if now.hour < 3:
                return True
            else:
                return False
        
    def findPath(route):
        if route =="university" and alternateTime():
            return c_university_alt
        if route =="connector" and alternateTime():
            return c_connector_alt
        if route =="university":
            return c_university
        if route =="4thst":
            return c_4thst
        if route =="connector":
            return c_connector
        if route =="stpaul":
            return c_stpaul
        if route =="stadium":
            return c_stadium
        else:
            return "ERROR"
        
    def snapPoint(p):
        cid = betweenCheckpoints(p)
        if cid == -1:
            return -1
        if cid < len(c.index)-2:
            a = adjustObservation(df2c(c,cid),df2c(c,cid+1),p)
        if cid == len(c.index)-1:
            a = adjustObservation(df2c(c,cid),df2c(c,0),p)
        point_a = cartTimeInd(a.x,a.y,a.z,0,cid)
        return point_a
                
    def correctPoint(point,c,cur,route):
            obs = geo2cart(float(point.lat),float(point.lon))
            obs = cartTime(obs.x,obs.y,obs.z,point.reportTimeEpoch,point.heading)
            pt = snapPoint(obs)
            if pt == -1:
                #data = [l['vehicleId'][i],l['route'][i],l['lat'][i],l['lon'][i],l['reportTimeEpoch'][i],"NA","NA","NA"]
                pt = cartTimeInd('NaN','NaN','NaN','NaN','-1')
                return pt
            #if route=='connector':
            #    route='connector_alt'
            cur.execute("SELECT checkpoint FROM collected_data WHERE route=(%s) ORDER BY checkpoint DESC",(route,)) #get last observation               
            lastcid = cur.fetchone()
            #print(cur.fetchone())
            if type(lastcid)==int:
                cid_delta = pt.cid-lastcid
                backwards = (cid_delta<0) and (cid_delta>((-len(c.index)+20)))
                if backwards:
                    pt = cartTimeInd('NaN','NaN','NaN','NaN','-1')
                    return pt
                else:
                    return pt
            else:
                return pt

    c_university = importCheckpoints("university")
    c_connector = importCheckpoints("connector")
    c_stadium = importCheckpoints("stadium")
    c_4thst = importCheckpoints("4thst")
    c_stpaul = importCheckpoints("stpaul")
    c_university_alt = importCheckpoints("university_alt")
    c_connector_alt = importCheckpoints("connector_alt")

    cur = conn.cursor()
     
    agency_tag = "umn-twin"
    campusConnector = route("connector")
    stPaulCirculator = route("stpaul")
    stadiumCirculator = route("stadium")
    fourthStCirculator = route("4thst")
    universityCirculator = route("university")
    routes= [campusConnector,stPaulCirculator,stadiumCirculator,fourthStCirculator,universityCirculator]
    while("TRUE"):
        for r in routes:
                try:
                    get1 = requests.get("http://webservices.nextbus.com/service/publicXMLFeed?command=vehicleLocations&a="+r.agency+"&r="+r.routeName+"&t="+str(r.lastUpdate))
                except requests.ConnectionError as e:
                    print(e)
                    time.sleep(10)
                    continue
                else:
                    try:
                        root = ET.fromstring(get1.text)   
                    except ET.ParseError:
                        continue
                    else:
                        for vehicles in root.findall('vehicle'):
                            bus = vehiclelog(vehicles.get('id'),r.routeName,float(vehicles.get('lat')),float(vehicles.get('lon')),float(vehicles.get('heading')),vehicles.get('speed'),vehicles.get('passengerCount'),vehicles.get('secsSinceReport'))
                            c = findPath(bus.route)
                            pt = correctPoint(bus,c,cur,bus.route)
                            if type(pt.x)!=str:
                                onRoute = True
                            else:
                                onRoute = False
                            cur.execute("INSERT INTO collected_data (vehicle_id,route,observed_lat,observed_lon,heading,speed,passenger_count,time_since_report,pull_time_epoch,pull_time,report_time_epoch,report_time,time_zone,on_route,adj_x,adj_y,adj_z,checkpoint) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",(bus.vehicleId,bus.route,bus.lat,bus.lon,bus.heading,bus.speed,bus.passengerCount,bus.timeSinceReport,bus.pullTimeEpoch,bus.pullTime,bus.reportTimeEpoch,bus.reportTime,bus.timeZone,onRoute,pt.x,pt.y,pt.z,pt.cid))
                            #cur.execute("INSERT INTO collected_data (vehicle_id,route,observed_lat,observed_lon,heading,speed,passenger_count,time_since_report,pull_time_epoch,pull_time,report_time_epoch,report_time,time_zone) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",(bus.vehicleId,bus.route,bus.lat,bus.lon,bus.heading,bus.speed,bus.passengerCount,bus.timeSinceReport,bus.pullTimeEpoch,bus.pullTime,bus.reportTimeEpoch,bus.reportTime,bus.timeZone))
                            conn.commit()
                            #print("observation saved correctly")
                        for t in root.findall('lastTime'):
                            r.lastUpdate = t.get('time')                 
    exit()
