# -*- coding: utf-8 -*-
"""
Created on Wed Jul  6 19:13:53 2016

@author: carstonhernke
"""
def calc_checkpoint_times(conn):
        
    import math
    import pandas as pd
    import numpy as np
    import psycopg2
    import pandas.io.sql as psql
    
    class route:
        def __init__(self, routeName):
            self.routeName = routeName
            self.last_time = 0
            
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
        def __init__(self, x, y, z, t=0, b=0):
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
    
    def speed(p1,p2): #takes two cartTimes, gives speed in m/s
        return dist(p1,p2)/(p2.time-p1.time)
    
    def importCheckpoints(conn,route):
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
        
    def importObservations(conn,route,last_time):
        df_o = psql.read_sql("SELECT vehicle_id,route,report_time_epoch,on_route,adj_x,adj_y,adj_z,checkpoint FROM collected_data WHERE report_time_epoch>%s AND route=%s ORDER BY report_time_epoch ASC",conn,params={last_time,route.routeName})
        last = psql.read_sql("SELECT vehicle_id,route,report_time_epoch,on_route,adj_x,adj_y,adj_z,checkpoint FROM collected_data WHERE report_time_epoch<%s AND route=%s ORDER BY report_time_epoch DESC LIMIT 1",conn,params={last_time,route.routeName})
        last = last.append(df_o)
        #df_o = pd.read_csv("corrected_data/"+filename,usecols=[0,1,4,5,6,7,8])
        return last
      
    def findAhead3(i,p1):
        for k in range(0,5):
            if ls['checkpoint'][(i+k)]==p1.cid:
                continue
            elif ls['checkpoint'][(i+k)]>p1.cid:
                return (i+k)
            elif (p1.cid-ls['checkpoint'][(i+k)])<len(c.index)+20:    
                return (i+k)
            else:
                print("error : "+str(l['on_route'][i+k])+" , "+str(ls['checkpoint'][(i+k)]))
        
    def getDistances(p1,p2,p1c,p2c,c):
        ds=[]
        ds.append(dist(p1,cartesian(c['x'][(p1c+1)%len(c.index)],c['y'][(p1c+1)%len(c.index)],c['z'][(p1c+1)%len(c.index)])))
        for ch in range(int(p1c+1),int(p1c+((p2c-p1c)%len(c.index)))):
            ds.append(dist(cartesian(c['x'][ch%len(c.index)],c['y'][ch%len(c.index)],c['z'][ch%len(c.index)]),cartesian(c['x'][(ch+1)%len(c.index)],c['y'][(ch+1)%len(c.index)],c['z'][(ch+1)%len(c.index)])))
        ds.append(dist(cartesian(c['x'][(p2c-1)%len(c.index)],c['y'][(p2c-1)%len(c.index)],c['z'][(p2c-1)%len(c.index)]),p2))
        return ds
        
    def cumulativeDistance(d,index): #takes an array of distances and an index
        distance = 0
        for i in range(0,index):
            distance = distance+d[i]
        return distance
        
    def findPath(route):
        if route =="c_university_alt":
            return c_university_alt
        if route =="connector_alt":
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
            
    def findStart(l):
        i=0
        while(True):
            if l['on_route'][i]!=True:
                i=i+1
                continue
            else:
                return i
        
        
    #csvFile =  open("checkpoint_logs/university_test.csv",'a') #file to output to
    #csvWriter = csv.writer(csvFile)
    
    cur = conn.cursor()
    c_university = importCheckpoints(conn,"university")
    c_connector = importCheckpoints(conn,"connector")
    c_stadium = importCheckpoints(conn,"stadium")
    c_4thst = importCheckpoints(conn,"4thst")
    c_stpaul = importCheckpoints(conn,"stpaul")
    c_university_alt = importCheckpoints(conn,"university_alt")
    c_connector_alt = importCheckpoints(conn,"connector_alt")
    
    campusConnector = route("connector")
    stPaulCirculator = route("stpaul")
    stadiumCirculator = route("stadium")
    fourthStCirculator = route("4thst")
    universityCirculator = route("university")
    routes= [campusConnector,stPaulCirculator,stadiumCirculator,fourthStCirculator,universityCirculator]
    
    for r in routes:
        
        c = findPath(r.routeName)
        l = importObservations(conn,r,r.last_time) #corrected observations
        
        idNumbers=l['vehicle_id'].unique()
        for i in idNumbers:
            start_num = findStart(l)
            ls = (l.loc[l['vehicle_id']==i]).reset_index(drop=True)
            #ls = ls.sort('time')
            p1 = cartTimeInd(ls['adj_x'][start_num],ls['adj_y'][start_num],ls['adj_z'][start_num],ls['report_time_epoch'][start_num],ls['checkpoint'][start_num])
            n=1
            #print(len(c.index))
            for i in range(1,len(ls.index)):
                print(str(i)+" : "+str(l['on_route'][i]))
                if l['on_route'][i]!=True:
                    continue
                if ls['checkpoint'][i]==ls['checkpoint'][i-1]:
                    #print("row "+str(i+2)+" skipped")
                    continue
                else:
                    n=findAhead3(i,p1)
                    p2 = cartTimeInd(ls['adj_x'][n],ls['adj_y'][n],ls['adj_z'][n],ls['report_time_epoch'][n],ls['checkpoint'][n])
                    t = p2.time-p1.time
                    ds = getDistances(p1,p2,p1.cid,p2.cid,c)
                    d=0
                    for val in ds:
                        d = d+val
                    s = d/t
                    for j in range(0,len(ds)-1):
                        time = cumulativeDistance(ds,j)/s
                        vehicle_id=ls['vehicle_id'][i]
                        checkpoint=(p1.cid+j+1)%len(c.index)
                        time = p1.time+time
                        if(time=="nan"):
                            time="""'nan'"""
                        cur.execute("INSERT INTO check_time (route,vehicle_id,checkpoint,time) VALUES (%s,%s,%s,%s)",(r.routeName,vehicle_id,checkpoint,time))
                        conn.commit()
                        #csvWriter.writerow(data)
                        #print("row "+ str(i) +" saved in "+str(etime-stime)+" seconds")
                    p1 = p2
