# -*- coding: utf-8 -*-
"""
Created on Fri Oct 21 16:08:46 2016

@author: carstonhernke
"""

def generateFactors(db_connection,route,stop,total_checkpoints,start_time=0): 
    import pandas as pd
    import pandas.io.sql as psql
    import math
    from time import strftime
    from time import localtime

    def checkpoint_delta(current,checkpoint,total_checkpoints):
        if checkpoint>=current:
            return (checkpoint-current)
        if current>checkpoint:
            return (total_checkpoints-current)+checkpoint
        else:
            return "error"
            
    def nextStop(start,data,total_checkpoints):
        for i in range(0,total_checkpoints*10):
            #print("running nextstop")
            #print("start: "+ str(start))
            #print("stop: "+ str(stop))
            a=(data['checkpoint'][start+i]%int(total_checkpoints))
            #print("iteration: "+ str(a))
            if data['checkpoint'][start+i]%int(total_checkpoints)==stop:
                return i+start;
            else:
                continue
        return "error"
            
    def importObservations(conn,route,start_time):
        df = psql.read_sql("SELECT vehicle_id,route,report_time_epoch,checkpoint FROM collected_data WHERE report_time_epoch>%s AND route=%s ORDER BY report_time_epoch ASC",conn,params={start_time,route})
        return df
         
    cur = db_connection.cursor()
    print("db connection successful")
    data = importObservations(db_connection,route,start_time)
    print("observations imported")
    
    idNumbers=data['vehicle_id'].unique()
    for i in idNumbers:
        data_sub = (data.loc[data['vehicle_id']==i]).reset_index(drop=True)
        #print("starting bus number: "+str(i))
        for r in range(2,len(data_sub.index)):
            print("starting iteration " + str(r))
            if pd.isnull(data_sub['checkpoint'][r]):
                print("NaN... continuing")
                continue
            nextstop_loc=nextStop(r,data_sub,total_checkpoints)
            if nextstop_loc=="error":
                print("nextstop error")
                continue
            d_time = data_sub['report_time_epoch'][nextstop_loc]-data_sub['report_time_epoch'][r]
            tod = int(strftime("%H",localtime(data_sub['report_time_epoch'][r])))*3600+int(strftime("%M",localtime(data_sub['report_time_epoch'][r])))*60+int(strftime("%S",localtime(data_sub['report_time_epoch'][r])))
            sintime = math.sin(2*math.pi*(tod/86400))
            costime = math.cos(2*math.pi*(tod/86400))   
            day = strftime("%A",localtime(data_sub['report_time_epoch'][r]))
            checkpoint_d=checkpoint_delta(data_sub['checkpoint'][r],stop,total_checkpoints)
            print("row "+str(r)+" complete")
            if d_time>5000:
                continue
            cur.execute("INSERT INTO factors (checkpoint_current,checkpoint_stop,checkpoint_delta,time_delta,day_of_week,time_sin,time_cos) VALUES (%s,%s,%s,%s,%s,%s,%s)",(int(data_sub['checkpoint'][r]),stop,checkpoint_d,d_time,day,sintime,costime))
            db_connection.commit()
            print("iteration "+str(r)+" saved")