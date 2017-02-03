# -*- coding: utf-8 -*-
"""
Created on Sun Jul 24 21:19:38 2016

@author: carstonhernke
"""
def train(conn,route,stop):
    import pandas as pd
    import numpy as np
    import pickle
    import time
    import pandas.io.sql as psql
    from sklearn.linear_model import Lasso
    from sklearn import preprocessing
    from sklearn.preprocessing import Imputer
    from sklearn.preprocessing import OneHotEncoder
    
    def importFactors(conn,route,checkpoint):
            df = psql.read_sql("SELECT checkpoint_current,checkpoint_delta,time_delta,day_of_week,time_sin,time_cos FROM factors WHERE checkpoint_stop=%s",conn,params={stop})
            return df
    
    data = importFactors(conn,route,stop)
    
    factors = ["checkpoint_current","checkpoint_delta","day_of_week","time_sin","time_cos"]
    x_train = data[factors]
    #x_train = data[factors].values
    y_train = data['time_delta'].values
    
    hot1 = pd.get_dummies(x_train['checkpoint_current'])
    #print(hot1.head)
    hot2 = pd.get_dummies(x_train['day_of_week'])
    #print(hot2.head)
 
    x_train = x_train.join(hot1)
    x_train = x_train.join(hot2)
    
    x_train = x_train.drop('checkpoint_current',1)
    x_train = x_train.drop('day_of_week',1)

    #print(x_train.head)

    y_train = (np.asarray(y_train,dtype="|S6"))
    
    """
    imp = Imputer(missing_values='NaN', strategy='median', axis=0) 
    imp.fit_transform(x_train)
    x_train_imp = imp.transform(x_train)
    x_train_imp = x_train_imp.astype(float)
    x_scaled=preprocessing.scale(x_train_imp) #scale x train values
    scaler = preprocessing.StandardScaler().fit(x_train_imp) #save scaler stats so it can be used for testing set
    
    x_test_imp = imp.transform(x_test) #process test values in same way
    x_test_imp = x_test_imp.astype(float)
    x_test = scaler.transform(x_test_imp)    
    """
    
    clf = Lasso()
    clf.fit(x_train,y_train)
    
    time = time.time()
    filename = str(route)+str(stop)+str(time)
    filepath=str("trained_regressors/"+filename+".pickle")
    with open(filepath,'wb') as f:
        pickle.dump(clf,f)
        
    cur = conn.cursor()
    cur.execute("INSERT INTO factors (route,stop,time,filename) VALUES (%s,%s,%s,%s)",(route,stop,time,filename))
    conn.commit()
    
    #print(list(x_train.columns.values))
        
    """    
    #y_test = (np.asarray(y_test,dtype="|S6"))
    prediction = clf.predict(x_test)
    print("PREDICTIONS:")
    for i,val in enumerate(prediction):
        print("pred"+str(i)+" : "+'{0:.01f}'.format(val))
    #print(prediction)
    print("\nTRUE VALUES:")
    for i,val in enumerate(y_test):
        print("obs"+str(i)+" : "+str(val))
    print("\nERRORS: (prediction-observation)")
    for i,val in enumerate(prediction):
        print("error"+str(i)+" : "+str(float(val)-float(y_test[i])))
    #print(y_test)
    print("\nCoefficient of Determination (R^2) : "+ str(clf.score(x_test,y_test)))
    #print(+" : " +str(clf.score(x_test,y_test)))
    print("\ncomplete")
"""