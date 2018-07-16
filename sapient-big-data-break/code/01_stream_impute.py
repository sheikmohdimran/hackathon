#Usage:
#export TZ=GMT
#python3 01_stream_impute 2> /tmp/stream.txt


from kafka import KafkaProducer
import time,csv
import logging

level=logging.DEBUG

# create producer to kafka connection
producer = KafkaProducer(bootstrap_servers='localhost:9092',\
							 compression_type ='gzip',\
							 batch_size=4*16384,\
							 linger_ms=2,\
							 api_version=(0, 10, 1)
							 )

# define *.csv file and a char that divide value
fname = "household.csv"
divider_char = ','
noval="0.0"		   
hhi="0_0"
# Value of dti corresponding to 31-08-2013 GMT
dti=1377986400
# open file
with open(fname) as fp:
    logging.basicConfig(level=level)
    # read header (first line of the input file)
    line = fp.readline()
    header = line.split(divider_char)

    #loop other data rows 
    line = fp.readline()    
    while line:
        # start to prepare data row to send
        data_to_send = ""
        values = line.split(divider_char)
        val1=time.localtime(int(values[2].strip()))
        #Has to be yyyy-mm-dd hh:mm:ss for spark to recognise as timestamp
        dt1=time.strftime('%Y-%m-%d', val1)                
        hr=time.strftime('%H', val1)
        # send data via producer
        hhj=values[0]+"_"+values[1]
         
        if ("10"== "10") :
         if (dti!=values[2].strip()):
          if (hhj == hhi) :
            count=0
            check=abs((int(values[2].strip())-int(dti))/60)
#            check=abs((int(values[2].strip())-int(dti))/3600)
            while count < check:
             val2=time.localtime(int(dti)+(count*60))
#             val2=time.localtime(int(dti)+(count*3600))
             dt2=time.strftime('%Y-%m-%d',val2)
             hr2=time.strftime('%H', val2)
             data_to_send = "{\"DT_HR_HH\":\""+dt2+"_"+hr2+"_"+values[0]+"_"+values[1]+"\",\"VAL\":\""+noval+"\"}"
             producer.send('sql-insert1', bytes(data_to_send, encoding='utf-8'))
             count +=1
          else:
           data_to_send = "{\"DT_HR_HH\":\""+dt1+"_"+hr+"_"+values[0]+"_"+values[1]+"\",\"VAL\":\""+values[3].strip()+"\"}"
           producer.send('sql-insert1', bytes(data_to_send, encoding='utf-8'))
           dti=str(int(values[2].strip())+60)
         else:
          data_to_send = "{\"DT_HR_HH\":\""+dt1+"_"+hr+"_"+values[0]+"_"+values[1]+"\",\"VAL\":\""+values[3].strip()+"\"}"
          producer.send('sql-insert1', bytes(data_to_send, encoding='utf-8'))
         dti=str(int(values[2].strip())+60)
         hhi=values[0]+"_"+values[1]

        line = fp.readline()          
producer.flush()
