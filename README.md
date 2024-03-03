# Streaming processing of cosmic rays using Drift Tubes detectors

The final dashboard is available at this link: https://youtu.be/3Sd7UJH7IFg


## Goal of the project

* Set up a cluster on CloudVeneto (**AWS**).

* Simulating a continuous Data acquisition stream from a particle physics detector by injecting the provided dataset into a **Kafka** topic. The dataset is hosted in a CloudVeneto bucket.

* Process the data using **Spark** and send the results to a **real-time dashboard** (Bokeh). 

## Network architecture
# aggiungi imamgine 4

### Producer
 
- Connect to the cluster -> `KAFKA_BOOTSTRAP_SERVER`

- Define a producer

- Create two topics -> data_raw, data_clean ; define the number of partitions and the replication factor

```python3
data_raw = NewTopic(name='data_raw', 
                        num_partitions=10,                      
                        replication_factor=1)                     #replication factor is 1 (no replication) because we have one broker    

data_clean = NewTopic(name='data_clean', 
                          num_partitions=1, 
                          replication_factor=1)

kafka_admin.create_topics(new_topics=[data_raw, data_clean])
kafka_admin.list_topics()

```

- Connect to s3 bucket
- Send message asynchronously -> `KafkaProducer.send()`
- Full batch -> `KafkaProducer.flush()`

```python3
batch_counter = 0 


### Change these parameters to adjust number of records/s

wait_time = 0.95  # we send 1 batch of data every wait_time seconds

batch_size = 1000  #number of rows sent for batch 


### we send more than one batch per second by design, we could also send 1 batch per second


### loop into s3 bucket and send batches to the broker simulating a streaming data

for obj in list_obj_contents:
    
    #load each txt file into pandas dataframe
    
    df=pd.read_csv(s3_client.get_object(Bucket='##', Key=obj['Key']).get('Body'), sep=' ')
    df.rename(columns = {'HEAD,FPGA,TDC_CHANNEL,ORBIT_CNT,BX_COUNTER,TDC_MEAS':'values'}, inplace = True)
    

    
    
    "..."

        # append a record to the msg to send
        producer.send('data_raw', row)
        
        
    "..."
        #send message to the topic when we reach batch size 
        if batch_counter==batch_size:
            producer.flush()
            # sleep time
            time.sleep(wait_time)
            batch_counter=0
            
    # send last batch 
    producer.flush()

```

### Spark analysis

* Session and Context creation

* Vary these parameters to test performance:

  - `spark.executor.instances`: n° of executors 

  - `spark.executor.cores`: n° of CPU cores for each executor

  - `spark.sql.shuffle.partitions`: n° of partitions used when shuffling for joins/aggregations

  - `spark.sql.execution.arrow.pyspark.enabled`: in memory columnar format → no major differences→left `true`

* Producer creation -> send the final message to the `data_clean` topic