# Streaming processing of cosmic rays using Drift Tubes detectors

The final dashboard is available at this link: https://youtu.be/3Sd7UJH7IFg


## Goal of the project

* Set up a cluster on CloudVeneto (**AWS**).

* Simulating a continuous Data acquisition stream from a particle physics detector by injecting the provided dataset into a **Kafka** topic. The dataset is hosted in a CloudVeneto bucket.

* Process the data using **Spark** and send the results to a **real-time dashboard** (Bokeh). 

## Network architecture
# aggiungi imamgine 4

### Producer
 
- Connect to the cluster -> KAFKA_BOOTSTRAP_SERVER

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