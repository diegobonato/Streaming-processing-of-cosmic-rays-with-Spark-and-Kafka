# Streaming processing of cosmic rays using Drift Tubes detectors

The final dashboard is available at this link: https://youtu.be/3Sd7UJH7IFg


#### Goal 
The goal of this project is to reproduce a real-time processing of real data collected in a particle physics detector and publish the results in a
dashboard for live monitoring.

## 1 Introduction 
The introductory set of information on the underlying physics and detector details can be found in Section 4.1 of [`MAPD_DataProcessing_2023.pdf`](https://github.com/Berto70/streaming_processing_mapdb_2023/blob/main/MAPD_DataProcessing_2023.pdf) document.

## 2 Task
Starting from data collected by the DAQ of the detector, this project will require creating a streaming application to monitor basic detector quality plots as an online streaming application.  
Data should be produced to a Kafka topic by a stream-emulator script. The processing of the hits should then be performed in a distributed framework such as `Dask` or `pySpark`. The results of the processing will have to be re-injected into a new `Kafka` topic hosted on the same brokers.
A final consumer (ideally a simple python script) will perform the plotting and display live updates of the quantities evaluated online.
(`bokeh` or `pyplot` are good choices to create simple "dashboards", but other solutions can be applied for this task).

### 2.1 Data structure 
The dataset is provided on a cloud storage s3 bucket hosted on Cloud Veneto.  
Instructions: http://userguide.cloudveneto.it/en/latest/ManagingStorage.html#object-storage-experimental  
The dataset is composed of multiple `txt` files formatted as comma-separated values, where each record (row) can be associated with each new signal processed in the DAQ of the detector.
The data-format is the following:
- TDC_MEAS
- BX_COUNTER
- ORBIT_CNT
- TDC_CHANNEL
- FPGA
- HEAD

Data should be injected into a Kafka topic by a process emulating a continuous DAQ stream.  
A realistic data production rate is about 1000 records/sec (i.e. 1000 rows/sec). However, it might be beneficial to create a tunable-rate producer to adjust the emulated hit-rate.  

### 2.2 Data processing
The processing can be performed with either Spark or Dask.  
It is suggested to work with batches of data of a few (1 to 5) seconds.  
Remember that the shorter the batch size, the fastest the processing should be to "keepup" with the polling of the data; however, the longer the batch size, the larger the dataset to be processed for each batch.  
The data processing will have to start with a data-cleansing step where all entries with `HEAD != 2` must be dropped from the dataset, as they will provide ancillary information not useful in the context of the project.  
The mapping between the data-format and the detectors is the following:
- Chamber 0 → (`FPGA = 0`) `AND` (`TDC_CHANNEL` in `[0-63]`)
- Chamber 1 → (`FPGA = 0`) `AND` (`TDC_CHANNEL` in `[64-127]`)
- Chamber 2 → (`FPGA = 1`) `AND` (`TDC_CHANNEL` in `[0-63]`)
- Chamber 3 → (`FPGA = 1`) `AND` (`TDC_CHANNEL` in `[64-127]`)

The following information should be produced per each batch:
1. total number of processed hits, post-clensing (1 value per batch)
2. total number of processed hits, post-clensing, per chamber (4 values per batch)
3. histogram of the counts of active `TDC_CHANNEL`, per chamber (4 arrays per batch)
4. histogram of the total number of active `TDC_CHANNEL` in each `ORBIT_CNT`, per chamber (4 arrays per batch)

These pieces of information should be wrapped in one message per batch and injected into a new Kafka topic.

### 2.3 Live plotting
The results of the processing should be presented in the form of a continuously updating dashboard.  
A python (or another language) script should consume data from the topic, and create plots and histograms of the aforementioned quantities.
Simple python modules such as `bokeh` or `pyplot` (in its free version) can help create a continuously updating webpage where to visualize the plots. You are however encouraged to explore and apply any other solution for this part of the task you might consider viable.


## 3 Hint
- Start by prototyping the processing for a single static batch before extending to the streaming part
- Dealing with a structured dataset, it might be useful to use directly Dask/pySpark DataFrames instead of Bags/RDDs
- The Kafka messages results of the distributed processing could be formatted in a way to encode, for each histogram, an array of bins and counts. For instance, a json structured as follows might be one (although not the only one!) viable alternative:
```python
msg_json = {
  "histo_1" = { "bin_edges" = [...],
                "bin_counts"= [...]
              },
  "histo_2" = {...},
    ...
    }
```
---

