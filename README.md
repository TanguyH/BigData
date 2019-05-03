# Big Data: Project Assingment Phase 1

### Requirements
This project assumes PySpark was installed. To avoid any confusion in Kafka and Zookeeper versions, we have included the binaries in this file. The following modules should be installed for Python 3:
* Kafka
* Pandas
* Pymongo
* Numpy
* Seaborn

As a reminder, these packages can be installed using the commind line tool and typing: 
```
pip3 install package_name
```

The created dashboard also requires installatition of the Jupyter Dashboard Layout extension with following commands:
```
pip3 install jupyter_dashboards
jupyter dashboards quick-setup --sys-prefix
```

### Execution

##### Preparig data
Data that is streamed accross the pipeline is obtained through the original file **data.conv.txt**. As this file is too big for GitHub, it should be placed in  the data directory.

This file contains (possibly faulty) information in non-chronological order. However, key assumptions of our project are that no faulty information is emitted, and that information is streamed in chronological order. For this purpose, the following command is to be executed:
```
python3 data_sorter.py
```

**Note:** this execution might take time..

##### Starting the pipeline
In the zookeeper folder, the conf folder contains a file "zoo.conf". Modify the path contained in that folder to contain the absolute location to that location.

In order to get the pipeline running, several servers should be instatiated: ZooKeeper, Kafka and MongoDB. To make this process easier, simply execute following command in Terminal:
```
sh start_backend.sh
```

As we know, producers and consumers in Kafka obey the pub-sub messaging queue paradigm. Normally, when producers (consumers) publish to (consume from) a topic, it is automatically created. If this is not the case, the following command creates the required topics:
```
sh create_topics.sh
```
The inverse operation is also made available through:
```
sh delete_topics.sh
```

Now the pipeline is initialized, data can be streamed on it by using the following command:
```
python3 stream_to_kafka.py
```
**Note:** this might take some time before streaming

Once we dispose of an streaming pipeline, we should capture streamed data using the queries described in the assignment. For this purpose, simply open a new Terminal and run:
```
sh start_queries.sh
```

As from now, the three queries are run on streamed data and results are being stored in the database. The next step is to ensure that data is indeed being received on the other side of the pipeline. As described in the report, we have a consumer for each sensor type. Running the following command will initialize them all:
```
sh start_consumers.sh
```

##### Loading known data in MongoDB
Together with the assignment, we have recieved a file that contains information of a paricular space (the lab). To be able to respond to queries correctly, this data is to be loaded in database. Additionnaly, it should be assigned a privacy value (public or private) as well as a municipality. This is done by executing the following script:
```
python3 municipality_dispatcher.py
```
**Note:** the backend must be running to load data into the database

Additionnaly, we know that developement and analysis features were also required. For this purpose, a big part of this script was commented out. This part generates locations and assigns sensors to it in a random way. Currrently, it is set to create 10,000 locations in which 100,000 sensors are dispersed.

##### Visualizing the dashboard
To visualize the dashboard, one simply requires to run jupyter with following command in an empty Terminal:
```
jupyter notebook
```

By selecting the file with .ipynb, you will have access to the Dashboard and be able to visualize queries.