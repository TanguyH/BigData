# Big Data: Project Assingment Phase 1

### Requirements
This project assumes PySpark was installed. To avoid any confusion in Kafka and Zookeeper versions, we have included the binaries in this file. The following modules should be installed for Python 3:
* Kafka
* Pandas
* Pymongo
* Numpy
* Seaborn

As a reminder, these packages can be installed using the commind line tool and typing: `pip3 install package_name` 

The created dashboard also requires installation of the Jupyter Dashboard Layout extension with following commands:
```
pip3 install jupyter_dashboards
jupyter dashboards quick-setup --sys-prefix
```

### Execution

##### Preparing data
Data that is streamed accross the pipeline is obtained through the original file **data.conv.txt**. As this file is too big for GitHub, it should be placed in  the data directory.

This file contains (possibly faulty) information in non-chronological order. However, key assumptions of our project are that no faulty information is emitted, and that information is streamed in chronological order. For this purpose, the following command is to be executed:
```
python3 data_sorter.py
```

**Note:** this execution might take time..

##### Starting the pipeline (in src/ folder)

In order to get the pipeline running, several servers should be instantiated: ZooKeeper, Kafka and MongoDB. To make this process easier, simply execute following command in Terminal:
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

As from now, we ensure that data is indeed being sent to the pipeline. This is done by executing:
```
python3 stream_to_kafka.py
```

Once we dispose of an initialized pipeline, we should capture streamed data using the queries described in the assignment. For this purpose, simply open a new Terminal and run:
```
sh start_reception.sh
```

##### Visualizing the dashboard
To visualize the dashboard, one simply requires to run jupyter with following command in an empty Terminal:
`jupyter notebook`

By selecting the file with .ipynb, you will have access to the Dashboard and be able to visualize queries.

##### Ending the pipeline
The scripts we use make a lot of tasks run in the background. For this purpose, we have decided to make ending these tasks easier by running following command:
```
sh kill_daemons.sh
```