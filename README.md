# Kafka - Storm


## Maven Build
`mvn clean install`

## SCP to EC2
	I configured the EC2 cluster in my ~/.ssh/config file.
	
`scp target/FirstTopology-1.0-SNAPSHOT-jar-with-dependencies.jar EC2:/tmp`

## Submit Storm

`storm jar /tmp/FirstTopology-1.0-SNAPSHOT-jar-with-dependencies.jar sheng.FirstTopology`

## Check the worker log
`tail -100f $STORM_HOME/logs/worker-*.log`


# Kafka on EC2

### My Tiny Storm / Kafka Cluster on EC2
	http://ec2-54-191-68-82.us-west-2.compute.amazonaws.com:8080/index.html
	
### Producer
`bin/kafka-console-producer.sh --broker-list  ec2-54-191-68-82.us-west-2.compute.amazonaws.com:9092 --topic firstT`

Sheng
 
