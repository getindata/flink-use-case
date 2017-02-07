# flink-use-case

## Run docker containers

### Prerequisites

[docker](https://www.docker.com/products/overview) >= 1.12.5

[docker-compose](https://docs.docker.com/compose/install/) >= 1.10

### Run containers

You can start containers by invoking:

```
    docker-compose -f ${FLINK_USE_CASE_DIR}/infrastructure/docker-compose.yml up -d
```

#### Linux

Then configure your `/etc/hosts` file at least for jobmanager, kafka and grafana approprietly.
You can check container ip by running (example for jobmanager):

```
    docker inspect infrastructure_jobmanager_1
```

#### Mac

On MacOS you can't connect to containers by their ips from host, so one have to use post forwarding.
Jobmanager UI is accessible at _localhost:48081_. Unfortunately kafka can be accessed only from within a container.

## Create fat jars

To build fat jars invoke:

```
    sbt assembly
```

Then you will get 3 interesting jars:

* `${FLINK_USE_CASE_DIR}/event-generator/target/scala-2.11/event-generator-assembly-1.0.jar`
* `${FLINK_USE_CASE_DIR}/flink-processing/target/scala-2.11/flink-processing-assembly-1.0.jar`
* `${FLINK_USE_CASE_DIR}/beam-processing/target/scala-2.11/beam-processing-assembly-1.0.jar`

## Submitting flink jobs

Flink has a very nice feature of submitting jobs from _FlinkWebUI_ which I recommend.
One can just upload fat-jars and specify program arguments.

## Event generator

This module generates random events and write them to kafka topic. As 
a kafka broker in the docker environmet one can pass `kafka:9092`


```
  -m, --max-interval  <arg>         Maximal interval in millis between two
                                    consecutive events (default: 1000)
      --max-time-deviation  <arg>   Maximal deviation in millis from current time while
                                    assigning event timestamp (default 0)
  -t, --topic  <arg>                Kafka topic to write (default: songs)
      --help                        Show help message

 trailing arguments:
  kafka-broker (required)   Kafka broker list
```

## Flink-processing

This module performs processing the use case requirements with flink as runtime.
As a kafka broker in the docker environmet one can pass `kafka:9092`

```
  -d, --discover-weekly
      --nodiscover-weekly         disable processing consecutive discover weekly
                                  events
  -s, --session-gap  <arg>        Maximal session inactivity in seconds
                                  (default: 20)
      --session-stats
      --nosession-stats           disable processing session stats
      --subsession-stats
      --nosubsession-stats        disable processing subsession stats
  -t, --topic  <arg>              Kafka topic to read (default: songs)
      --trigger-interval  <arg>   Intervals in which to early trigger windows in
                                  seconds (default: 5).
      --help                      Show help message

 trailing arguments:
  kafka-broker (required)   Kafka broker list
```

The results are printed out into stdout. One can read the stdout of taskmanager from _FlinkWebUI_.

## Beam-processing

Right now one can run the example with direct-runner. Just copy the generated fat-jar
to one of the docker-container. (Personally recommend the kafka container).

You can do it by:

```
docker cp beam-processing/target/scala-2.11/beam-processing-assembly-1.0.jar infrastructure_kafka_1:/home
```

Then exec bash within the container you've copied the jar to.

```
docker exec -it infrastructure_kafka_1 bash
```

Then execute the copied jar:

```
java -jar beam-processing-assembly-1.0.jar kafka:9092
```

Arguments:

```
  -d, --discover-weekly-write-topic  <arg>   Kafka topic to write (default:
                                             discover-weekly-stats)
      --session-gap  <arg>                   Maximal session inactivity in
                                             seconds (default: 20)
  -s, --session-write-topic  <arg>           Kafka topic to write (default:
                                             session-stats)
      --subsession-write-topic  <arg>        Kafka topic to write (default:
                                             subsession-stats)
  -t, --topic  <arg>                         Kafka topic to read (default:
                                             songs)
      --help                                 Show help message

 trailing arguments:
  kafka-broker (required)   Kafka broker list
```

The resulting messages are written back to kafka by default to topic: `session-stats`. You can read them for example by using kafka-console-consumer.