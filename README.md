KafkaNetClient
=========

Native C# client for Apache Kafka with full async support.

Kafka versions 0.8, 0.9 & 0.10.

License
-----------
Copyright 2016, Nudge Software Inc under Apache License, V2.0. See LICENSE file.

History
-----------
This library is a fork of [gigya]'s [KafkaNetClient], itself a fork of [jroland]'s [kafka-net] library. There are significant API changes.

The original .Net project is a port of the [Apache Kafka protocol]. The wire protocol portion is based on the [kafka-python] library writen by [David Arthur] and the general class layout attempts to follow a similar pattern as his project. To that end, this project builds up from the low level Connection object for handling async requests to/from the kafka server, all the way up to a higher level Producer/Consumer classes.

Code Examples
-----------
##### Producer
```sh
var options = new KafkaOptions(new Uri("http://SERVER1:9092"), new Uri("http://SERVER2:9092"));
var router = new BrokerRouter(options);
using(var client = new Producer(router))
{
     await client.SendMessageAsync("TestTopic", new Message("hello world"));
}


```
##### Consumer
```sh
var options = new KafkaOptions(new Uri("http://SERVER1:9092"), new Uri("http://SERVER2:9092"));
var router = new BrokerRouter(options);
var consumer = new Consumer(new ConsumerOptions("TestHarness", new BrokerRouter(options)));

//Consume returns a blocking IEnumerable (ie: never ending stream)
foreach (var message in consumer.Consume())
{
    Console.WriteLine("Response: P{0},O{1} : {2}",
        message.Meta.PartitionId, message.Meta.Offset, message.Value);  
}
```

##### ExampleApp
The ExampleApp project it a simple example console application that will read message from a kafka server and write them to the screen.  It will also take anything typed in the console and send this as a message to the kafka servers.  

Simply modify the kafka server Uri in the code to point to a functioning test server.


Key Components
-----------
##### Connection (`KafkaClient.Connection.IConnection`)
Provides async methods on a persistent connection to a kafka broker (server).  The send method uses the TcpClient sendAsync function and the read stream has a dedicated thread which uses the correlation Id to match send responses to the correct request.

##### Producer
Provides a high level abstraction for sending batches of messages to a Kafka cluster. Internally, it uses the IBrokerRouter and IConnection.

##### Consumer
Provides a high level abstraction for receiving messages from a Kafka cluster. It will consumer messages from a whitelist of partitions from a single topic.  The consumption mechanism is a blocking IEnumerable of messages. If no whitelist is provided then all partitions will be consumed, creating one Connection for each partition leader.

##### Partition Selection (`KafkaClint.IPartitionSelector`)
Provides the logic for routing requests to a particular topic to a partition.  The default selector (PartitionSelector) will use round robin partition selection if the key property on the message is null and a mod/hash of the key value if present.

##### BrokerRouter (`KafkaClient.IBrokerRouter`)
Provides routing for topics to partitions, topics and partitions to brokers, and cached access to topic metadata. This class also manages the multiple IConnections to each Kafka broker. Routing logic for partitions is provided by IPartitionSelector.

##### Binary Protocol (`KafkaClient.Protocol.*`)
The protocol has been divided up into concrete classes for each request/response pair. An Encoder class knows how to encode requests and decode responses into/from their appropriate Kafka protocol byte array. One benefit of this is that it allows for a nice generic send method on the Connection.

#### ManualConsumer *
A class which enables simple manual consuming of messages which encapsulates the Kafka protocol details and enables message fetching, offset fetching, and offset updating. All of these operations are on demand.

Status
-----------
[![Build status](https://ci.appveyor.com/api/projects/status/e7ej2g9q77if8mkf/branch/master?svg=true)](https://ci.appveyor.com/project/AndrewRobinson/kafkanetclient/branch/master)

This library is still work in progress and has not yet been deployed to production. We will update when it does.

##### The major items that needs work are:
* Full support for consumer groups, with consumer coordination and server heartbeats
* Support for Kafka 0.9.0 and 0.10.0
* Better handling of options for providing customization of internal behaviour of the base API. (right now the classes pass around option parameters)
* General structure of the classes is not finalized and breaking changes will occur.
* Only Gzip compression is implemented, snappy on the todo.
* Currently only works with .NET Framework 4.5 as it uses the await command.
* Test coverage.
* Documentation.



[Apache Kafka protocol]:https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol
[kafka-python]:https://github.com/mumrah/kafka-python
[David Arthur]:https://github.com/mumrah
[kafka-net]:https://github.com/Jroland/kafka-net
[jroland]:https://github.com/jroland
[KafkaNetClient]:https://github.com/gigya/KafkaNetClient
[gigya]:https://github.com/gigya
