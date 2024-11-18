Super stream example
---

[Super Streams Documentation](https://www.rabbitmq.com/streams.html#super-streams) for more details.
[Super Streams blog post](https://blog.rabbitmq.com/posts/2022/07/rabbitmq-3-11-feature-preview-super-streams)


This example shows how to use the Super Stream feature in RabbitMQ 3.11.0.

Then run the producer in one terminal:

    $  cargo run -- --producer


And the consumer in another terminal:

    $ cargo run -- --consumer

You should see the consumer receiving the messages from the producer.

It would be something like:
```bash
$ cargo run -- --producer
Starting SuperStream Producer
Super Stream Producer connected to RabbitMQ
Super Stream Producer sent 0 messages to invoices
Super Stream Producer sent 1 messages to invoices
Super Stream Producer sent 2 messages to invoices
Super Stream Producer sent 3 messages to invoices
```

```bash
$ dotnet run --consumer my_first_consumer
Starting SuperStream Consumer my_first_consumer
Super Stream Consumer connected to RabbitMQ. ConsumerName my_first_consumer
Consumer Name my_first_consumer -Received message id: hello0 body: hello0, Stream invoices-2
Consumer Name my_first_consumer -Received message id: hello1 body: hello1, Stream invoices-1
Consumer Name my_first_consumer -Received message id: hello2 body: hello2, Stream invoices-0
Consumer Name my_first_consumer -Received message id: hello3 body: hello3, Stream invoices-1
Consumer Name my_first_consumer -Received message id: hello4 body: hello4, Stream invoices-2
Consumer Name my_first_consumer -Received message id: hello5 body: hello5, Stream invoices-0
Consumer Name my_first_consumer -Received message id: hello6 body: hello6, Stream invoices-2
Consumer Name my_first_consumer -Received message id: hello7 body: hello7, Stream invoices-0
Consumer Name my_first_consumer -Received message id: hello8 body: hello8, Stream invoices-1
Consumer Name my_first_consumer -Received message id: hello9 body: hello9, Stream invoices-0
```


To see the Single active consumer in action, run another consumer:

    $ dotnet run --consumer my_second_consumer

You should see the second consumer receiving the part of the messages from the producer. In thi case only the messages coming from the `invoices-1`.

It should be something like:
```bash
$ dotnet run --consumer my_second_consumer
Starting SuperStream Consumer my_second_consumer
Super Stream Consumer connected to RabbitMQ. ConsumerName my_second_consumer
Consumer Name my_second_consumer -Received message id: hello1 body: hello1, Stream invoices-1
Consumer Name my_second_consumer -Received message id: hello3 body: hello3, Stream invoices-1
Consumer Name my_second_consumer -Received message id: hello8 body: hello8, Stream invoices-1
Consumer Name my_second_consumer -Received message id: hello14 body: hello14, Stream invoices-1
Consumer Name my_second_consumer -Received message id: hello15 body: hello15, Stream invoices-1
Consumer Name my_second_consumer -Received message id: hello16 body: hello16, Stream invoices-1
Consumer Name my_second_consumer -Received message id: hello19 body: hello19, Stream invoices-1
```
and the first consumer should be receiving the rest of the messages:
```bash
Consumer Name my_first_consumer -Received message id: hello0 body: hello0, Stream invoices-2
Consumer Name my_first_consumer -Received message id: hello2 body: hello2, Stream invoices-0
Consumer Name my_first_consumer -Received message id: hello4 body: hello4, Stream invoices-2
Consumer Name my_first_consumer -Received message id: hello5 body: hello5, Stream invoices-0
Consumer Name my_first_consumer -Received message id: hello6 body: hello6, Stream invoices-2
Consumer Name my_first_consumer -Received message id: hello7 body: hello7, Stream invoices-0
Consumer Name my_first_consumer -Received message id: hello9 body: hello9, Stream invoices-0
Consumer Name my_first_consumer -Received message id: hello10 body: hello10, Stream invoices-2
Consumer Name my_first_consumer -Received message id: hello11 body: hello11, Stream invoices-0
```




