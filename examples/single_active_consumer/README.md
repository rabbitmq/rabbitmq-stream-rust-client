Single active consumer example
---

[Single-Active-Consumer Java doc](https://rabbitmq.github.io/rabbitmq-stream-java-client/stable/htmlsingle/#single-active-consumer) for more details
[single-active-consumer blog post](https://www.rabbitmq.com/blog/2022/07/05/rabbitmq-3-11-feature-preview-single-active-consumer-for-streams)


This example shows how to use the Single active consumer feature in RabbitMQ 3.11.0.

Then run the producer in one terminal:

    $  cargo run --release -- --producer


And the consumer in another terminal:

    $ cargo run --release -- --consumer

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
$ cargo run --release -- --consumer my_first_consumer
Starting SuperStream Consumer my_first_consumer
Super Stream Consumer connected to RabbitMQ. ConsumerName my_first_consumer
Consumer Name my_first_consumer: Got message: super_stream_message_1 from stream: invoices-1 with offset: 33 
Consumer Name my_first_consumer: Got message: super_stream_message_2 from stream: invoices-2 with offset: 34 
Consumer Name my_first_consumer: Got message: super_stream_message_3 from stream: invoices-0 with offset: 37 
Consumer Name my_first_consumer: Got message: super_stream_message_4 from stream: invoices-0 with offset: 36 
Consumer Name my_first_consumer: Got message: super_stream_message_5 from stream: invoices-1 with offset: 39 
Consumer Name my_first_consumer: Got message: super_stream_message_6 from stream: invoices-2 with offset: 40 
Consumer Name my_first_consumer: Got message: super_stream_message_7 from stream: invoices-0 with offset: 41 
Consumer Name my_first_consumer: Got message: super_stream_message_8 from stream: invoices-1 with offset: 42 
Consumer Name my_first_consumer: Got message: super_stream_message_9 from stream: invoices-2 with offset: 43 
Consumer Name my_first_consumer: Got message: super_stream_message_10 from stream: invoices-1 with offset: 44 
```

To see the Single active consumer in action, run another consumer:

    $ cargo run --release -- --consumer my_second_consumer

You should see the second consumer receiving the part of the messages from the producer. In thi case only the messages coming from the `invoices-1`.

It should be something like:
```bash
$ cargo run --release -- --consumer my_second_consumer
Starting SuperStream Consumer my_second_consumer
Super Stream Consumer connected to RabbitMQ. ConsumerName my_second_consumer
Consumer Name my_second_consumer: Got message: super_stream_message_64 from stream: invoices-1 with offset: 86 
Consumer Name my_second_consumer: Got message: super_stream_message_65 from stream: invoices-1 with offset: 87
Consumer Name my_second_consumer: Got message: super_stream_message_66 from stream: invoices-1 with offset: 88
Consumer Name my_second_consumer: Got message: super_stream_message_67 from stream: invoices-1 with offset: 89 
Consumer Name my_second_consumer: Got message: super_stream_message_68 from stream: invoices-1 with offset: 90
Consumer Name my_second_consumer: Got message: super_stream_message_69 from stream: invoices-1 with offset: 90
Consumer Name my_second_consumer: Got message: super_stream_message_70 from stream: invoices-1 with offset: 90
```
and the first consumer should be receiving the rest of the messages:
```bash
Consumer Name my_first_consumer: Got message: super_stream_message_88 from stream: invoices-0 with offset: 92 
Consumer Name my_first_consumer: Got message: super_stream_message_87 from stream: invoices-2 with offset: 93
Consumer Name my_first_consumer: Got message: super_stream_message_89 from stream: invoices-2 with offset: 95
Consumer Name my_first_consumer: Got message: super_stream_message_90 from stream: invoices-0 with offset: 97 
Consumer Name my_first_consumer: Got message: super_stream_message_91 from stream: invoices-0 with offset: 96
Consumer Name my_first_consumer: Got message: super_stream_message_92 from stream: invoices-2 with offset: 99
Consumer Name my_first_consumer: Got message: super_stream_message_93 from stream: invoices-2 with offset: 101
```




