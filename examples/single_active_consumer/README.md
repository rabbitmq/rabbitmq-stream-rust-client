Single active consumer
---

This is an example to enable single active consumer functionality for superstream:
https://www.rabbitmq.com/blog/2022/07/05/rabbitmq-3-11-feature-preview-single-active-consumer-for-streams
https://www.rabbitmq.com/blog/2022/07/13/rabbitmq-3-11-feature-preview-super-streams

This folder contains a super-stream consumer configured to enable it.
You can use the example in the super-stream folder to produce messages for a super-stream.

You can then run the consumer in this folder.
Assuming the super-stream is composed by three streams, you can see that the Consumer will consume messages from all the streams part of the superstream.

You can then run another consumer in parallel. 
now you'll see that one of the two consumers will consume from 2 streams while the other on one stream.

If you run another you'll see that every Consumer will read from a single stream.

If you then stop one of the Consumer you'll notice that the related stream is now read from on the Consumer which is still running.




