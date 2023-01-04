## About this project

This is a proof of concept of the CQRS inspired pattern for writing asynchronously in the database. To be specific, this repository contains the eventual consistency step called ALeRCE Scribe. This step will consume from a specific topic, where insert or update commands will be published from the different steps of the pipeline during it's execution.

## Things to do and consider

 - The event broker (Kafka or SQS) **must** work as a queue and guarantee the order of the messages, this is important when updating a previously inserted object
 - Would be a good idea to requeue rejected messages at least once
 - We need to define the format of the commands so we can write an adequate parser (and maybe create a shared class)