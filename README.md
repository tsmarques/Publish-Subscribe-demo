# Publish-Subscribe Service

This is a simple implementation of a Publish-Subscribe service for streaming data. All the data are random floats generated at publication time. The main class **EventBus** implements the following functions:

  - **subscribe < s >** - register the subscriber associated with this message as a subscriber for stream **s**;
  
  - **unsubscribe < s >** - remove the subscriber associated with this message from the list of subscribers of stream **s**;
  
  - **list** - send the subscriber associated with the message a complete list of the streams available for subscription;
  
  - **publish < s >** - register the IP/Socket associated with the message as a publisher of the stream **s**;
  
  - **unpublish < s >** - remove the IP/Socket associated with the message as a publisher of stream **s**;
  
  - **forward** - publish to all streams this IP/Sockets is a publisher of, and send the data to the subscribers.
  
  - **forward < s >** - publish data to stream **s** and send the data go the subscribers..
