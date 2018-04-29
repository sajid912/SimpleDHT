# SimpleDHT-DS

This project focuses on implementing Distributed Hash Table, shortly called DHT using Chord ring based routing.
It is a simplified version of Chord.

In this project, we do the following things
• Implement ID space partitioning and re-partitioning
• Implement ring-based routing
• Handle node joins to a DHT containing data

The content provider implements ring-based routing (not Chord finger routing or finger tables). 
Following the Chord ring design, the content provider maintains predecessor and successor pointers, 
then forward each request for data not stored locally to its successor until the request arrives at the correct node. 
When a node receives a request for an ID that it maintains, it processes the request and 
sends the result to the content provider instance initiating the request.

The content provider also handles node joins. 
For this functionality, the emulator instance emulator-5554 receives all new node join requests. 
We don't choose a random node to receive new node join requests, and we start the content
provider on emulator-5554 first to enable this. upon completing a new node join
request, affected nodes update their predecessor and successor pointers correctly.

Note: It does not implement finger based routing or finger tables, neither does it handle nodes leaving the ring.
