Engine
======
> Distributed identifier generation service

## Components

* Engine
* Supplier
* Controller
* Generator

### Engine

* Tornado powered web-server which returns next set of IDs to fetch and current system stats

### Supplier

* Keeps an eye on the storage system using distributed locking
* Returns state of the system and storage
* Collect perpared set of IDs from storage and pass it to 'Engine' on demand
* Invokes 'controller' when 'master' storage is in 'caution' state

### Controller

* Shift main system storage between 'master' and 'slave'
* Insert data from 'generator' into 'slave' storage on demand

### Generator

* Generate *non-sequential* identifiers of *fixed-length*
* Uses a little *Group Theory* and properties of *Anagrams*

### Storage

* RethinkDB
