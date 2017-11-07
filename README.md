# Distributed Objects Course Project

## Dependencies
- Maven
- Java

## Build
- Run `build.sh`, will use Maven to compile project and prepare binaries to run.

## Run
- Build first!
### Client
- Run `client.sh`

### Server
- Run `server.sh`

### Parameters
These must be provided as system properties (eg. `./server.sh -Da=b -Dc=d`). As for which values are acceptable, see
[ClientArguments](https://github.com/lipusal/POD-TPE/blob/master/client/src/main/java/ar/edu/itba/client/util/ClientArguments.java#L25)
or [ServerArguments](https://github.com/lipusal/POD-TPE/blob/master/server/src/main/java/ar/edu/itba/server/util/ServerArguments.java#L18). 
