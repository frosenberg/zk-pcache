# Zookeeper PathCache implementation

Taken from [here](https://github.com/skynetservices/zkmanager) and extracted to be standalone.


### Run tests

Run Zookeeper instance:
```
docker run -d --rm -p 2181:2181 jplock/zookeeper
```

Run tests:

```
go test
```

If Zookeeper is not accessible via `localhost:2181`, then run the tests as follows:

```
ZK_SERVER=<ZK_IP>:2181 go test
```
