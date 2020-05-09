MQTT
====
```
 
credentials = {
    c1 = {
        username = "username"
        password = "password"
    }
}

client {
    credential-name = "c1"
    client-id       = ""
    broker-server   = "127.0.0.1:1883"

    keep-alive    = 3s
    ping-timeout  = 1s
    clean-session = false
    quiesce = 250

    subscribe = {
        topic-01 {
            topic = "API"
            qos   = 0
        }
    }

    store {
        provider  = in-memory
    }
}
```