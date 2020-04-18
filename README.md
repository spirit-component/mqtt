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
    broker-server   = ""

    keep-alive    = 3s
    ping-timeout  = 1s
    clean-session = false
    quiesce = 250

    subscribe = {
        topic = "API"
        qos   = 0
    }

    store {
        provider  = in-memory
    }
}
```