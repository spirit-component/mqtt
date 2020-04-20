package mqtt

import (
	"encoding/base64"
	"fmt"
	"github.com/spirit-component/mqtt/store"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/go-spirit/spirit/mail"
	"github.com/go-spirit/spirit/message"
	"github.com/go-spirit/spirit/worker/fbp"
	"github.com/go-spirit/spirit/worker/fbp/protocol"
	"github.com/gogap/config"

	_ "github.com/spirit-component/mqtt/store/file"
	_ "github.com/spirit-component/mqtt/store/memory"
)

type MQTTClient struct {
	client   mqtt.Client
	topic    string
	qos      byte
	broker   string
	username string

	quiesce uint
	postman mail.Postman
}

func NewMQTTClient(conf config.Configuration, postman mail.Postman) (ret *MQTTClient, err error) {

	clientConf := conf.GetConfig("client")

	clientID := clientConf.GetString("client-id")

	if len(clientID) == 0 {
		err = fmt.Errorf("client-id could not be empty")
		return
	}

	brokerServer := clientConf.GetString("broker-server")
	keepAlive := clientConf.GetTimeDuration("keep-alive")
	pingTimeout := clientConf.GetTimeDuration("ping-timeout")
	cleanSession := clientConf.GetBoolean("clean-session")
	quiesce := uint(clientConf.GetInt32("quiesce"))
	if quiesce == 0 {
		quiesce = 250 //ms
	}

	topic := clientConf.GetString("subscribe.topic")
	qos := byte(clientConf.GetInt32("subscribe.qos"))

	credentialName := clientConf.GetString("credential-name")

	if len(credentialName) == 0 {
		err = fmt.Errorf("credential name is empty")
		return
	}

	username := conf.GetString("credentials." + credentialName + ".username")
	password := conf.GetString("credentials." + credentialName + ".password")

	opts := mqtt.NewClientOptions()

	storeConf := clientConf.GetConfig("store")

	storeProvier := storeConf.GetString("provider")
	if len(storeProvier) > 0 {
		var mqttStore mqtt.Store
		mqttStore, err = store.NewStore(storeProvier, storeConf)
		if err != nil {
			return
		}
		opts.SetStore(mqttStore)
	}

	opts.AddBroker(brokerServer)
	opts.SetClientID(clientID)
	opts.SetUsername(username)
	opts.SetPassword(password)
	opts.SetCleanSession(cleanSession)
	opts.SetKeepAlive(keepAlive)
	opts.SetPingTimeout(pingTimeout)

	client := mqtt.NewClient(opts)

	ret = &MQTTClient{
		client: client,

		qos:      qos,
		topic:    topic,
		broker:   brokerServer,
		username: username,
		postman:  postman,
	}

	return
}

func (p *MQTTClient) Start() (err error) {

	if token := p.client.Connect(); token.Wait() && token.Error() != nil {
		err = token.Error()
		return
	}

	if len(p.topic) > 0 && p.postman != nil {
		if token := p.client.Subscribe(p.topic, p.qos, p.messageHandler); token.Wait() && token.Error() != nil {
			err = token.Error()
			return
		}
	}

	return
}

func (p *MQTTClient) Stop() (err error) {
	p.client.Disconnect(p.quiesce)
	return
}

type MQSendResult struct {
	Topic    string `json:"topic"`
	Broker   string `json:"broker"`
	Username string `json:"username"`
	MsgID    uint16 `json:"msg_id"`
	Qos      byte   `json:"qos"`
	Retained bool   `json:"retained"`
}

func (p *MQTTClient) SendMessage(topic string, qos byte, retained bool, msg []byte) (ret MQSendResult, err error) {

	token := p.client.Publish(topic, qos, retained, msg)

	if token.Wait() && token.Error() != nil {
		err = token.Error()
		return
	}

	pubToken := token.(*mqtt.PublishToken)

	ret = MQSendResult{
		Topic:    topic,
		Broker:   p.broker,
		Username: p.username,
		MsgID:    pubToken.MessageID(),
		Qos:      qos,
		Retained: retained,
	}

	return
}

func (p *MQTTClient) messageHandler(client mqtt.Client, msg mqtt.Message) {

	data, err := base64.StdEncoding.DecodeString(string(msg.Payload()))
	if err != nil {
		return
	}

	payload := &protocol.Payload{}
	err = protocol.Unmarshal(data, payload)

	if err != nil {
		return
	}

	graph, exist := payload.GetGraph(payload.GetCurrentGraph())

	if !exist {
		err = fmt.Errorf("could not get graph of %s in MQTTComponent.postMessage", payload.GetCurrentGraph())
		return
	}

	graph.MoveForward()

	port, err := graph.CurrentPort()

	if err != nil {
		return
	}

	fromUrl := ""
	prePort, preErr := graph.PrevPort()
	if preErr == nil {
		fromUrl = prePort.GetUrl()
	}

	session := mail.NewSession()

	session.WithPayload(payload)
	session.WithFromTo(fromUrl, port.GetUrl())

	fbp.SessionWithPort(session, graph.GetName(), port.GetUrl(), port.GetMetadata())

	err = p.postman.Post(
		message.NewUserMessage(session),
	)

	if err != nil {
		return
	}

	return
}
