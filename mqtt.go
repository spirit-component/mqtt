package mqtt

import (
	"encoding/base64"
	"fmt"
	"strconv"
	"sync"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/go-spirit/spirit/component"
	"github.com/go-spirit/spirit/doc"
	"github.com/go-spirit/spirit/mail"
	"github.com/go-spirit/spirit/message"
	"github.com/go-spirit/spirit/worker"
	"github.com/go-spirit/spirit/worker/fbp"
	"github.com/go-spirit/spirit/worker/fbp/protocol"
	"github.com/gogap/tinymqtt"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

type ConsumeFunc func(msg mqtt.Message) (err error)

type MQTTComponent struct {
	opts component.Options

	alias string

	client       *tinymqtt.MQTTClient
	producerLock sync.RWMutex
}

func init() {
	component.RegisterComponent("mqtt", NewMQTTComponent)
	doc.RegisterDocumenter("mqtt", &MQTTComponent{})
}

func (p *MQTTComponent) Alias() string {
	if p == nil {
		return ""
	}
	return p.alias
}

func (p *MQTTComponent) Start() error {
	return p.client.Start()
}

func (p *MQTTComponent) Stop() (err error) {

	err = p.client.Stop()
	if err != nil {
		return
	}

	return
}

func NewMQTTComponent(alias string, opts ...component.Option) (comp component.Component, err error) {

	rmqComp := &MQTTComponent{
		alias: alias,
	}

	err = rmqComp.init(opts...)
	if err != nil {
		return
	}

	comp = rmqComp

	return
}

func (p *MQTTComponent) init(opts ...component.Option) (err error) {

	for _, o := range opts {
		o(&p.opts)
	}

	if p.opts.Config == nil {
		err = errors.New("mqtt component config is nil")
		return
	}

	subscribeConf := p.opts.Config.GetConfig("subscribe")

	var subOptions []tinymqtt.SubscribeOption

	for _, key := range subscribeConf.Keys() {
		topic := subscribeConf.GetString(key + ".topic")
		qos := int(subscribeConf.GetInt32("key" + ".qos"))

		if len(topic) == 0 {
			logrus.WithField("config-key", key).Warnln("topic is empty")
			continue
		}

		subOptions = append(
			subOptions,
			tinymqtt.SubscribeOption{
				Topic:   topic,
				Qos:     byte(qos),
				Handler: p.messageHandler,
			},
		)
	}

	p.client, err = tinymqtt.NewMQTTClient(p.opts.Config, subOptions...)

	if err != nil {
		return
	}

	return
}

func (p *MQTTComponent) Route(mail.Session) worker.HandlerFunc {
	return p.sendMessage
}

func (p *MQTTComponent) sendMessage(session mail.Session) (err error) {

	logrus.WithField("component", "mqtt").WithField("To", session.To()).Debugln("send message")

	setBody := session.Query("setbody")

	if len(setBody) == 0 {
		fbp.BreakSession(session)
	}

	port := fbp.GetSessionPort(session)

	if port == nil {
		err = errors.New("port info not exist")
		return
	}

	topic := session.Query("topic")
	qosStr := session.Query("qos")
	retainedStr := session.Query("retained")

	if len(qosStr) == 0 {
		qosStr = "0"
	}

	if len(retainedStr) == 0 {
		retainedStr = "false"
	}

	qos, err := strconv.Atoi(qosStr)
	if err != nil {
		err = fmt.Errorf("parse cos failure %w", err)
		return
	}

	retained, err := strconv.ParseBool(retainedStr)
	if err != nil {
		err = fmt.Errorf("parse retained failure %w", err)
		return
	}

	toClientID, _ := session.Payload().Content().HeaderOf("MQTT-TO-CLIENT-ID")
	if len(toClientID) == 0 {
		toClientID = session.Query("to_client_id")
	}

	if len(topic) == 0 {
		err = fmt.Errorf("query of %s is empty", "topic")
		return
	}

	partition := session.Query("partition")
	errorGraph := session.Query("error")
	entrypointGraph := session.Query("entrypoint")

	var sendSession mail.Session = session

	if partition == "1" {
		var newSession mail.Session
		newSession, err = fbp.PartitionFromSession(session, entrypointGraph, errorGraph)
		if err != nil {
			return
		}
		sendSession = newSession
	}

	payload, ok := sendSession.Payload().Interface().(*protocol.Payload)
	if !ok {
		err = errors.New("could not convert session payload to *protocol.Payload")
		return
	}

	data, err := payload.ToBytes()
	if err != nil {
		return
	}

	msgBody := base64.StdEncoding.EncodeToString(data)

	sendResult, err := p.client.SendMessage(topic, byte(qos), retained, []byte(msgBody))

	if err != nil {
		return
	}

	if setBody == "1" {
		err = session.Payload().Content().SetBody(&sendResult)
		if err != nil {
			return
		}
	}

	return
}

func (p *MQTTComponent) Document() doc.Document {

	document := doc.Document{
		Title:       "MQTT Sender And Receiver",
		Description: "we could receive queue message from mqtt and send message to mqtt",
	}

	return document
}

func (p *MQTTComponent) messageHandler(client mqtt.Client, msg mqtt.Message) {

	data, err := base64.StdEncoding.DecodeString(string(msg.Payload()))
	if err != nil {
		logrus.WithError(err).Errorln("decode message failure")
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

	err = p.opts.Postman.Post(
		message.NewUserMessage(session),
	)

	if err != nil {
		return
	}

	return
}
