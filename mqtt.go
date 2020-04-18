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
	"github.com/go-spirit/spirit/worker"
	"github.com/go-spirit/spirit/worker/fbp"
	"github.com/go-spirit/spirit/worker/fbp/protocol"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

type ConsumeFunc func(msg mqtt.Message) (err error)

type MQTTComponent struct {
	opts component.Options

	alias string

	client       *MQTTClient
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

	p.client, err = NewMQTTClient(p.opts.Config, p.opts.Postman)
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
