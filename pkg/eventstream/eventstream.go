// Copyright (c) 2024 AccelByte Inc. All Rights Reserved.
// This is licensed software from AccelByte Inc, for limitations
// and restrictions contact your company contract manager.

package eventstream

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	validator "github.com/AccelByte/justice-input-validation-go"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/sirupsen/logrus"
	"time"
)

const (
	eventStreamNull   = "none"
	eventStreamStdout = "stdout"
	eventStreamKafka  = "kafka"

	actorTypeUser   = "USER"
	actorTypeClient = "CLIENT"

	slugSeparator = "$"
)

const (
	separator      = "." // topic prefix separator
	defaultVersion = 1
	dlq            = "dlq"
)

// log level
const (
	OffLevel   = "off"
	InfoLevel  = "info"
	DebugLevel = "debug"
	WarnLevel  = "warn"
	ErrorLevel = "error"
)

// Event defines the structure of event
type Event struct {
	ID               string                 `json:"id,omitempty"`
	EventName        string                 `json:"name,omitempty"`
	ClientID         string                 `json:"clientId,omitempty"`
	TraceID          string                 `json:"traceId,omitempty"`
	SpanContext      string                 `json:"spanContext,omitempty"`
	UserID           string                 `json:"userId,omitempty"`
	Timestamp        string                 `json:"timestamp,omitempty"`
	Version          int                    `json:"version,omitempty"`
	EventID          int                    `json:"event_id,omitempty"`
	EventType        int                    `json:"event_type,omitempty"`
	EventLevel       int                    `json:"event_level,omitempty"`
	ServiceName      string                 `json:"service,omitempty"`
	ClientIDs        []string               `json:"client_ids,omitempty"`
	TargetUserIDs    []string               `json:"target_user_ids,omitempty"`
	Privacy          bool                   `json:"privacy,omitempty"`
	Topic            string                 `json:"topic,omitempty"`
	AdditionalFields map[string]interface{} `json:"additional_fields,omitempty"`
	Payload          map[string]interface{} `json:"payload,omitempty"`

	Partition int    `json:",omitempty"`
	Offset    int64  `json:",omitempty"`
	Key       string `json:",omitempty"`
}

var (
	NotificationEventNamePath       = "name"
	FreeformNotificationUserIDsPath = []string{"payload", "userIds"}
)

// BrokerConfig is custom configuration for message broker
type BrokerConfig struct {
	StrictValidation bool
	CACertFile       string
	DialTimeout      time.Duration
	SecurityConfig   *SecurityConfig

	// Enable auto commit on every consumer polls when the interval has stepped in.
	// Enabling it will override CommitBeforeProcessing config. Default: 0 (disabled).
	AutoCommitInterval time.Duration

	// Enable committing the message offset right after consumer polls and before the message is processed.
	// Otherwise, the message offset will be committed after it is processed.
	CommitBeforeProcessing bool

	// BaseConfig is a map to store key-value configuration of a broker.
	// It will override other configs that have been set using other BrokerConfig options.
	// Only Kafka broker is supported.
	// 		List of supported Kafka configuration: https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md
	BaseConfig map[string]interface{}
}

// SecurityConfig contains security configuration for message broker
type SecurityConfig struct {
	AuthenticationType string
	SASLUsername       string
	SASLPassword       string
}

// PublishBuilder defines the structure of message which is sent through message broker
type PublishBuilder struct {
	topic            string
	eventName        string
	clientID         string
	traceID          string
	spanContext      string
	userID           string
	version          int
	eventID          int
	eventType        int
	eventLevel       int
	serviceName      string
	clientIDs        []string
	targetUserIDs    []string
	privacy          bool
	additionalFields map[string]interface{}
	key              string
	payload          map[string]interface{}
	errorCallback    func(event *Event, err error)
	ctx              context.Context
	timeout          time.Duration
}

// NewPublish create new PublishBuilder instance
func NewPublish() *PublishBuilder {
	return &PublishBuilder{
		version:       defaultVersion,
		ctx:           context.Background(),
		errorCallback: nil,
	}
}

// Topic set channel / topic name
func (p *PublishBuilder) Topic(topic string) *PublishBuilder {
	p.topic = topic
	return p
}

// EventName set name of published event
func (p *PublishBuilder) EventName(eventName string) *PublishBuilder {
	p.eventName = eventName
	return p
}

// ClientID set clientID of publisher event
func (p *PublishBuilder) ClientID(clientID string) *PublishBuilder {
	p.clientID = clientID
	return p
}

// TraceID set traceID of publisher event
func (p *PublishBuilder) TraceID(traceID string) *PublishBuilder {
	p.traceID = traceID
	return p
}

// SpanContext set jaeger spanContext of publisher event
func (p *PublishBuilder) SpanContext(spanID string) *PublishBuilder {
	p.spanContext = spanID
	return p
}

// UserID set userID of publisher event
func (p *PublishBuilder) UserID(userID string) *PublishBuilder {
	p.userID = userID
	return p
}

// Version set event schema version
func (p *PublishBuilder) Version(version int) *PublishBuilder {
	p.version = version
	return p
}

// EventID set eventID of publisher event
func (p *PublishBuilder) EventID(eventID int) *PublishBuilder {
	p.eventID = eventID
	return p
}

// EventType set eventType of publisher event
func (p *PublishBuilder) EventType(eventType int) *PublishBuilder {
	p.eventType = eventType
	return p
}

// EventLevel set eventLevel of publisher event
func (p *PublishBuilder) EventLevel(eventLevel int) *PublishBuilder {
	p.eventLevel = eventLevel
	return p
}

// ServiceName set serviceName of publisher event
func (p *PublishBuilder) ServiceName(serviceName string) *PublishBuilder {
	p.serviceName = serviceName
	return p
}

// ClientIDs set clientIDs of publisher event
func (p *PublishBuilder) ClientIDs(clientIDs []string) *PublishBuilder {
	p.clientIDs = clientIDs
	return p
}

// TargetUserIDs set targetUserIDs of publisher event
func (p *PublishBuilder) TargetUserIDs(targetUserIDs []string) *PublishBuilder {
	p.targetUserIDs = targetUserIDs
	return p
}

// Privacy set privacy of publisher event
func (p *PublishBuilder) Privacy(privacy bool) *PublishBuilder {
	p.privacy = privacy
	return p
}

// AdditionalFields set AdditionalFields of publisher event
func (p *PublishBuilder) AdditionalFields(additionalFields map[string]interface{}) *PublishBuilder {
	p.additionalFields = additionalFields
	return p
}

// Key is a message key that used to determine the partition of the topic
// if client require strong order for the events
func (p *PublishBuilder) Key(key string) *PublishBuilder {
	p.key = key
	return p
}

// Payload is a event payload that will be published
func (p *PublishBuilder) Payload(payload map[string]interface{}) *PublishBuilder {
	p.payload = payload
	return p
}

// ErrorCallback function to handle the event when failed to publish
func (p *PublishBuilder) ErrorCallback(errorCallback func(event *Event, err error)) *PublishBuilder {
	p.errorCallback = errorCallback
	return p
}

// Context define client context when publish event.
// default: context.Background()
func (p *PublishBuilder) Context(ctx context.Context) *PublishBuilder {
	p.ctx = ctx
	return p
}

// Timeout is an upper bound on the time to report success or failure after a call to send() returns.
// The value of this config should be greater than or equal to the sum of request.timeout.ms and linger.ms.
// Default value: 60000 ms
func (p *PublishBuilder) Timeout(timeout time.Duration) *PublishBuilder {
	p.timeout = timeout
	return p
}

// SubscribeBuilder defines the structure of message which is sent through message broker
type SubscribeBuilder struct {
	topic           string
	groupID         string
	groupInstanceID string
	offset          int64
	callback        func(ctx context.Context, event *Event, err error) error
	eventName       string
	ctx             context.Context
	callbackRaw     func(ctx context.Context, msgValue []byte, err error) error
	// flag to send error message to DLQ
	sendErrorDLQ bool
	// flag to use async commit consumer
	asyncCommitMessage bool
}

// NewSubscribe create new SubscribeBuilder instance
func NewSubscribe() *SubscribeBuilder {
	return &SubscribeBuilder{
		ctx:    context.Background(),
		offset: int64(kafka.OffsetEnd),
	}
}

// Topic sets the topic to subscribe to
func (s *SubscribeBuilder) Topic(topic string) *SubscribeBuilder {
	s.topic = topic
	return s
}

// Offset sets the offset of the event to start with
func (s *SubscribeBuilder) Offset(offset int64) *SubscribeBuilder {
	s.offset = offset
	return s
}

// GroupID set subscriber groupID. A random groupID will be generated by default.
func (s *SubscribeBuilder) GroupID(groupID string) *SubscribeBuilder {
	s.groupID = groupID
	return s
}

// GroupInstanceID set subscriber group instance ID
func (s *SubscribeBuilder) GroupInstanceID(groupInstanceID string) *SubscribeBuilder {
	s.groupInstanceID = groupInstanceID
	return s
}

// EventName set event name that will be subscribe
func (s *SubscribeBuilder) EventName(eventName string) *SubscribeBuilder {
	s.eventName = eventName
	return s
}

// Callback to do when the event received
func (s *SubscribeBuilder) Callback(
	callback func(ctx context.Context, event *Event, err error) error,
) *SubscribeBuilder {
	s.callback = callback
	return s
}

// CallbackRaw callback that receives the undecoded payload
func (s *SubscribeBuilder) CallbackRaw(
	f func(ctx context.Context, msgValue []byte, err error) error,
) *SubscribeBuilder {
	s.callbackRaw = f
	return s
}

// Context define client context when subscribe event.
// default: context.Background()
func (s *SubscribeBuilder) Context(ctx context.Context) *SubscribeBuilder {
	s.ctx = ctx
	return s
}

// SendErrorDLQ to send error message to DLQ topic.
// DLQ topic: 'topic' + -dlq
func (s *SubscribeBuilder) SendErrorDLQ(dlq bool) *SubscribeBuilder {
	s.sendErrorDLQ = dlq
	return s
}

// AsyncCommitMessage to asynchronously commit message offset.
// This setting will be overridden by AutoCommitInterval on BrokerConfig
func (s *SubscribeBuilder) AsyncCommitMessage(async bool) *SubscribeBuilder {
	s.asyncCommitMessage = async
	return s
}

// Slug is a string describing a unique subscriber (topic, eventName, groupID)
func (s *SubscribeBuilder) Slug() string {
	return fmt.Sprintf("%s%s%s%s%s", s.topic, slugSeparator, s.eventName, slugSeparator, s.groupID)
}

func NewClient(prefix, stream string, brokers []string, config ...*BrokerConfig) (Client, error) {
	switch stream {
	case eventStreamNull:
		return newBlackholeClient(), nil
	case eventStreamStdout:
		return newStdoutClient(prefix), nil
	case eventStreamKafka:
		return newKafkaClient(brokers, prefix, config...)
	default:
		return nil, errors.New("unsupported stream")
	}
}

// Client is an interface for event stream functionality
type Client interface {
	Publish(publishBuilder *PublishBuilder) error
	PublishSync(publishBuilder *PublishBuilder) error
	Register(subscribeBuilder *SubscribeBuilder) error
	PublishAuditLog(auditLogBuilder *AuditLogBuilder) error
	GetMetadata(topic string, timeout time.Duration) (*Metadata, error)
}

type AuditLog struct {
	ID         string `json:"_id" valid:"required"`
	ActionName string `json:"actionName" valid:"required"`
	Timestamp  int64  `json:"timestamp" valid:"required"`
	IP         string `json:"ip,omitempty" valid:"optional"`
	ActorID    string `json:"actorID" valid:"uuid4WithoutHyphens,required"`
	ActorType  string `json:"actorType" valid:"required~actorType values: USER CLIENT"`
	ObjectID   string `json:"objectId,omitempty" valid:"optional"`
	ObjectType string `json:"objectType,omitempty" valid:"optional"`

	ActionDetails AuditLogPayload `json:"payload" valid:"required"`
}

type PublishErrorCallbackFunc func(message []byte, err error)

type AuditLogBuilder struct {
	actionName string `description:"required"`
	ip         string `description:"optional"`
	actorID    string `description:"uuid4WithoutHyphens,required"`
	actorType  string `description:"required~actorType values: USER CLIENT"`
	objectID   string `description:"optional"`
	objectType string `description:"optional"`

	content map[string]interface{} `description:"optional"`

	key           string
	errorCallback PublishErrorCallbackFunc
	ctx           context.Context
	version       int
}

// NewAuditLogBuilder create new AuditLogBuilder instance
func NewAuditLogBuilder() *AuditLogBuilder {
	return &AuditLogBuilder{
		version:       defaultVersion,
		ctx:           context.Background(),
		errorCallback: nil,
	}
}

type AuditLogPayload struct {
	Content map[string]interface{} `json:"content"`
}

func (auditLogBuilder *AuditLogBuilder) ActionName(actionName string) *AuditLogBuilder {
	auditLogBuilder.actionName = actionName
	return auditLogBuilder
}

func (auditLogBuilder *AuditLogBuilder) IP(ip string) *AuditLogBuilder {
	auditLogBuilder.ip = ip
	return auditLogBuilder
}

func (auditLogBuilder *AuditLogBuilder) Actor(actor string) *AuditLogBuilder {
	auditLogBuilder.actorID = actor
	return auditLogBuilder
}

func (auditLogBuilder *AuditLogBuilder) IsActorTypeUser(isActorTypeUser bool) *AuditLogBuilder {
	if isActorTypeUser {
		auditLogBuilder.actorType = actorTypeUser
	} else {
		auditLogBuilder.actorType = actorTypeClient
	}

	return auditLogBuilder
}

func (auditLogBuilder *AuditLogBuilder) ObjectID(objectID string) *AuditLogBuilder {
	auditLogBuilder.objectID = objectID
	return auditLogBuilder
}

func (auditLogBuilder *AuditLogBuilder) ObjectType(objectType string) *AuditLogBuilder {
	auditLogBuilder.objectType = objectType
	return auditLogBuilder
}

func (auditLogBuilder *AuditLogBuilder) Content(content map[string]interface{}) *AuditLogBuilder {
	auditLogBuilder.content = content
	return auditLogBuilder
}

func (auditLogBuilder *AuditLogBuilder) ErrorCallback(errCallback PublishErrorCallbackFunc) *AuditLogBuilder {
	auditLogBuilder.errorCallback = errCallback
	return auditLogBuilder
}

func (auditLogBuilder *AuditLogBuilder) Key(key string) *AuditLogBuilder {
	auditLogBuilder.key = key
	return auditLogBuilder
}

func (auditLogBuilder *AuditLogBuilder) Build() (*kafka.Message, error) {
	auditLog := &AuditLog{
		ID:         generateID(),
		Timestamp:  time.Now().UnixMilli(),
		ActionName: auditLogBuilder.actionName,
		ActorID:    auditLogBuilder.actorID,
		ActorType:  auditLogBuilder.actorType,
		ObjectID:   auditLogBuilder.objectID,
		ObjectType: auditLogBuilder.objectType,
		IP:         auditLogBuilder.ip,
	}

	var content map[string]interface{}
	if auditLogBuilder.content == nil {
		content = make(map[string]interface{})
	} else {
		content = auditLogBuilder.content
	}

	auditLog.ActionDetails = AuditLogPayload{
		Content: content,
	}

	valid, err := validator.ValidateStruct(auditLog)
	if err != nil {
		logrus.WithField("action", auditLog.ActionName).
			Errorf("Unable to validate audit log. Error: %v", err)
		return &kafka.Message{}, err
	}
	if !valid {
		return &kafka.Message{}, errInvalidPubStruct
	}

	auditLogBytes, marshalErr := json.Marshal(auditLog)
	if marshalErr != nil {
		logrus.WithField("action", auditLog.ActionName).
			Errorf("Unable to marshal audit log: %v, error: %v", auditLog, marshalErr)
		return &kafka.Message{}, marshalErr
	}

	return &kafka.Message{
		Key:   []byte(auditLogBuilder.key),
		Value: auditLogBytes,
	}, nil
}
