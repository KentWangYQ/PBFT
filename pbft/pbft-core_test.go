package pbft

import (
	"strings"
	"testing"

	"github.com/kentwangyq/pbft/util/events"
	logging "github.com/op/go-logging"
)

func init() {
	logging.SetLevel(logging.DEBUG, "")
}

func TestConfigSet(t *testing.T) {
	config := loadConfig()

	testKeys := []string{
		"general.N",
		"general.f",
		"general.K",
		"general.logmultiplier",
		"general.byzantine",
	}

	for _, key := range testKeys {
		if ok := config.IsSet(key); !ok {
			t.Errorf("Cannot test env override because \"%s\" does not seem to be set", key)
		}
	}
}

func TestWrongReplicaID(t *testing.T) {
	mock := &omniProto{}
	instance := newPbftCore(0, loadConfig(), mock)

	reqBatch := createPbftReqBatch(1, 1)
	pbftMsg := &Message{Payload: &Message_PrePrepare{PrePrepare: &PrePrepare{
		View:           0,
		SequenceNumber: 1,
		BatchDigest:    hash(reqBatch),
		RequestBatch:   reqBatch,
		ReplicaId:      1,
	}}}
	next, err := instance.recvMsg(pbftMsg, 2)

	if next != nil || err == nil {
		t.Fatalf("Shouldn't have processed message with incorrect replica ID")
	}
	if err != nil {
		rightError := strings.HasPrefix(err.Error(), "Sender ID")
		if !rightError {
			t.Fatalf("Should have returned error about incorrect replica ID on the incoming message")
		}
	}
}

func TestMaliciousPrePrepare(t *testing.T) {
	mock := &omniProto{
		broadcastImpl: func(msgPayload []byte) {
			t.Fatalf("Expected to ignore malicious pre-prepare")
		},
	}
	instance := newPbftCore(1, loadConfig(), mock)

	instance.replicaCount = 5

	pbftMsg := &PrePrepare{
		View:           0,
		SequenceNumber: 1,
		BatchDigest:    hash(createPbftReqBatch(1, 1)),
		RequestBatch:   createPbftReqBatch(1, 2),
		ReplicaId:      0,
	}
	events.SendEvent(instance, pbftMsg)
}

func TestIncompletePayload(t *testing.T) {
	mock := &omniProto{}
	instance := newPbftCore(1, loadConfig(), mock)

	instance.replicaCount = 5

	broadcaster := uint64(generateBroadcaster(instance.replicaCount))

	checkMsg := func(msg *Message, errMsg string, args ...interface{}) {
		mock.broadcastImpl = func(msgPayload []byte) {
			t.Errorf(errMsg, args...)
		}
		events.SendEvent(instance, pbftMessageEvent{msg: msg, sender: broadcaster})
	}

	checkMsg(&Message{}, "Expected to reject empty message")
	checkMsg(&Message{Payload: &Message_PrePrepare{PrePrepare: &PrePrepare{ReplicaId: broadcaster}}}, "Expected to reject empty pre-prepare")
}
