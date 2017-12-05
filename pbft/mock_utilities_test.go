package pbft

import (
	"fmt"
	"math/rand"
	"time"

	proto "github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/timestamp"
	pb "github.com/kentwangyq/pbft/protos"
)

type omniProto struct {
	// Inner Stack
	broadcastImpl func(payload []byte)
	unicastImpl   func(payload []byte, receiver uint64) (err error)
	executeImpl   func(seqNo uint64, reqBatch *RequestBatch)
}

func (op *omniProto) broadcast(payload []byte) {
	if nil != op.broadcastImpl {
		op.broadcastImpl(payload)
		return
	}
	panic("Unimplemented")
}

func (op *omniProto) unicast(payload []byte, receiver uint64) error {
	if nil != op.unicastImpl {
		return op.unicastImpl(payload, receiver)
	}
	panic("Unimplemented")
}

func (op *omniProto) execute(seqNo uint64, reqBatch *RequestBatch) {
	if nil != op.executeImpl {
		op.executeImpl(seqNo, reqBatch)
		return
	}
	panic("Unimplemented")
}

func createPbftReqBatch(tag int64, replica uint64) (reqBatch *RequestBatch) {
	req := createPbftReq(tag, replica)
	reqBatch = &RequestBatch{Batch: []*Request{req}}
	return
}

func createPbftReq(tag int64, replica uint64) (req *Request) {
	tx := createTx(tag)
	txPacked := marshalTx(tx)
	req = &Request{
		Timestamp: tx.GetTimestamp(),
		ReplicaId: replica,
		Payload:   txPacked,
	}
	return
}

func createTx(tag int64) (tx *pb.Transaction) {
	txTime := &timestamp.Timestamp{Seconds: tag, Nanos: 0}
	tx = &pb.Transaction{Type: pb.Transaction_CHAINCODE_DEPLOY,
		Timestamp: txTime,
		Payload:   []byte(fmt.Sprint(tag)),
	}
	return
}

func marshalTx(tx *pb.Transaction) (txPacked []byte) {
	txPacked, _ = proto.Marshal(tx)
	return
}

func generateBroadcaster(validatorCount int) (requestBroadcaster int) {
	seed := rand.NewSource(time.Now().UnixNano())
	rndm := rand.New(seed)
	requestBroadcaster = rndm.Intn(validatorCount)
	return
}
