package pbft

import (
	fmt "fmt"
	"math/rand"
	"time"

	proto "github.com/golang/protobuf/proto"
	logging "github.com/op/go-logging"
)

var logger *logging.Logger // package-level logger

type pbftCore struct {
	N            int
	F            int
	replicaCount int
	view         uint64
	seqNo        uint64
	h            uint64
	L            uint64

	id        uint64
	byzantine bool

	reqBatchStore        map[string]*RequestBatch
	outstandingReqBatchs map[string]*RequestBatch
	certStore            map[msgID]*msgCert

	consumer innerStack
}

type msgID struct {
	v uint64
	n uint64
}

type msgCert struct {
	digest      string
	prePrepare  *PrePrepare
	sendPrepare bool
	prepare     []*Prepare
	sendCommit  bool
	commit      []*Commit
}

type innerStack interface {
	broadcast(msgPayload []byte)
	unicast(msgPayload []byte, reveiverID uint64) (err error)
}

func (instance *pbftCore) primary(n uint64) uint64 {
	return n % uint64(instance.replicaCount)
}

func (instance *pbftCore) inW(n uint64) bool {
	return n > instance.h && n <= instance.h+instance.L
}

func (instance *pbftCore) inWV(v uint64, n uint64) bool {
	return instance.view == v && instance.inW(n)
}

func (instance *pbftCore) getCert(v uint64, n uint64) (cert *msgCert) {
	idx := msgID{v: v, n: n}
	cert, ok := instance.certStore[idx]
	if ok {
		return
	}
	cert = &msgCert{}
	instance.certStore[idx] = cert
	return
}

func (instance *pbftCore) innerBroadCast(msg *Message) error {
	msgRaw, err := proto.Marshal(msg)
	if err != nil {
		return fmt.Errorf("Cannot marshal message %s", err)
	}

	doByzantine := false
	if instance.byzantine {
		rand1 := rand.New(rand.NewSource(time.Now().UnixNano()))
		doIt := rand1.Intn(3)
		if doIt == 1 {
			doByzantine = true
		}
	}

	if doByzantine {
		rand2 := rand.New(rand.NewSource(time.Now().UnixNano()))
		ignoreidx := rand2.Intn(instance.N)
		for i := 0; i < instance.N; i++ {
			if i != ignoreidx && uint64(i) != instance.id {
				instance.consumer.unicast(msgRaw, uint64(i))
			} else {
				logger.Debugf("PBFT byzantine: not broadcasting to replica %v", i)
			}
		}
	} else {
		instance.consumer.broadcast(msgRaw)
	}
	return nil
}

func (instance *pbftCore) recvMsg(msg *Message, senderID uint64) (interface{}, error) {
	if reqBatch := msg.GetRequestBatch(); reqBatch != nil {
		return reqBatch, nil
	} else if preprep := msg.GetPrePrepare(); preprep != nil {
		if senderID != preprep.ReplicaId {
			return nil, fmt.Errorf("Sender ID included in pre-prepare message (%v) doesn't match ID corresponding to the receiving stream (%v)", preprep.ReplicaId, senderID)
		}
		return preprep, nil
	} else if prep := msg.GetPrepare(); prep != nil {
		if senderID != prep.ReplicaId {
			return nil, fmt.Errorf("Sender ID included in prepare message (%v) doesn't match ID corresponding to the receiving stream (%v)", preprep.ReplicaId, senderID)
		}
		return prep, nil
	} else if commit := msg.GetCommit(); commit != nil {
		if senderID != commit.ReplicaId {
			return nil, fmt.Errorf("Sender ID included in commit message (%v) doesn't match ID corresponding to the receiving stream (%v)", preprep.ReplicaId, senderID)
		}
		return commit, nil
	}
	return nil, fmt.Errorf("Invalid message: %v", msg)
}

func (instance *pbftCore) recvRequestBatch(reqBatch *RequestBatch) error {
	digest := hash(reqBatch)
	logger.Debugf("Replica %d received request batch %s", instance.id, digest)

	instance.reqBatchStore[digest] = reqBatch
	instance.outstandingReqBatchs[digest] = reqBatch
	instance.persistRequestBatch(digest)

	if instance.primary(instance.view) == instance.id {
		instance.sendPrePrepare(reqBatch, digest)
	} else {
		logger.Debugf("Replica %d is backup, not sending pre-prepare for request batch %s", instance.id, digest)
	}
	return nil
}

func (instance *pbftCore) sendPrePrepare(reqBatch *RequestBatch, digest string) {
	logger.Debugf("Replica %d is primary, issuing pre-prepare for request batch %s", instance.id, digest)

	n := instance.seqNo + 1
	for _, cert := range instance.certStore {
		if p := cert.prePrepare; p != nil {
			if p.View == instance.view && p.SequenceNumber != n && p.BatchDigest != digest {
				logger.Infof("Other pre-prepare found with same digest but different seqNo: %d instead of %d", p.SequenceNumber, n)
				return
			}
		}
	}

	if !instance.inWV(instance.view, n) || n > instance.h+instance.L/2 {
		logger.Warningf("Primary %d not sending pre-prepare for batch %s - out of sequence numbers", instance.id, digest)
		return
	}

	logger.Debugf("Primary %d broadcasting pre-prepare for view=%d/seqNo=%d and digest %s", instance.id, instance.view, n, digest)
	instance.seqNo = n
	preprep := &PrePrepare{
		View:           instance.view,
		SequenceNumber: n,
		BatchDigest:    digest,
		RequestBatch:   reqBatch,
		ReplicaId:      instance.id,
	}
	cert := instance.getCert(instance.view, n)
	cert.prePrepare = preprep
	cert.digest = digest
	instance.persistQSet()
	instance.innerBroadCast(&Message{Payload: &Message_PrePrepare{PrePrepare: preprep}})

}

func (instance *pbftCore) recvPrePrepare(preprep *PrePrepare) error {
	return nil
}

func (instance *pbftCore) prepare() error {
	return nil
}

func commit() {}

func reply() {}

func main() {

}
