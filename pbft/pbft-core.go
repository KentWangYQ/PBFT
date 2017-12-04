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
	f            int
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

	skipInProgress bool

	currentExec *uint64
	lastExec uint64
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
	execute(seqNo uint64,reqBatch *RequestBatch)
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

func (instance *pbftCore)intersectionQuorum() int{
	return (instance.N+instance.f+2)/2
}

func (instance *pbftCore) prePrepared(digest string,v uint64,n uint64)bool{
	_,mInLog:=instance.reqBatchStore[digest]

	if digest!=""&&!mInLog{
		return false
	}

	cert:=instance.certStore[msgID{v,n}]
	if cert!=nil{
		p:=cert.prePrepare
		if p!=nil&&p.View==v&&p.SequenceNumber==n&&p.BatchDigest==digest{
			return true
		}
	}
	logger.Debugf("Replica %d does not have view=%d/seqNo=%d pre-prepared",
		instance.id, v, n)
	return false
}

func (instance *pbftCore) prepared(digest string ,v uint64,n uint64)bool{
	if !instance.prePrepared(digest,v,n){
		return false
	}

	quorum:=0
	cert:=instance.certStore[msgID{v,n}]
	if cert==nil{
		return false
	}

	for _,p:=range cert.prepare{
		if p.View==instance.view&&p.SequenceNumber==n&&p.BatchDigest==digest{
			quorum++
		}
	}

	logger.Debugf("Replica %d prepare count for view=%d/seqNo=%d: %d",
		instance.id, v, n, quorum)

	return quorum>=instance.intersectionQuorum()-1
}

func(instance *pbftCore)committed(digest string,v uint64,n uint64)bool{
	if !instance.prepared(digest,v,n){
		return false
	}

	quorum:=0
	cert:=instance.certStore[msgID{v,n}]
	if cert==nil{
		return false
	}

	for _,p:=range cert.commit{
		if p.View==v&&p.SequenceNumber==n{
			quorum++
		}
	}

	logger.Debugf("Replica %d commit count for view=%d/seqNo=%d: %d",
		instance.id, v, n, quorum)
		
	return quorum>=instance.intersectionQuorum()
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

func (instance *pbftCore) maybeSendCommit(digest string,v uint64,n uint64)error{
	cert:=instance.getCert(v,n)
	if instance.prepared(digest,v,n)&&!cert.sendCommit{
		logger.Debugf("Replica %d broadcasting commit for view=%d/seqNo=%d",
			instance.id, v, n)
			commit:=&Commit{
				View:v,
				SequenceNumber:n,
				BatchDigest:digest,
				ReplicaId:instance.id,
			}
			cert.sendCommit=true
			instance.recvCommit(commit)
			return instance.innerBroadCast(&Message{&Message_Commit{Commit:commit}})
	}
	return nil
}

func (instance *pbftCore)recvCommit(commit *Commit)error{
	logger.Debugf("Replica %d received commit from replica %d for view=%d/seqNo=%d",
		instance.id, commit.ReplicaId, commit.View, commit.SequenceNumber)
	
	if !instance.inWV(commit.View,commit.SequenceNumber){
		if commit.SequenceNumber!=instance.h&&!instance.skipInProgress{
			logger.Warningf("Replica %d ignoring commit for view=%d/seqNo=%d: not in-wv, in view %d, high water mark %d", instance.id, commit.View, commit.SequenceNumber, instance.view, instance.h)
		} else {
			// This is perfectly normal
			logger.Debugf("Replica %d ignoring commit for view=%d/seqNo=%d: not in-wv, in view %d, high water mark %d", instance.id, commit.View, commit.SequenceNumber, instance.view, instance.h)
		}
		return nil
	}

	cert:=instance.getCert(commit.View,commit.SequenceNumber)
	for _,prevCommit:=range cert.commit{
		if prevCommit.ReplicaId==commit.ReplicaId{
			logger.Warningf("Ignoring duplicate commit from %d", commit.ReplicaId)
			return nil
		}
	}
	cert.commit=append(cert.commit,commit)

	if instance.committed(commit.BatchDigest,commit.View,commit.SequenceNumber){
		delete(instance.outstandingReqBatchs,commit.BatchDigest)

		instance.executeOutstanding()
	}

	return nil
}

func (instance *pbftCore)executeOutstanding(){
	if instance.currentExec!=nil{
		logger.Debugf("Replica %d not attempting to executeOutstanding because it is currently executing %d", instance.id, *instance.currentExec)
		return
	}
	logger.Debugf("Replica %d attempting to executeOutstanding", instance.id)
	
	for idx:=range instance.certStore{
		if instance.executeOne(idx){
			break
		}
	}
	
	logger.Debugf("Replica %d certstore %+v", instance.id, instance.certStore)
	
}

func (instance *pbftCore)executeOne(idx msgID)bool{
	cert:=instance.certStore[idx]

	if idx.n!=instance.lastExec||cert==nil||cert.prePrepare==nil{
		return false
	}

	if instance.skipInProgress{
		logger.Debugf("Replica %d currently picking a starting point to resume, will not execute", instance.id)
		return false
	}

	digest:=cert.digest
	reqBatch:=instance.reqBatchStore[digest]

	if !instance.committed(digest,idx.v,idx.n){
		return false
	}

	currentExec:=idx.n
	instance.currentExec=&currentExec

	if digest==""{

	}else{
		logger.Infof("Replica %d executing/committing request batch for view=%d/seqNo=%d and digest %s",
			instance.id, idx.v, idx.n, digest)
		instance.consumer.execute(idx.n,reqBatch)
	}
	return true
}
