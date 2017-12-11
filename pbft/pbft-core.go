package pbft

import (
	"encoding/base64"
	fmt "fmt"
	"math/rand"
	"sort"
	"time"

	proto "github.com/golang/protobuf/proto"
	"github.com/kentwangyq/pbft/util/events"
	logging "github.com/op/go-logging"
	"github.com/spf13/viper"
)

var logger *logging.Logger // package-level logger

func init() {
	logger = logging.MustGetLogger("pbft/pbft")
}

type pbftCore struct {
	consumer innerStack // 消息消费者

	id            uint64            // 副本编号; PBFT `i`
	byzantine     bool              //指示该节点是否故意为拜占庭节点，用于测试网络Debug
	N             int               // 网络中节点最大数量
	f             int               // 容忍的故障节点最大数量
	replicaCount  int               // 副本数量; PBFT `|R|`
	view          uint64            // 当前视图
	seqNo         uint64            // 严格单调递增序号; PBFT "n"
	h             uint64            // 水线下限
	L             uint64            // 日志大小
	K             uint64            // 检查点周期
	logMultiplier uint64            // 使用该值来计算日志大小：k*logMultiplier
	chkpts        map[uint64]string // 状态检查点，将lastExec映射到全局哈希（本地生成）
	lastExec      uint64            // 我们执行过的最后一个请求

	skipInProgress    bool               // 当检测到落后是设置为true，直到重新赶上进度
	stateTransferring bool               // 执行状态转换时设置为true
	highStateTarget   *stateUpdateTarget // 设置我们接收到的最高的弱检查点凭证
	hChkpts           map[uint64]uint64  // 接收到的每个副本的最大的检查点序号，hChkpts[chkpt.ReplicaId] = chkpt.SequenceNumber

	currentExec          *uint64                  // 当前正在执行的请求
	outstandingReqBatchs map[string]*RequestBatch // 追踪我们是否正等待某些请求批次执行

	reqBatchStore   map[string]*RequestBatch // 追踪请求批次
	certStore       map[msgID]*msgCert       // 为请求追踪仲裁集凭证
	checkpointStore map[Checkpoint]bool      // 追踪检查点集合
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

type pbftMessage struct {
	sender uint64
	msg    *Message
}

type checkpointMessage struct {
	seqNo uint64
	id    []byte
}

type stateUpdateTarget struct {
	checkpointMessage
	replicas []uint64
}

type pbftMessageEvent pbftMessage

type innerStack interface {
	broadcast(msgPayload []byte)
	unicast(msgPayload []byte, reveiverID uint64) (err error)
	execute(seqNo uint64, reqBatch *RequestBatch)
	getState() []byte
	skipTo(seqNo uint64, snapshotID []byte, peers []uint64)

	invalidateState()
}

type sortableUint64Slice []uint64

func (a sortableUint64Slice) Len() int {
	return len(a)
}

func (a sortableUint64Slice) Swap(i, j int) {
	a[i], a[j] = a[j], a[i]
}

func (a sortableUint64Slice) Less(i, j int) bool {
	return a[i] < a[j]
}

func newPbftCore(id uint64, config *viper.Viper, consumer innerStack) *pbftCore {
	instance := &pbftCore{}
	instance.id = id
	instance.consumer = consumer

	instance.N = config.GetInt("general.N")
	instance.f = config.GetInt("general.f")
	if instance.f*3+1 > instance.N {
		panic(fmt.Sprintf("need at least %d enough replicas to tolerate %d byzantine faults, but only %d replicas configured", instance.f*3+1, instance.f, instance.N))
	}

	instance.K = uint64(config.GetInt("general.K"))

	instance.logMultiplier = uint64(config.GetInt("general.logmultiplier"))
	if instance.logMultiplier < 2 {
		panic("Log multiplier must be greater than or equal to 2")
	}

	instance.L = instance.logMultiplier * instance.K

	instance.byzantine = config.GetBool("general.byzantine")

	instance.replicaCount = instance.N

	logger.Infof("PBFT type = %T", instance.consumer)
	logger.Infof("PBFT Max number of validating peers (N) = %v", instance.N)
	logger.Infof("PBFT Max number of failing peers (f) = %v", instance.f)
	logger.Infof("PBFT byzantine flag = %v", instance.byzantine)
	logger.Infof("PBFT Checkpoint period (K) = %v", instance.K)
	logger.Infof("PBFT Log multiplier = %v", instance.logMultiplier)
	logger.Infof("PBFT log size (L) = %v", instance.L)

	instance.certStore = make(map[msgID]*msgCert)
	instance.reqBatchStore = make(map[string]*RequestBatch)
	instance.outstandingReqBatchs = make(map[string]*RequestBatch)

	return instance
}

// 计算网络主节点
func (instance *pbftCore) primary(n uint64) uint64 {
	return n % uint64(instance.replicaCount)
}

// 判断n是否在水线范围内
func (instance *pbftCore) inW(n uint64) bool {
	return n > instance.h && n <= instance.h+instance.L
}

// 判断v是否为当前视图，且n是否在水线范围内
func (instance *pbftCore) inWV(v uint64, n uint64) bool {
	return instance.view == v && instance.inW(n)
}

// 获取视图v，编号n的请求的凭证
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

// 广播消息
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

// 返回交叉仲裁集数量下限
func (instance *pbftCore) intersectionQuorum() int {
	// ⌈(instance.N + instance.f + 1)/2⌉，向上取整，该处采用分子+1来实现，在分母为2，分子整数增减的前提下有效
	return (instance.N + instance.f + 2) / 2
}

// 调整水线
func (instance *pbftCore) moveWatermarks(n uint64) {
	// 计算n所在的水线范围下限
	h := n / instance.K * instance.K

	// 清理凭证集
	for idx, cert := range instance.certStore {
		// 如果消息序号小于水线范围下限，清理这些记录
		if idx.n <= h {
			logger.Debugf("Replica %d cleaning quorum certificate for view=%d/seqNo=%d",
				instance.id, idx.v, idx.n)
			instance.persistDelAllRequestBatch(cert.digest)
			delete(instance.reqBatchStore, cert.digest)
			delete(instance.certStore, idx)
		}
	}

	// 清理检查点集
	for testChkpt := range instance.checkpointStore {
		if testChkpt.SequenceNumber <= h {
			logger.Debugf("Replica %d cleaning checkpoint message from replica %d, seqNo %d, b64 snapshot id %s",
				instance.id, testChkpt.ReplicaId, testChkpt.SequenceNumber, testChkpt.Id)
			delete(instance.checkpointStore, testChkpt)
		}
	}

	// 清理检查点映射
	for n := range instance.chkpts {
		if n < h {
			delete(instance.chkpts, n)
			instance.persistDelCheckpoint(n)
		}
	}

	instance.h = h

	logger.Debugf("Replica %d updated low watermark to %d",
		instance.id, instance.h)

	instance.resubmitRequestBatches()
}

// 验证请求是否处于pre-prepare阶段
func (instance *pbftCore) prePrepared(digest string, v uint64, n uint64) bool {
	_, mInLog := instance.reqBatchStore[digest]

	// 空信息或没有请求记录，false
	if digest != "" && !mInLog {
		return false
	}

	// 检查pre-prepare凭证
	cert := instance.certStore[msgID{v, n}]
	if cert != nil {
		p := cert.prePrepare
		if p != nil && p.View == v && p.SequenceNumber == n && p.BatchDigest == digest {
			return true
		}
	}
	logger.Debugf("Replica %d does not have view=%d/seqNo=%d pre-prepared",
		instance.id, v, n)
	return false
}

// 验证请求是否处于prepare阶段
func (instance *pbftCore) prepared(digest string, v uint64, n uint64) bool {
	if !instance.prePrepared(digest, v, n) {
		return false
	}

	quorum := 0
	cert := instance.certStore[msgID{v, n}]
	if cert == nil {
		return false
	}

	// 统计prepare凭证
	for _, p := range cert.prepare {
		if p.View == instance.view && p.SequenceNumber == n && p.BatchDigest == digest {
			quorum++
		}
	}

	logger.Debugf("Replica %d prepare count for view=%d/seqNo=%d: %d",
		instance.id, v, n, quorum)

	// 判断是否满足仲裁集数量，因为主节点不参与发送prepare消息，所以-1
	return quorum >= instance.intersectionQuorum()-1
}

// 验证请求是否处于commit阶段
func (instance *pbftCore) committed(digest string, v uint64, n uint64) bool {
	if !instance.prepared(digest, v, n) {
		return false
	}

	quorum := 0
	cert := instance.certStore[msgID{v, n}]
	if cert == nil {
		return false
	}

	for _, p := range cert.commit {
		if p.View == v && p.SequenceNumber == n {
			quorum++
		}
	}

	logger.Debugf("Replica %d commit count for view=%d/seqNo=%d: %d",
		instance.id, v, n, quorum)

	return quorum >= instance.intersectionQuorum()
}

// 信息路由
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
	} else if chkpt := msg.GetCheckpoint(); chkpt != nil {
		if senderID != chkpt.ReplicaId {
			return nil, fmt.Errorf("Sender ID included in checkpoint message (%v) doesn't match ID corresponding to the receiving stream (%v)", chkpt.ReplicaId, senderID)
		}
		return chkpt, nil
	}
	return nil, fmt.Errorf("Invalid message: %v", msg)
}

// 接收RequestBatch处理
func (instance *pbftCore) recvRequestBatch(reqBatch *RequestBatch) error {
	digest := hash(reqBatch)
	logger.Debugf("Replica %d received request batch %s", instance.id, digest)

	// 加入RequestBatch存储
	instance.reqBatchStore[digest] = reqBatch
	// 加入待执行RequestBatch集合
	instance.outstandingReqBatchs[digest] = reqBatch
	// 持久化RequestBatch
	instance.persistRequestBatch(digest)

	if instance.primary(instance.view) == instance.id {
		// 主节点发起共识，发送Pre-prepare消息
		instance.sendPrePrepare(reqBatch, digest)
	} else {
		logger.Debugf("Replica %d is backup, not sending pre-prepare for request batch %s", instance.id, digest)
	}
	return nil
}

// 发送Pre-prepare消息
func (instance *pbftCore) sendPrePrepare(reqBatch *RequestBatch, digest string) {
	logger.Debugf("Replica %d is primary, issuing pre-prepare for request batch %s", instance.id, digest)

	n := instance.seqNo + 1
	// 检查是否存在当前视图中具有相同摘要，但有不同序号的消息，空消息除外
	for _, cert := range instance.certStore {
		if p := cert.prePrepare; p != nil {
			if p.View == instance.view && p.SequenceNumber != n && p.BatchDigest == digest && digest != "" {
				logger.Infof("Other pre-prepare found with same digest but different seqNo: %d instead of %d", p.SequenceNumber, n)
				return
			}
		}
	}

	// 检查是否符合视图和水线要求，instance.L/2：表示当序号达到日志大小一般时，触发checkpoint，保证checkpoint安全生成，详见论文GC部分
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

	// Pre-prepare凭证存入instance.certStore
	cert := instance.getCert(instance.view, n)
	cert.prePrepare = preprep
	cert.digest = digest

	// 持久化QSet
	instance.persistQSet()

	// 广播Pre-prepare消息
	instance.innerBroadCast(&Message{Payload: &Message_PrePrepare{PrePrepare: preprep}})

}

// 主节点在某些特殊情况下重新提交为执行的RequestBatch
func (instance *pbftCore) resubmitRequestBatches() {
	if instance.primary(instance.view) != instance.id {
		return
	}

	var submissionOrder []*RequestBatch

outer:
	for d, reqBatch := range instance.outstandingReqBatchs {
		for _, cert := range instance.certStore {
			// 如果有该RequestBatch的凭证信息，则表示该条RequestBatch已经被提交过
			if cert.digest == d {
				logger.Debugf("Replica %d already has certificate for request batch %s - not going to resubmit", instance.id, d)
				continue outer
			}
		}
		logger.Debugf("Replica %d has detected request batch %s must be resubmitted", instance.id, d)
		submissionOrder = append(submissionOrder, reqBatch)
	}

	if len(submissionOrder) == 0 {
		return
	}

	for _, reqBatch := range submissionOrder {
		instance.recvRequestBatch(reqBatch)
	}
}

// 接收并处理Pre-prepare消息
func (instance *pbftCore) recvPrePrepare(preprep *PrePrepare) error {
	logger.Debugf("Replica %d received pre-prepare from replica %d for view=%d/seqNo=%d",
		instance.id, preprep.ReplicaId, preprep.View, preprep.SequenceNumber)

	// 是否由主节点发送
	if instance.primary(instance.view) != preprep.ReplicaId {
		logger.Warningf("Pre-prepare from other than primary: got %d, should be %d", preprep.ReplicaId, instance.primary(instance.view))
		return nil
	}

	// 是否符合视图和水线
	if !instance.inWV(preprep.View, preprep.SequenceNumber) {
		if preprep.SequenceNumber != instance.h && !instance.skipInProgress {
			logger.Warningf("Replica %d pre-prepare view different, or sequence number outside watermarks: preprep.View %d, expected.View %d, seqNo %d, low-mark %d", instance.id, preprep.View, instance.primary(instance.view), preprep.SequenceNumber, instance.h)
		} else {
			// This is perfectly normal
			logger.Debugf("Replica %d pre-prepare view different, or sequence number outside watermarks: preprep.View %d, expected.View %d, seqNo %d, low-mark %d", instance.id, preprep.View, instance.primary(instance.view), preprep.SequenceNumber, instance.h)
		}

		return nil
	}

	// 是否存在不一致凭证
	cert := instance.getCert(preprep.View, preprep.SequenceNumber)
	if cert.digest != "" && cert.digest != preprep.BatchDigest {
		logger.Warningf("Pre-prepare found for same view/seqNo but different digest: received %s, stored %s", preprep.BatchDigest, cert.digest)
		// instance.sendViewChange()
		return nil
	}

	cert.prePrepare = preprep
	cert.digest = preprep.BatchDigest

	if _, ok := instance.reqBatchStore[preprep.BatchDigest]; !ok && preprep.BatchDigest != "" {
		digest := hash(preprep.GetRequestBatch())
		// 判断Pre-prepare消息中的摘要是否正确
		if digest != preprep.BatchDigest {
			logger.Warningf("Pre-prepare and request digest do not match: request %s, digest %s", digest, preprep.BatchDigest)
			return nil
		}
		instance.reqBatchStore[digest] = preprep.GetRequestBatch()
		logger.Debugf("Replica %d storing request batch %s in outstanding request batch store", instance.id, digest)
		instance.outstandingReqBatchs[digest] = preprep.GetRequestBatch()
		instance.persistRequestBatch(digest)
	}

	// 备份节点确认处于Pre-prepare阶段后，发送prepare消息
	if instance.primary(instance.view) != instance.id && instance.prePrepared(preprep.BatchDigest, preprep.View, preprep.SequenceNumber) && !cert.sendPrepare {
		logger.Debugf("Backup %d broadcasting prepare for view=%d/seqNo=%d", instance.id, preprep.View, preprep.SequenceNumber)
		prep := &Prepare{
			View:           preprep.View,
			SequenceNumber: preprep.SequenceNumber,
			BatchDigest:    preprep.BatchDigest,
			ReplicaId:      preprep.ReplicaId,
		}
		cert.sendPrepare = true

		// 向自己发送，影响仲裁集的计算
		instance.recvPrepare(prep)

		// 向外广播
		return instance.innerBroadCast(&Message{Payload: &Message_Prepare{Prepare: prep}})
	}
	return nil
}

// 接收并处理prepare消息
func (instance *pbftCore) recvPrepare(prep *Prepare) error {
	logger.Debugf("Replica %d received prepare from replica %d for view=%d/seqNo=%d",
		instance.id, prep.ReplicaId, prep.View, prep.SequenceNumber)

	// 主节点不参与发送prepare消息
	if instance.primary(prep.View) == prep.ReplicaId {
		logger.Warningf("Replica %d received prepare from primary, ignoring", instance.id)
		return nil
	}

	if !instance.inWV(prep.View, prep.SequenceNumber) {
		if prep.SequenceNumber != instance.h && !instance.skipInProgress {
			logger.Warningf("Replica %d ignoring prepare for view=%d/seqNo=%d: not in-wv, in view %d, low water mark %d", instance.id, prep.View, prep.SequenceNumber, instance.view, instance.h)
		} else {
			// This is perfectly normal
			logger.Debugf("Replica %d ignoring prepare for view=%d/seqNo=%d: not in-wv, in view %d, low water mark %d", instance.id, prep.View, prep.SequenceNumber, instance.view, instance.h)
		}
		return nil
	}

	cert := instance.getCert(prep.View, prep.SequenceNumber)
	for _, prevPrep := range cert.prepare {
		if prevPrep.ReplicaId == prep.ReplicaId {
			logger.Warningf("Ignoring duplicate prepare from %d", prep.ReplicaId)
			return nil
		}
	}

	cert.prepare = append(cert.prepare, prep)

	// 验证是否满足prepare条件，如果满足，发送commit消息
	return instance.maybeSendCommit(prep.BatchDigest, prep.View, prep.SequenceNumber)
}

// 若满足prepare条件，则发送commit消息
func (instance *pbftCore) maybeSendCommit(digest string, v uint64, n uint64) error {
	cert := instance.getCert(v, n)
	if instance.prepared(digest, v, n) && !cert.sendCommit {
		logger.Debugf("Replica %d broadcasting commit for view=%d/seqNo=%d",
			instance.id, v, n)
		commit := &Commit{
			View:           v,
			SequenceNumber: n,
			BatchDigest:    digest,
			ReplicaId:      instance.id,
		}
		cert.sendCommit = true

		// 向自己发送
		instance.recvCommit(commit)

		// 对外广播
		return instance.innerBroadCast(&Message{&Message_Commit{Commit: commit}})
	}
	return nil
}

// 接收并处理commit消息
func (instance *pbftCore) recvCommit(commit *Commit) error {
	logger.Debugf("Replica %d received commit from replica %d for view=%d/seqNo=%d",
		instance.id, commit.ReplicaId, commit.View, commit.SequenceNumber)

	if !instance.inWV(commit.View, commit.SequenceNumber) {
		if commit.SequenceNumber != instance.h && !instance.skipInProgress {
			logger.Warningf("Replica %d ignoring commit for view=%d/seqNo=%d: not in-wv, in view %d, high water mark %d", instance.id, commit.View, commit.SequenceNumber, instance.view, instance.h)
		} else {
			// This is perfectly normal
			logger.Debugf("Replica %d ignoring commit for view=%d/seqNo=%d: not in-wv, in view %d, high water mark %d", instance.id, commit.View, commit.SequenceNumber, instance.view, instance.h)
		}
		return nil
	}

	cert := instance.getCert(commit.View, commit.SequenceNumber)
	for _, prevCommit := range cert.commit {
		if prevCommit.ReplicaId == commit.ReplicaId {
			logger.Warningf("Ignoring duplicate commit from %d", commit.ReplicaId)
			return nil
		}
	}
	cert.commit = append(cert.commit, commit)

	// 完成commit阶段，达成共识，将该RequestBatch从outstandingReqBatchs中移除
	if instance.committed(commit.BatchDigest, commit.View, commit.SequenceNumber) {
		delete(instance.outstandingReqBatchs, commit.BatchDigest)

		instance.executeOutstanding()
	}

	return nil
}

// 遍历执行已完成共识的RequestBatch
func (instance *pbftCore) executeOutstanding() {
	if instance.currentExec != nil {
		logger.Debugf("Replica %d not attempting to executeOutstanding because it is currently executing %d", instance.id, *instance.currentExec)
		return
	}
	logger.Debugf("Replica %d attempting to executeOutstanding", instance.id)

	for idx := range instance.certStore {
		if instance.executeOne(idx) {
			break
		}
	}

	logger.Debugf("Replica %d certstore %+v", instance.id, instance.certStore)

}

// 执行请求
func (instance *pbftCore) executeOne(idx msgID) bool {
	cert := instance.certStore[idx]

	// 消息序号严格单调递增，所以idx.n==instance.lastExec+1
	if idx.n != instance.lastExec+1 || cert == nil || cert.prePrepare == nil {
		return false
	}

	if instance.skipInProgress {
		logger.Debugf("Replica %d currently picking a starting point to resume, will not execute", instance.id)
		return false
	}

	digest := cert.digest
	reqBatch := instance.reqBatchStore[digest]

	if !instance.committed(digest, idx.v, idx.n) {
		return false
	}

	currentExec := idx.n
	instance.currentExec = &currentExec

	if digest == "" {
		// 处理空请求
		logger.Infof("Replica %d executing/committing null request for view=%d/seqNo=%d",
			instance.id, idx.v, idx.n)
		// 空请求不需要执行，直接标记完成
		instance.execDoneSync()
	} else {
		// consumer处理请求
		logger.Infof("Replica %d executing/committing request batch for view=%d/seqNo=%d and digest %s",
			instance.id, idx.v, idx.n, digest)
		instance.consumer.execute(idx.n, reqBatch)
	}
	return true
}

// 执行结束时执行该函数
func (instance *pbftCore) execDoneSync() {
	// 指示正在执行状态
	if instance.currentExec != nil {
		logger.Infof("Replica %d finished execution %d, trying next", instance.id, *instance.currentExec)
		// 标记执行完成
		instance.lastExec = *instance.currentExec

		// 若达到检查点周期，发起创建检查点
		if instance.lastExec%instance.K == 0 {
			instance.Checkpoint(instance.lastExec, instance.consumer.getState())
		}
	} else {
		// 某处有Bug，调用该函数时，currentExec不应该为nil
		logger.Warningf("Replica %d had execDoneSync called, flagging ourselves as out of date", instance.id)
		instance.skipInProgress = true
	}

	// 执行状态结束
	instance.currentExec = nil

	// 继续执行其他等待的RequestBatch
	instance.executeOutstanding()
}

// 创建检查点
func (instance *pbftCore) Checkpoint(seqNo uint64, id []byte) {
	if seqNo%instance.K != 0 {
		// 未达到创建检查点条件
		logger.Errorf("Attempted to checkpoint a sequence number (%d) which is not a multiple of the checkpoint interval (%d)", seqNo, instance.K)
		return
	}

	idAsString := base64.StdEncoding.EncodeToString(id)

	logger.Debugf("Replica %d preparing checkpoint for view=%d/seqNo=%d and b64 id of %s",
		instance.id, instance.view, seqNo, idAsString)

	chkpt := &Checkpoint{
		SequenceNumber: seqNo,
		ReplicaId:      instance.id,
		Id:             idAsString,
	}

	instance.chkpts[seqNo] = idAsString

	// 持久化检查点
	instance.persistCheckpoint(seqNo, id)
	// 向自己发送检查点消息
	instance.recvCheckpoint(chkpt)
	// 对外发送检查点消息
	instance.innerBroadCast(&Message{Payload: &Message_Checkpoint{Checkpoint: chkpt}})
}

// 接收并处理检查点消息
func (instance *pbftCore) recvCheckpoint(chkpt *Checkpoint) events.Event {
	logger.Debugf("Replica %d received checkpoint from replica %d, seqNo %d, digest %s",
		instance.id, chkpt.ReplicaId, chkpt.SequenceNumber, chkpt.Id)

	if instance.weakCheckpointSetOutOfRange(chkpt) {
		return nil
	}

	if !instance.inW(chkpt.SequenceNumber) {
		if chkpt.SequenceNumber != instance.h && !instance.skipInProgress {
			// It is perfectly normal that we receive checkpoints for the watermark we just raised, as we raise it after 2f+1, leaving f replies left
			logger.Warningf("Checkpoint sequence number outside watermarks: seqNo %d, low-mark %d", chkpt.SequenceNumber, instance.h)
		} else {
			logger.Debugf("Checkpoint sequence number outside watermarks: seqNo %d, low-mark %d", chkpt.SequenceNumber, instance.h)
		}
		return nil
	}

	// 存储检查点
	instance.checkpointStore[*chkpt] = true

	// 追踪已经收集到了多少匹配该序号的不同检查点值
	diffValues := make(map[string]struct{})
	diffValues[chkpt.Id] = struct{}{}

	matching := 0
	for testChkpt := range instance.checkpointStore {
		if testChkpt.SequenceNumber == chkpt.SequenceNumber {
			if testChkpt.Id == chkpt.Id {
				// 值匹配，计数器+1
				matching++
			} else {
				// 值不匹配
				if _, ok := diffValues[testChkpt.Id]; !ok {
					diffValues[testChkpt.Id] = struct{}{}
				}
			}
		}
	}
	logger.Debugf("Replica %d found %d matching checkpoints for seqNo %d, digest %s",
		instance.id, matching, chkpt.SequenceNumber, chkpt.Id)

	// 网络拜占庭故障： 就该序号收到大于instance.f+1个不同值，超出允许的拜占庭数量，无法就该序号达成共识，也就无法生成稳定检查点。
	if count := len(diffValues); count > instance.f+1 {
		logger.Panicf("Network unable to find stable certificate for seqNo %d (%d different values observed already)",
			chkpt.SequenceNumber, count)
	}

	if matching == instance.f+1 {
		// 收集到弱凭证
		// 如果我们已经为该序号生成了一个检查点，确保摘要一致
		if ownChkptID, ok := instance.chkpts[chkpt.SequenceNumber]; ok {
			if ownChkptID != chkpt.Id {
				logger.Panicf("Own checkpoint for seqNo %d (%s) different from weak checkpoint certificate (%s)",
					chkpt.SequenceNumber, ownChkptID, chkpt.Id)
			}
		}
		instance.witnessCheckpointWeakCert(chkpt)
	}

	if matching < instance.intersectionQuorum() {
		// 不满足仲裁集，继续接收
		return nil
	}

	// 接收到足够凭证，满足仲裁集要求
	if _, ok := instance.chkpts[chkpt.SequenceNumber]; !ok {
		// 本地尚未达到检查点状态（落后）
		logger.Debugf("Replica %d found checkpoint quorum for seqNo %d, digest %s, but it has not reached this checkpoint itself yet",
			instance.id, chkpt.SequenceNumber, chkpt.Id)
		if instance.skipInProgress {
			logSafetyBound := instance.h + instance.L/2

			if chkpt.SequenceNumber >= logSafetyBound {
				// 本副本处于状态转换过程中，但是网络已经切换到了下一个检查点周期，移动水线到新的周期
				logger.Debugf("Replica %d is in state transfer, but, the network seems to be moving on past %d, moving our watermarks to stay with it", instance.id, logSafetyBound)
				instance.moveWatermarks(chkpt.SequenceNumber)
			}
		}
		return nil
	}

	logger.Debugf("Replica %d found checkpoint quorum for seqNo %d, digest %s",
		instance.id, chkpt.SequenceNumber, chkpt.Id)

	// 完成检查点创建，移动水线
	instance.moveWatermarks(chkpt.SequenceNumber)

	return instance.processNewView()
}

// 弱检查点集合超出范围
func (instance *pbftCore) weakCheckpointSetOutOfRange(chkpt *Checkpoint) bool {
	// 水线上线
	H := instance.h + instance.L

	// 追踪最后观察到的大于水线上限的检查点序号，以副本为键值，避免无限增长
	if chkpt.SequenceNumber < H {
		// 水线范围内，删除记录；非拜占庭节点，检查点序号单调上升
		delete(instance.hChkpts, chkpt.ReplicaId)
	} else {
		// 我们没有追踪到最大的序号，拜占庭节点会随意的选取一个很大的序号，甚至当它恢复为非拜占庭节点时，我们仍然相信它是遥遥领先的
		instance.hChkpts[chkpt.ReplicaId] = chkpt.SequenceNumber

		// 如果有f+1个副本报告的检查点序号超出我们的水线范围，我们就需要检查我们是否落后了
		if len(instance.hChkpts) >= instance.f+1 {
			chkptSeqNumArray := make([]uint64, len(instance.hChkpts))
			index := 0
			// 统计hChkpts中的序号
			for replicaID, hchkpt := range instance.hChkpts {
				chkptSeqNumArray[index] = hchkpt
				index++
				if hchkpt < H {
					// 如果符合水线范围，删除记录
					delete(instance.hChkpts, replicaID)
				}
			}

			// 序号正序排序
			sort.Sort(sortableUint64Slice(chkptSeqNumArray))

			// 序号数组中，如果倒数第f+1个序号大于H，则意味着至少有f+1个节点发送了大于H的检查点，说明我们落后了
			// 如果f+1节点处理了大于水线的检查点，则我们永远无法接受到2f+1对该序号的检查点确认，说明我们落后了
			if m := chkptSeqNumArray[len(chkptSeqNumArray)-(instance.f+1)]; m > H {
				logger.Warningf("Replica %d is out of date, f+1 nodes agree checkpoint with seqNo %d exists but our high water mark is %d", instance.id, chkpt.SequenceNumber, H)
				// 重置状态
				instance.reqBatchStore = make(map[string]*RequestBatch)
				instance.persistDelAllRequestBatches()
				instance.moveWatermarks(m)
				instance.outstandingReqBatchs = make(map[string]*RequestBatch)
				instance.skipInProgress = true
				instance.consumer.invalidateState()

				// TODO, 重新处理已收集的检查点，这将加快恢复速度，尽管目前是正确的
				return true
			}
		}
	}
	return true
}

// 见证检查点弱证书
func (instance *pbftCore) witnessCheckpointWeakCert(chkpt *Checkpoint) {
	// 只有第一个弱凭证调用（收集到第f+1个凭证时），所以设置为f+1
	checkpointMembers := make([]uint64, instance.f+1)
	i := 0
	// 收集检查点副本ID
	for testChkpt := range instance.checkpointStore {
		if testChkpt.SequenceNumber == chkpt.SequenceNumber && testChkpt.Id == chkpt.Id {
			checkpointMembers[i] = testChkpt.ReplicaId
			logger.Debugf("Replica %d adding replica %d (handle %v) to weak cert", instance.id, testChkpt.ReplicaId, checkpointMembers[i])
			i++
		}
	}

	snapshotID, err := base64.StdEncoding.DecodeString(chkpt.Id)
	if nil != err {
		err = fmt.Errorf("Replica %d received a weak checkpoint cert which could not be decoded (%s)", instance.id, chkpt.Id)
		logger.Error(err.Error())
		return
	}

	target := &stateUpdateTarget{
		checkpointMessage: checkpointMessage{
			seqNo: chkpt.SequenceNumber,
			id:    snapshotID,
		},
		replicas: checkpointMembers,
	}
	instance.updateHighStateTarget(target)

	if instance.skipInProgress {
		logger.Debugf("Replica %d is catching up and witnessed a weak certificate for checkpoint %d, weak cert attested to by %d of %d (%v)",
			instance.id, chkpt.SequenceNumber, i, instance.replicaCount, checkpointMembers)

		instance.retryStateTransfer(target)
	}
}

// 更新highStateTarget
func (instance *pbftCore) updateHighStateTarget(target *stateUpdateTarget) {
	if instance.highStateTarget != nil && instance.highStateTarget.seqNo >= target.seqNo {
		logger.Debugf("Replica %d not updating state target to seqNo %d, has target for seqNo %d", instance.id, target.seqNo, instance.highStateTarget.seqNo)
		return
	}

	instance.highStateTarget = target
}

func (instance *pbftCore) ProcessEvent(e events.Event) events.Event {
	var err error
	logger.Debugf("Replica %d processing event", instance.id)
	switch et := e.(type) {
	case *pbftMessage:
		return pbftMessageEvent(*et)
	case pbftMessageEvent:
		msg := et
		logger.Debugf("Replica %d received incoming message from %v", instance.id, msg.sender)
		next, err := instance.recvMsg(et.msg, et.sender)
		if err != nil {
			break
		}
		return next
	case *RequestBatch:
		err = instance.recvRequestBatch(et)
	case *PrePrepare:
		err = instance.recvPrePrepare(et)
	case *Prepare:
		err = instance.recvPrepare(et)
	case *Commit:
		err = instance.recvCommit(et)
	default:
		logger.Warningf("Replica %d received an unknown message type %T", instance.id, et)
	}

	if err != nil {
		logger.Warning(err.Error())
	}
	return nil
}

func (instance *pbftCore) retryStateTransfer(optional *stateUpdateTarget) {
	if instance.currentExec != nil {
		logger.Debugf("Replica %d is currently mid-execution, it must wait for the execution to complete before performing state transfer", instance.id)
		return
	}

	if instance.stateTransferring {
		logger.Debugf("Replica %d is currently mid state transfer, it must wait for this state transfer to complete before initiating a new one", instance.id)
		return
	}

	target := optional
	if target == nil {
		if instance.highStateTarget == nil {
			logger.Debugf("Replica %d has no targets to attempt state transfer to, delaying", instance.id)
			return
		}
		target = instance.highStateTarget
	}

	instance.stateTransferring = true

	logger.Debugf("Replica %d is initiating state transfer to seqNo %d", instance.id, target.seqNo)
	instance.consumer.skipTo(target.seqNo, target.id, target.replicas)
}
