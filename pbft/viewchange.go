package pbft

import "github.com/kentwangyq/pbft/util/events"

// 处理新视图
func (instance *pbftCore) processNewView() events.Event {
	var newReqBatchMissing bool
	nv, ok := instance.newViewStore[instance.view]
	if !ok {
		// 副本没有找到新视图，忽略
		logger.Debugf("Replica %d ignoring processNewView as it could not find view %d in its newViewStore", instance.id, instance.view)
		return nil
	}

	if instance.activeView {
		// 副本正在进行视图变更，忽略该新视图
		logger.Infof("Replica %d ignoring new-view from %d, v:%d: we are active in view %d",
			instance.id, nv.ReplicaId, nv.View, instance.view)
		return nil
	}

	cp, ok, replicas := instance.selectInitialCheckpoint(nv.Vset)
	if !ok {
		logger.Warningf("Replica %d could not determine initial checkpoint: %+v",
			instance.id, instance.viewChangeStore)
		return instance.sendViewChange()
	}

	speculativeLastExec := instance.lastExec
	if instance.currentExec != nil {
		speculativeLastExec = *instance.currentExec
	}

}

func sendViewChange() events.Event {
	instance.stopTimer()
}

// 选择ViewChange消息集合中初始（最新）检查点
func (instance *pbftCore) selectInitialCheckpoint(vset []*ViewChange) (checkpoint ViewChange_C, ok bool, replicas []uint64) {
	checkpoints := make(map[ViewChange_C][]*ViewChange)
	// 从ViewChange消息中收集checkpoint
	for _, vc := range vset {
		for _, c := range vc.Cset {
			checkpoints[*c] = append(checkpoints[*c], vc)
			logger.Debugf("Replica %d appending checkpoint from replica %d with seqNo=%d, h=%d, and checkpoint digest %s", instance.id, vc.ReplicaId, vc.H, c.SequenceNumber, c.Id)
		}
	}

	if len(checkpoints) == 0 {
		logger.Debugf("Replica %d has no checkpoints to select from: %d %s",
			instance.id, len(instance.viewChangeStore), checkpoints)
		return
	}

	for idx, vcList := range checkpoints {
		if len(vcList) <= instance.f {
			// 检查点idx没有收集到大于instan.f个ViewChange消息，不满足弱证明
			logger.Debugf("Replica %d has no weak certificate for n:%d, vcList was %d long",
				instance.id, idx.SequenceNumber, len(vcList))
			continue
		}

		quorum := 0
		// 注意，在论文中，这里用的是整个vset(S)，而不仅仅是checkpoint set(S') (vcList)
		// 我们需要来自所有副本的集合S中的2f+1个水线下限小于该序号
		// 我们需要f+1个检查点符合该序号(S')
		for _, vc := range vset {
			if vc.H <= idx.SequenceNumber {
				quorum++
			}
		}

		if quorum < instance.intersectionQuorum() {
			logger.Debugf("Replica %d has no quorum for n:%d", instance.id, idx.SequenceNumber)
			continue
		}

		replicas = make([]uint64, len(vcList))
		// 收集发送检查点的副本
		for i, vc := range vcList {
			replicas[i] = vc.ReplicaId
		}

		// 如果当前检查点序号大于之前的检查点（range checkpoints），则记录当前检查点
		if checkpoint.SequenceNumber <= idx.SequenceNumber {
			checkpoint = idx
			ok = true
		}
	}
	return
}
