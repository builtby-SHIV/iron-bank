package cluster

import (
	"context"
	"time"

	"iron-bank/proto"
)

type Replicator struct {
	followerID        uint64
	peerLink          *PeerLink
	raft              RaftHooks
	heartbeatInterval time.Duration
	requestTimeout    time.Duration
	reportHigherTerm  func(term uint64)

	wakeUpChan chan struct{}
}

func LivenessInterval(followerID uint64) time.Duration {
	if followerID == 0 {
		return 2 * time.Second
	}
	return time.Duration(float64(followerID) * 2.0 * float64(time.Second))
}

func NewReplicator(
	followerID uint64,
	peerLink *PeerLink,
	raft RaftHooks,
	heartbeatInterval time.Duration,
	requestTimeout time.Duration,
	reportHigherTerm func(term uint64)) *Replicator {
	if heartbeatInterval <= 0 {
		heartbeatInterval = LivenessInterval(followerID)
	}
	if requestTimeout <= 0 {
		requestTimeout = heartbeatInterval
	}

	return &Replicator{
		followerID:        followerID,
		peerLink:          peerLink,
		raft:              raft,
		heartbeatInterval: heartbeatInterval,
		requestTimeout:    requestTimeout,
		reportHigherTerm:  reportHigherTerm,
		wakeUpChan:        make(chan struct{}, 1),
	}
}

func (r *Replicator) Trigger() {
	select {
	case r.wakeUpChan <- struct{}{}:
	default:
	}
}

func (r *Replicator) Run(ctx context.Context) {
	nextIndex := r.raft.LastLogIndex() + 1
	matchIndex := uint64(0)

	replyChan := make(chan *proto.AppendEntriesReply, 1)
	r.peerLink.RegisterAppendEntriesReply(replyChan)
	defer r.peerLink.UnregisterAppendEntriesReply()

	heartbeatTicker := time.NewTicker(r.heartbeatInterval)
	defer heartbeatTicker.Stop()

	var requestTimer *time.Timer
	var requestTimerC <-chan time.Time
	waitingForReply := false
	pendingWakeup := false

	stopTimer := func() {
		if requestTimer != nil {
			if !requestTimer.Stop() {
				select {
				case <-requestTimer.C:
				default:
				}
			}
			requestTimerC = nil
		}
	}
	defer stopTimer()

	sendAppendEntries := func() {
		prevLogIndex, prevLogTerm, entries, leaderCommit := r.raft.GetLogEntries(nextIndex)

		args := &proto.AppendEntriesArgs{
			Term:         r.raft.CurrentTerm(),
			LeaderID:     r.raft.NodeID(),
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			LeaderCommit: leaderCommit,
			Entries:      entries,
		}

		if err := r.peerLink.Send(TypeAppendEntries, args); err != nil {
			r.peerLink.Reconnect()
			return
		}

		waitingForReply = true

		heartbeatTicker.Reset(r.heartbeatInterval)

		if requestTimer == nil {
			requestTimer = time.NewTimer(r.requestTimeout)
		} else {
			if !requestTimer.Stop() {
				select {
				case <-requestTimer.C:
				default:
				}
			}
			requestTimer.Reset(r.requestTimeout)
		}
		requestTimerC = requestTimer.C
	}

	sendAppendEntries()

	for {
		select {
		case <-ctx.Done():
			return

		case <-r.wakeUpChan:
			if waitingForReply {
				pendingWakeup = true
			} else {
				sendAppendEntries()
			}

		case <-heartbeatTicker.C:
			if !waitingForReply {
				sendAppendEntries()
			}

		case <-requestTimerC:
			waitingForReply = false
			requestTimerC = nil
			r.peerLink.Reconnect()
			sendAppendEntries()

		case reply := <-replyChan:
			if !waitingForReply {
				continue
			}

			waitingForReply = false
			stopTimer()
			
			if reply.Term > r.raft.CurrentTerm() {
				if r.reportHigherTerm != nil {
					go r.reportHigherTerm(reply.Term)
				}
				return
			}

			if reply.Success {
				prevLogIndex, _, entries, _ := r.raft.GetLogEntries(nextIndex)
				if len(entries) > 0 {
					matchIndex = prevLogIndex + uint64(len(entries))
				} else if prevLogIndex > matchIndex {
					matchIndex = prevLogIndex
				}
				nextIndex = matchIndex + 1

				if r.raft.LastLogIndex() >= nextIndex {
					sendAppendEntries()
				} else if pendingWakeup {
					pendingWakeup = false
					sendAppendEntries()
				}
			} else {
				if reply.ConflictIndex > 0 && reply.ConflictIndex < nextIndex {
					nextIndex = reply.ConflictIndex
				} else if nextIndex > 1 {
					nextIndex--
				}
				sendAppendEntries()
			}
		}
	}
}
