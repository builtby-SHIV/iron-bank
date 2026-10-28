package raft

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	"iron-bank/proto"
)

type Transport interface {
	RequestVote(
		ctx context.Context, 
		id string, 
		a *proto.RequestVoteArgs,
	) (proto.RequestVoteReply, error)
	AppendEntries(
		ctx context.Context, 
		id string, 
		args *proto.AppendEntriesArgs,
	) (proto.AppendEntriesReply, error)
	BroadcastAppendEntries(args *proto.AppendEntriesArgs)
	// Consume() <-chan rpcEvent
	Close() error
}

type TCPTransport struct {
	HTTPAddr	string
	TCPAddr		string
	Peers		map[string]*peerConn
	ctx			context.Context
	cancel		context.CancelFunc
	mu			sync.RWMutex
}

type peerConn struct {
	addr	string
	conn	net.Conn
	mu		sync.RWMutex
}

func NewTCPTransport(HTTPAddr, TCPAddr string, ctx context.Context) *TCPTransport {
	newCtx, cancel := context.WithCancel(ctx)
	return &TCPTransport{
		HTTPAddr: HTTPAddr,
		TCPAddr:  TCPAddr,
		ctx: newCtx,
		cancel: cancel,
	}
}

func (t *TCPTransport) Add(id, addr string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.Peers[id] = &peerConn{ addr: addr }
}

func (t *TCPTransport) BroadcastAppendEntries(args *proto.AppendEntriesArgs) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	for id, _ := range t.Peers {
		go func(id string) {
			newCtx, cancel := context.WithCancel(t.ctx)
			defer cancel()
			if err := t.AppendEntries(newCtx, id, args); err != nil {
				fmt.Println(err.Error())
			}
		}(id)
	}
}

func (t *TCPTransport) AppendEntries(
	ctx context.Context, 
	id string, 
	args *proto.AppendEntriesArgs,
) error {
	t.mu.RLock()
	peer, exists := t.Peers[id]
	t.mu.RUnlock() 
	if !exists {
		return fmt.Errorf("peer %s not found", id)
	}

	done := make(chan struct{})
	defer close(done)
	go func(ctx context.Context) {
		select {
		case <-t.ctx.Done():
			if peer.conn != nil {
				peer.conn.Close()
				peer.conn = nil
			}
		case <-done:
		}
		return
	}(ctx)

	var(
		conn net.Conn
		err error
	)
	
	encodedMsg, err := EncodeMessage(TypeAppendEntries, args)
	if err != nil {
		return err
	}

	if peer.conn == nil {
		backoff := 50 * time.Millisecond
		for {
			dialer := &net.Dialer{}
			conn, err = dialer.DialContext(ctx, "tcp", peer.addr)
			if err == nil {
				break
			}

			select {
			case <-time.After(backoff):
				if backoff < 500 * time.Millisecond {
					backoff *= 2
				}
			case <-ctx.Done():
				return nil
			}
		}
	}

	peer.mu.Lock()
	peer.conn = conn
	peer.mu.Unlock()
	
	w := bufio.NewWriter(peer.conn)
	if _, err := w.Write(encodedMsg); err != nil {
		return err
	}
	if err := w.Flush(); err != nil {
		return err
	}
	
	return nil
}

func (t *TCPTransport) RequestVote(ctx context.Context, id string, a *proto.RequestVoteArgs) (proto.RequestVoteReply, error)