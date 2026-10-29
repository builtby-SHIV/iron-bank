package raft

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	googleproto "google.golang.org/protobuf/proto"
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
	ListenLoop(addr string)
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

func (t *TCPTransport) ListenLoop(addr string, rpCh chan *RPCEvent) {
	var(
		listener net.Listener
		lc net.ListenConfig
		conn net.Conn
		err error
	)
	
	if listener, err = lc.Listen(t.ctx, "tcp", addr); err != nil {
		log.Fatalf("unable to start tcp connection on port %v, %v", addr, err)
	}
	go func(ln net.Listener) { 
		<-t.ctx.Done(); 
		ln.Close() 
	}(listener)

	backoff := 50 * time.Millisecond
	for {
		conn, err = listener.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) { 
				return 
			}
			select {
			case <-time.After(backoff):
				fmt.Printf("Error in listening to incoming message %v\n", err)
				if backoff < 500 * time.Millisecond {
					backoff *= 2
				}
				continue
			case <-t.ctx.Done():
				return
			}
		} 
		go handleIncoming(conn, t.ctx, rpCh)
	}

}

func handleIncoming(conn net.Conn, ctx context.Context, rpCh chan *RPCEvent) {
	var(
		msgType	byte
		msg		googleproto.Message
		err		error
	)

	if msgType, msg, err = DecodeMessage(conn); err != nil {
		fmt.Printf("Error in reading the incoming message %v\n", err)
			return
	}

	select {
		case rpCh<- &RPCEvent{ msgType: msgType, msg: msg }:
		case <-ctx.Done():
		return
	}
}

func (t *TCPTransport) sendToAppendEntries(
	id string, 
	msgType byte,
	args *proto.AppendEntriesArgs,
	replyCh chan *RPCEvent,
) {
	done := make(chan struct{})
	defer close(done)
	go func(ctx context.Context) {
		select {
		case <-t.ctx.Done():
		case <-done:
		}
		return
	}(t.ctx)

	reply, err := t.AppendEntries(id, args)
	if err != nil {
		fmt.Printf("Error in sending or receiving message %v\n", err)
	}

	select {
		case replyCh<- &RPCEvent{ msgType: msgType, msg: reply, peer: id }:
		case <-t.ctx.Done():
	}
	return
}

func (t *TCPTransport) sendToRequestVote(
	id string, 
	msgType byte,
	args *proto.RequestVoteArgs,
	replyCh chan *RPCEvent,
) {
	done := make(chan struct{})
	defer close(done)
	go func(ctx context.Context) {
		select {
		case <-t.ctx.Done():
		case <-done:
		}
		return
	}(t.ctx)

	reply, err := t.RequestVote(id, args)
	if err != nil {
		fmt.Printf("Error in sending or receiving message %v\n", err)
	}

	select {
		case replyCh<- &RPCEvent{ msgType: msgType, msg: reply, peer: id }:
		case <-t.ctx.Done():
	}
	return
}

func (t *TCPTransport) AppendEntries(
	id string, 
	args *proto.AppendEntriesArgs,
) (*proto.AppendEntriesReply, error) {
	t.mu.RLock()
	peer, exists := t.Peers[id]
	t.mu.RUnlock() 
	if !exists {
		return &proto.AppendEntriesReply{}, fmt.Errorf("peer %s not found", id)
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
	}(t.ctx)

	var(
		conn net.Conn
		err error
	)
	
	encodedMsg, err := EncodeMessage(TypeAppendEntries, args)
	if err != nil {
		return &proto.AppendEntriesReply{}, err
	}

	if peer.conn == nil {
		backoff := 50 * time.Millisecond
		for {
			dialer := &net.Dialer{}
			conn, err = dialer.DialContext(t.ctx, "tcp", peer.addr)
			if err == nil {
				break
			}

			select {
			case <-time.After(backoff):
				if backoff < 500 * time.Millisecond {
					backoff *= 2
				}
			case <-t.ctx.Done():
				return nil, nil
			}
		}
		peer.mu.Lock()
		peer.conn = conn
		peer.mu.Unlock()
	}

	w := bufio.NewWriter(peer.conn)
	if _, err := w.Write(encodedMsg); err != nil {
		return &proto.AppendEntriesReply{}, err
	}
	if err := w.Flush(); err != nil {
		return &proto.AppendEntriesReply{}, err
	}

	_, decodedMsg, err := DecodeMessage(peer.conn)
	if err != nil {
		return &proto.AppendEntriesReply{}, err
	}
	
	return decodedMsg.(*proto.AppendEntriesReply), nil
}

func (t *TCPTransport) RequestVote(
	id string, 
	args *proto.RequestVoteArgs,
) (*proto.RequestVoteReply, error) {
	t.mu.RLock()
	peer, exists := t.Peers[id]
	t.mu.RUnlock() 
	if !exists {
		return &proto.RequestVoteReply{}, fmt.Errorf("peer %s not found", id)
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
	}(t.ctx)

	var(
		conn net.Conn
		err error
	)
	
	encodedMsg, err := EncodeMessage(TypeAppendEntries, args)
	if err != nil {
		return &proto.RequestVoteReply{}, err
	}

	if peer.conn == nil {
		backoff := 50 * time.Millisecond
		for {
			dialer := &net.Dialer{}
			conn, err = dialer.DialContext(t.ctx, "tcp", peer.addr)
			if err == nil {
				break
			}

			select {
			case <-time.After(backoff):
				if backoff < 500 * time.Millisecond {
					backoff *= 2
				}
			case <-t.ctx.Done():
				return nil, nil
			}
		}
		peer.mu.Lock()
		peer.conn = conn
		peer.mu.Unlock()
	}

	w := bufio.NewWriter(peer.conn)
	if _, err := w.Write(encodedMsg); err != nil {
		return &proto.RequestVoteReply{}, err
	}
	if err := w.Flush(); err != nil {
		return &proto.RequestVoteReply{}, err
	}

	_, decodedMsg, err := DecodeMessage(peer.conn)
	if err != nil {
		return &proto.RequestVoteReply{}, err
	}
	
	return decodedMsg.(*proto.RequestVoteReply), nil
}