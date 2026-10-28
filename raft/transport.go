package raft

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
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

	backoff := 50 * time.Millisecond
	for {
		conn, err = listener.Accept()
		if err == nil {
			break
		}
		select {
		case <-time.After(backoff):
			fmt.Printf("Error in listening to incoming message %v\n", err)
			if backoff < 500 * time.Millisecond {
				backoff *= 2
			}
		case <-t.ctx.Done():
			return
		}
	}

	go handleIncoming(conn, t.ctx, rpCh)
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
		peer.mu.Lock()
		peer.conn = conn
		peer.mu.Unlock()
	}

	w := bufio.NewWriter(peer.conn)
	if _, err := w.Write(encodedMsg); err != nil {
		return err
	}
	if err := w.Flush(); err != nil {
		return err
	}
	
	return nil
}

func (t *TCPTransport) RequestVote(
	ctx context.Context, 
	id string, 
	a *proto.RequestVoteArgs,
) (proto.RequestVoteReply, error)