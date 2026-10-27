package raft

import (
	"context"

	"iron-bank/proto"
)

type Transport interface {
	RequestVote(ctx context.Context, peer string, a proto.RequestVoteArgs) (proto.RequestVoteReply, error)
    AppendEntries(ctx context.Context, peer string, a proto.AppendEntriesArgs) (proto.AppendEntriesReply, error)	
    // Consume() <-chan rpcEvent   
    Close() error
}

type TCPTransport struct {
	HTTPAddr	string
	TCPAddr		string
}

func NewTCPTransport(HTTPAddr, TCPAddr string) *TCPTransport {
	return &TCPTransport{
		HTTPAddr: HTTPAddr,
		TCPAddr: TCPAddr,
	}
}