package raft

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	googleproto "google.golang.org/protobuf/proto"
	"iron-bank/proto"
)

const (
	TypeAppendEntries      byte = 1
	TypeRequestVote        byte = 2
	TypeAppendEntriesReply byte = 3
	TypeRequestVoteReply   byte = 4
)

const MaxPayloadSize uint64 = 16 * 1024 * 1024

var (
	ErrUnknownMessageType = errors.New("unknown message type")
	ErrPayloadTooLarge    = errors.New("payload length exceeds maximum allowed size")
)

func EncodeMessage(msgType byte, msg googleproto.Message) ([]byte, error) {
	if msg == nil {
		return nil, errors.New("cannot encode nil protobuf message")
	}

	payload := proto.MustMarshal(msg)

	length := uint64(len(payload))
	if length > MaxPayloadSize {
		return nil, fmt.Errorf("%w: %d > %d", ErrPayloadTooLarge, length, MaxPayloadSize)
	}

	buf := make([]byte, 1 + 8 + len(payload))
	buf[0] = msgType
	binary.LittleEndian.PutUint64(buf[1:9], length)
	copy(buf[9:], payload)

	return buf, nil
}

func DecodeMessage(r io.Reader) (msgType byte, msg googleproto.Message, err error) {
	var header [9]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return 0, nil, err
	}

	msgType = header[0]
	length := binary.LittleEndian.Uint64(header[1:9])

	switch msgType {
	case TypeAppendEntries:
		msg = &proto.AppendEntriesArgs{}
	case TypeRequestVote:
		msg = &proto.RequestVoteArgs{}
	case TypeAppendEntriesReply:
		msg = &proto.AppendEntriesReply{}
	case TypeRequestVoteReply:
		msg = &proto.RequestVoteReply{}
	default:
		return msgType, nil, fmt.Errorf("%w: %d", ErrUnknownMessageType, msgType)
	}

	if length > MaxPayloadSize {
		return msgType, nil, fmt.Errorf("%w: %d > %d", ErrPayloadTooLarge, length, MaxPayloadSize)
	}

	payload := make([]byte, length)
	if _, err := io.ReadFull(r, payload); err != nil {
		return msgType, nil, err
	}

	defer func() {
		if rec := recover(); rec != nil {
			err = fmt.Errorf("proto unmarshal panic: %v", rec)
			msg = nil
		}
	}()

	proto.MustUnmarshal(payload, msg)

	return msgType, msg, nil
}

func NewLeaderState() *LeaderState {
	return &LeaderState{
		nextIndex:  make(map[string]uint64),
		matchIndex: make(map[string]uint64),
		inflight:   make(map[string]bool),
		pending:    make(map[uint64]chan result),
	}
}