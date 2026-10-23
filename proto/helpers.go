package proto

import (
	"fmt"

	googleproto "google.golang.org/protobuf/proto"
)

func MustMarshal[T googleproto.Message](pb T) []byte {
	bytes, err := googleproto.Marshal(pb)
	if err != nil {
		panic(fmt.Sprintf("failed to marshal proto: %v", err))
	}
	return bytes
}

func MustUnmarshal[T googleproto.Message](bytes []byte, pb T) {
	err := googleproto.Unmarshal(bytes, pb)
	if err != nil {
		panic(fmt.Sprintf("failed to unmarshal proto: %v", err))
	}
}
