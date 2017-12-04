package pbft

import (
	"encoding/base64"

	proto "github.com/golang/protobuf/proto"
	"github.com/kentwangyq/pbft/core/util"
)

func hash(msg interface{}) string {
	var raw []byte
	switch converted := msg.(type) {
	case *Request:
		raw, _ = proto.Marshal(converted)
	case *RequestBatch:
		raw, _ = proto.Marshal(converted)
	default:
		logger.Error("Asked to hash non-supported message type, ignoring")
		return ""
	}
	return base64.StdEncoding.EncodeToString(util.ComputeCryptoHash(raw))
}
