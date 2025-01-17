package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"log"
)

// FLX protocol accepted message structure
type Message struct {
	Action  byte              // 2nd part of message 1 byte
	Length  uint16            // 2 bytes, expected first value in the protocl
	Payload string            // last part of the expected message, actual data to procces
	Headers map[string]string // headers 3rd part of expected message seperated by \r\n, ends with \r\n\r\n
}

func NewMessage(length uint16, action byte, headers map[string]string, payload string) *Message {
	return &Message{
		Length:  length,
		Action:  action,
		Headers: headers,
		Payload: payload,
	}
}

func NewAutoLengthMessage(action byte, headers map[string]string, payload string) *Message {
	payloadLenght := uint16(len([]byte(payload)))
	headersLenght := uint16(len(serializeHeaders(headers)))
	fullLenght := 1 + 2 + payloadLenght + headersLenght
	return &Message{
		Length:  fullLenght,
		Action:  action,
		Headers: headers,
		Payload: payload,
	}
}

func serializeHeaders(headers map[string]string) []byte {
	var headerBuff bytes.Buffer
	for key, val := range headers {
		// Write Header: key:value\r\n
		headerBuff.Write([]byte(key))
		headerBuff.Write([]byte(":"))
		headerBuff.Write([]byte(val))
		headerBuff.Write([]byte("\r\n"))
	}

	headerBuff.Write([]byte("\r\n\r\n"))
	return headerBuff.Bytes()
}

// serializes message back to binary
func (msg *Message) SerializeMessage() []byte {
	var buff bytes.Buffer

	lengthBytes := make([]byte, 2)
	binary.BigEndian.PutUint16(lengthBytes, uint16(msg.Length))
	buff.Write(lengthBytes)
	buff.Write([]byte{msg.Action})

	for key, val := range msg.Headers {
		var headerBuff bytes.Buffer
		// Write Header: key:value\r\n
		headerBuff.Write([]byte(key))
		headerBuff.Write([]byte(":"))
		headerBuff.Write([]byte(val))
		headerBuff.Write([]byte("\r\n"))

		buff.Write(headerBuff.Bytes())
	}
	buff.Write([]byte("\r\n\r\n"))
	buff.Write([]byte(msg.Payload))
	return buff.Bytes()
}

// parsing messsage to FLEX protocl
func ParseMessage(payload []byte) (*Message, error) {
	if len(payload) < 4 {
		return nil, errors.New("message too short")
	}
	//length to int
	// length := int(payload[0])<<8 | int(payload[1])
	lenU16 := binary.BigEndian.Uint16(payload[:2])
	length := uint16(lenU16)

	action := payload[2]
	headersAndPayload := payload[3:]

	headerEnd := bytes.Index(headersAndPayload, []byte("\r\n\r\n"))
	if headerEnd == -1 {
		log.Println("Malformed message: Missing header terminator")
		return nil, errors.New("malformed message missing header terminator")
	}

	headers := parseHeaders(headersAndPayload[:headerEnd])
	payloadData := string(headersAndPayload[headerEnd+4:]) // skiping the \r\n\r\n

	return NewMessage(length, action, headers, payloadData), nil
}

// parseHeaders makes a map of string string values
func parseHeaders(data []byte) map[string]string {
	headers := make(map[string]string)
	// spliting headers by "\r\n"
	lines := bytes.Split(data, []byte("\r\n"))
	for _, line := range lines {
		// spliting headers to key value pairs
		parts := bytes.SplitN(line, []byte(":"), 2)
		if len(parts) == 2 {
			headers[string(parts[0])] = string(parts[1])
		}
	}
	return headers
}
