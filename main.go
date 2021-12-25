//go:generate msgp
package main

import (
	"bytes"
	"context"
	"encoding/gob"
	"encoding/hex"
	"encoding/json"
	"fmt"

	examplethrift "schema_practice/gen-go/example"
	"schema_practice/proto/schema_practice/example"

	"github.com/apache/thrift/lib/go/thrift"
	"google.golang.org/protobuf/proto"
)

type JsonPerson struct {
	Username       string   `json:"userName" msg:"userName"`
	FavoriteNumber int64    `json:"favouriteNumber" msg:"favouriteNumber"`
	Interests      []string `json:"interests" msg:"interests"`
}

type Person struct {
	Username       string
	FavoriteNumber int64
	Interests      []string
}

type PersonOld struct {
	Username       string
	FavoriteNumber int32
	Interests      []string
}

func main() {
	ctx := context.Background()
	data := &example.Person{
		UserName:        "Martin",
		FavouriteNumber: 1337,
		Interests:       []string{"daydreaming", "hacking"},
	}
	protoBytes, _ := proto.Marshal(data)
	jsonPerson := &JsonPerson{"Martin", 1337, []string{"daydreaming", "hacking"}}

	person := &Person{"Martin", 1337, []string{"daydreaming", "hacking"}}
	var b bytes.Buffer
	gob.NewEncoder(&b).Encode(person)
	gobBytes := b.Bytes()

	personOld := &PersonOld{}
	gob.NewDecoder(bytes.NewReader(gobBytes)).Decode(personOld)
	fmt.Println(personOld)
	jsonBytes, _ := json.Marshal(jsonPerson)
	msgpBytes, _ := jsonPerson.MarshalMsg(nil)
	thriftBinaryBytes := binary(ctx)
	thriftCompactBytes := compact(ctx)

	fmt.Println("data", data)
	fmt.Println("-------------- GOB ------------------")
	fmt.Println("Hex Format:", hex.EncodeToString(gobBytes))
	fmt.Println("String Format:", string(gobBytes))
	fmt.Println("Length:", len(gobBytes))

	fmt.Println("-------------- PROTOBUF ------------------")
	fmt.Println("Hex Format:", hex.EncodeToString(protoBytes))
	fmt.Println("String Format:", string(protoBytes))
	fmt.Println("Length:", len(protoBytes))

	fmt.Println("-------------- RAW JSON ------------------")
	fmt.Println("Hex Format:", hex.EncodeToString(jsonBytes))
	fmt.Println("String Format:", string(jsonBytes))
	fmt.Println("Length:", len(jsonBytes))

	fmt.Println("-------------- MSGPACK ------------------")
	fmt.Println("Hex Format:", hex.EncodeToString(msgpBytes))
	fmt.Println("String Format:", string(msgpBytes))
	fmt.Println("Length:", len(msgpBytes))

	fmt.Println("-------------- THRIFT BINARY ------------------")
	fmt.Println("Hex Format:", hex.EncodeToString(thriftBinaryBytes))
	fmt.Println("String Format:", string(thriftBinaryBytes))
	fmt.Println("Length:", len(thriftBinaryBytes))

	fmt.Println("-------------- THRIFT COMPACT ------------------")
	fmt.Println("Hex Format:", hex.EncodeToString(thriftCompactBytes))
	fmt.Println("String Format:", string(thriftCompactBytes))
	fmt.Println("Length:", len(thriftCompactBytes))
}

func binary(ctx context.Context) []byte {
	fmt.Printf("\n ==== demo Thrift Binary serialization ====\n")
	t := thrift.NewTMemoryBufferLen(1024)
	p := thrift.NewTBinaryProtocolFactoryDefault().GetProtocol(t)

	tser := &thrift.TSerializer{
		Transport: t,
		Protocol:  p,
	}
	num := int64(1337)
	a := &examplethrift.Person{
		UserName:        "Martin",
		FavouriteNumber: &num,
		Interests:       []string{"daydreaming", "hacking"},
	}

	by, _ := tser.Write(ctx, a)
	fmt.Printf("by = '%v', length %v\n", string(by), len(by))

	t2 := thrift.NewTMemoryBufferLen(1024)
	p2 := thrift.NewTBinaryProtocolFactoryDefault().GetProtocol(t2)

	deser := &thrift.TDeserializer{
		Transport: t2,
		Protocol:  p2,
	}

	b := examplethrift.NewPerson()
	deser.Transport.Close() // resets underlying bytes.Buffer
	deser.Read(ctx, b, by)
	fmt.Printf("b = '%#v'\n", b)
	return by
}

func compact(ctx context.Context) []byte {
	fmt.Printf("\n ==== demo Thrift Compact Binary serialization ====\n")
	t := thrift.NewTMemoryBufferLen(1024)
	p := thrift.NewTCompactProtocolFactory().GetProtocol(t)

	tser := &thrift.TSerializer{
		Transport: t,
		Protocol:  p,
	}

	num := int64(1337)

	a := &examplethrift.Person{
		UserName:        "Martin",
		FavouriteNumber: &num,
		Interests:       []string{"daydreaming", "hacking"},
	}

	by, _ := tser.Write(ctx, a)
	fmt.Printf("by = '%v', length %v\n", string(by), len(by))

	t2 := thrift.NewTMemoryBufferLen(1024)
	p2 := thrift.NewTCompactProtocolFactory().GetProtocol(t2)

	deser := &thrift.TDeserializer{
		Transport: t2,
		Protocol:  p2,
	}

	b := examplethrift.NewPerson()
	deser.Transport.Close() // resets underlying bytes.Buffer
	deser.Read(ctx, b, by)
	fmt.Printf("b = '%#v'\n", b)
	return by
}

// 1 byte: 5 bit field tag number + 3 bit field type
// string: 1 byte of defining number of length
// 0a064d617274696e10b90a1a0b646179647265616d696e671a076861636b696e67

// 0a => 00001 010 => field id 1, field type 2
// 06 => length = 6 => string 6 bytes
// 4d617274696e => Martin
