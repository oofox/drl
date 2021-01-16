package main

import (
	"context"
	"log"
	"time"

	"github.com/oofox/drl/gen"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/durationpb"
)

func main() {
	conn, err := grpc.Dial("127.0.0.1:9090", grpc.WithInsecure())
	if err != nil {
		panic(err)
	}

	client, err := gen.NewDrlClient(conn).Connect(context.TODO())
	if err != nil {
		panic(err)
	}

	for {
		event, err := client.Recv()
		if err != nil {
			panic(err)
		}

		switch event.Type {
		case gen.EventType_Pull:
			client.Send(&gen.Event{
				Type:     gen.EventType_Quota,
				Key:      event.Key,
				Quota:    10,
				Duration: durationpb.New(time.Millisecond * 100),
			})
		case gen.EventType_Quota:
			log.Printf("%v", event)
		}
	}
}
