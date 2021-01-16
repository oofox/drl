package main

import (
	"fmt"
	"log"
	"net"

	"github.com/oofox/drl/gen"
	"google.golang.org/grpc"
)

func main() {
	server := grpc.NewServer()

	coordinator := NewCoordinator()
	gen.RegisterDrlServer(server, coordinator)

	meta := NewMetaBase("http://10.220.79.69:10080")

	for _, database := range meta.Databases() {
		for _, tableID := range meta.Tables(database) {
			coordinator.Set(fmt.Sprintf("t_%d", tableID), 100)
		}
	}

	listener, err := net.Listen("tcp", ":9000")
	if err != nil {
		panic(err)
	}
	log.Fatal(server.Serve(listener))
}
