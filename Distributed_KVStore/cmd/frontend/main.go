package main

import (
	"fmt"
	"github.com/DistributedClocks/tracing"
	"log"

	distkvs "example.org/cpsc416/a6"
)

func main() {
	var config distkvs.FrontEndConfig
	err := distkvs.ReadJSONConfig("config/frontend_config.json", &config)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(config)

	tracer := tracing.NewTracer(tracing.TracerConfig{
		ServerAddress:  config.TracerServerAddr,
		TracerIdentity: "frontend",
		Secret:         config.TracerSecret,
	})

	frontend := distkvs.FrontEnd{}
	err = frontend.Start(config.ClientAPIListenAddr, config.StorageAPIListenAddr, 10, tracer)

	if err != nil {
		log.Fatal(err)
	}
}
