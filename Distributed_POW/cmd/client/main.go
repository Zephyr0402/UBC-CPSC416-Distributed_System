package main

import (
	"flag"
	"log"

	"example.org/cpsc416/a2/powlib"

	distpow "example.org/cpsc416/a2"
)

var nonce1 = []uint8{0x23, 0xff, 0x13, 0xae}
var nonce2 = []uint8{0xeb, 0x04, 0x59, 0x14}

func main() {
	var config distpow.ClientConfig
	err := distpow.ReadJSONConfig("config/client_config.json", &config)
	if err != nil {
		log.Fatal(err)
	}
	flag.StringVar(&config.ClientID, "id", config.ClientID, "Client ID, e.g. client1")
	flag.Parse()

	var config2 distpow.ClientConfig
	err = distpow.ReadJSONConfig("config/client2_config.json", &config2)
	if err != nil {
		log.Fatal(err)
	}
	flag.StringVar(&config2.ClientID, "id2", config2.ClientID, "Client ID, e.g. client1")
	flag.Parse()

	client := distpow.NewClient(config, powlib.NewPOW())
	if err := client.Initialize(); err != nil {
		log.Fatal(err)
	}
	client2 := distpow.NewClient(config2, powlib.NewPOW())
	if err := client2.Initialize(); err != nil {
		log.Fatal(err)
	}
	defer client.Close()
	defer client2.Close()

	if err := client.Mine(nonce1, 2); err != nil {
		log.Println(err)
	}
	if err := client.Mine(nonce2, 3); err != nil {
		log.Println(err)
	}
	//if err := client2.Mine([]uint8{2, 2, 2, 2}, 5); err != nil {
	//	log.Println(err)
	//}
	//if err := client2.Mine([]uint8{2, 2, 2, 2}, 7); err != nil {
	//	log.Println(err)
	//}

	for i := 0; i < 2; i++ {
		select {
		case mineResult := <-client.NotifyChannel:
			log.Println(mineResult)
		case mineResult := <-client2.NotifyChannel:
			log.Println(mineResult)
		}
	}
}
