package main

import (
	distkvs "example.org/cpsc416/a6"
	"example.org/cpsc416/a6/kvslib"
	"flag"
	"log"
	"time"
)

func main() {
	var config distkvs.ClientConfig
	err := distkvs.ReadJSONConfig("config/client_config.json", &config)
	if err != nil {
		log.Fatal(err)
	}
	flag.StringVar(&config.ClientID, "id", config.ClientID, "Client ID, e.g. client1")
	flag.Parse()

	var config2 distkvs.ClientConfig
	err = distkvs.ReadJSONConfig("config/client_config2.json", &config2)
	if err != nil {
		log.Fatal(err)
	}
	flag.StringVar(&config2.ClientID, "id2", config2.ClientID, "Client ID, e.g. client1")
	flag.Parse()

	client1 := distkvs.NewClient(config, kvslib.NewKVS())
	if err := client1.Initialize(); err != nil {
		log.Fatal(err)
	}
	client2 := distkvs.NewClient(config2, kvslib.NewKVS())
	if err := client2.Initialize(); err != nil {
		log.Fatal(err)
	}
	defer client1.Close()
	defer client2.Close()

	//if err, _ := client1.Put("client1", "key1", "value1"); err != 0 {
	//	log.Println(err)
	//}
	//if err, _ := client1.Put("client1", "key2", "value2"); err != 0 {
	//	log.Println(err)
	//}

	//time.Sleep(time.Second * 10)
	//
	if err, _ := client1.Put("client1", "key3", "value3"); err != 0 {
		log.Println(err)
	}

	if err, _ := client1.Get("client1", "key1"); err != 0 {
		log.Println(err)
	}

	if err, _ := client1.Put("client1", "key4", "value4"); err != 0 {
		log.Println(err)
	}

	if err, _ := client1.Get("client1", "key1"); err != 0 {
		log.Println(err)
	}

	if err, _ := client2.Put("client2", "key1", "value2"); err != 0 {
		log.Println(err)
	}

	if err, _ := client1.Get("client1", "key1"); err != 0 {
		log.Println(err)
	}

	time.Sleep(10 * time.Second)

	if err, _ := client2.Get("client2", "key1"); err != 0 {
		log.Println(err)
	}

	if err, _ := client1.Put("client1", "key2", "value2"); err != 0 {
		log.Println(err)
	}

	if err, _ := client2.Get("client2", "key2"); err != 0 {
		log.Println(err)
	}

	if err, _ := client2.Get("client2", "key3"); err != 0 {
		log.Println(err)
	}

	for i := 0; i < 10; i++ {
		select {
		case result := <-client1.NotifyChannel:
			log.Println(result)
			if result.Result != nil {
				log.Println(*result.Result)
			}
		case result := <-client2.NotifyChannel:
			log.Println(result)
			if result.Result != nil {
				log.Println(*result.Result)
			}
		}

	}

	//for i := 0; i < 100; i++ {
	//	client1.Put("client1", "key3", "value3_"+strconv.Itoa(i))
	//	client1.Put("client1", "key3", "!!!!!value3_"+strconv.Itoa(i))
	//	client2.Put("client2", "key3", "value2_"+strconv.Itoa(i))
	//	client1.Get("client1", "key3")
	//}
	//for i := 0; i < 400; i++ {
	//	select {
	//	case result := <-client1.NotifyChannel:
	//		log.Println(result)
	//		if result.Result != nil {
	//			log.Println(*result.Result)
	//		}
	//	case result := <-client2.NotifyChannel:
	//		log.Println(result)
	//		if result.Result != nil {
	//			log.Println(*result.Result)
	//		}
	//	}
	//
	//}

}
