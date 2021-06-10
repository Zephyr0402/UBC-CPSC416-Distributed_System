
.PHONY: pre storage storage2 storage3 frontend client tracing-server config-gen clean

all: pre storage storage2 storage3 frontend client tracing-server

pre:
	rm -f /tmp/disk1/DiskStorage.json
	rm -f /tmp/disk2/DiskStorage.json
	rm -f /tmp/disk3/DiskStorage.json

storage:
	go build -o storage cmd/storage/main.go

storage2:
	go build -o storage2 cmd/storage/main2.go

storage3:
	go build -o storage3 cmd/storage/main3.go

frontend:
	go build -o frontend cmd/frontend/main.go

client:
	go build -o client cmd/client/main.go

tracing-server:
	go build -o tracing-server cmd/tracing-server/main.go

clean:
	rm storage3 storage2 storage frontend client tracing-server *".log" *"-Log.txt" 2> /dev/null || true
