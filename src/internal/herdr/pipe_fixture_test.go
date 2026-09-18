package herdr

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"
)

func readTestFrame(conn net.Conn, value any) error {
	// Decode alone can leave the newline unread and deadlock both net.Pipe writers.
	frame, err := bufio.NewReader(conn).ReadBytes('\n')
	if err != nil {
		return err
	}
	return json.Unmarshal(frame, value)
}

func TestReadTestFrameConsumesDelimiter(t *testing.T) {
	for _, size := range []int{128, 129, 256, 257, 512, 513, 1024, 1025, 4096, 4097} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			const prefix = `{"id":"herdr-1","method":"ping","params":{"padding":"`
			const suffix = `"}}`
			frame := prefix + strings.Repeat("x", size-len(prefix)-len(suffix)) + suffix + "\n"
			client, peer := net.Pipe()
			defer client.Close()
			defer peer.Close()
			deadline := time.Now().Add(5 * time.Second)
			if err := client.SetWriteDeadline(deadline); err != nil {
				t.Fatal(err)
			}
			if err := peer.SetReadDeadline(deadline); err != nil {
				t.Fatal(err)
			}
			written := make(chan error, 1)
			go func() {
				_, err := client.Write([]byte(frame))
				written <- err
			}()
			var request testRequest
			if err := readTestFrame(peer, &request); err != nil {
				t.Fatal(err)
			}
			if err := <-written; err != nil {
				t.Fatalf("request delimiter was left unread: %v", err)
			}
			if request.ID != "herdr-1" || request.Method != "ping" || len(request.Params) != 1 {
				t.Fatalf("request changed: %+v", request)
			}
		})
	}
}
