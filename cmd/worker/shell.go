package main

import (
	"encoding/json"
	"log"
	"net/http"
	"os"
	"os/exec"
	"time"

	"github.com/creack/pty"
	"github.com/gorilla/websocket"
)

type AsciinemaEvent struct {
	Time   float64
	Stream string // "o" or "i"
	Data   string
}

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true }, // for demo
}

func wsHandler(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println("upgrade error:", err)
		return
	}
	defer conn.Close()

	cmd := exec.Command("zsh", "-li")
	ptmx, err := pty.Start(cmd)
	if err != nil {
		log.Println("pty error:", err)
		return
	}
	defer ptmx.Close()

	start := time.Now()
	var events []AsciinemaEvent
	done := make(chan struct{})

	// capture output
	go func() {
		buf := make([]byte, 1024)
		for {
			n, err := ptmx.Read(buf)
			if n > 0 {
				data := buf[:n]
				conn.WriteMessage(websocket.TextMessage, data)
				events = append(events, AsciinemaEvent{
					Time:   time.Since(start).Seconds(),
					Stream: "o",
					Data:   string(data),
				})
			}
			if err != nil {
				break
			}
		}
		close(done)
	}()

	// capture input
	go func() {
		for {
			_, msg, err := conn.ReadMessage()
			if err != nil {
				break
			}
			ptmx.Write(msg)
			events = append(events, AsciinemaEvent{
				Time:   time.Since(start).Seconds(),
				Stream: "i",
				Data:   string(msg),
			})
		}
		cmd.Process.Kill()
	}()

	<-done
	cmd.Wait()

	// save asciinema v2 format
	saveAsciinemaRecording(events)
}

func saveAsciinemaRecording(events []AsciinemaEvent) {
	f, err := os.Create("session.cast")
	if err != nil {
		log.Println("failed to save session:", err)
		return
	}
	defer f.Close()

	header := map[string]interface{}{
		"version":   2,
		"width":     80,
		"height":    24,
		"timestamp": time.Now().Unix(),
		"env": map[string]string{
			"SHELL": "/bin/bash",
			"TERM":  "xterm-256color",
		},
	}
	headerJson, _ := json.Marshal(header)
	f.Write(headerJson)
	f.Write([]byte("\n"))

	for _, ev := range events {
		entry, _ := json.Marshal([]interface{}{ev.Time, ev.Stream, ev.Data})
		f.Write(entry)
		f.Write([]byte("\n"))
	}
}

func main2() {
	fs := http.FileServer(http.Dir("."))
	http.Handle("/", fs)
	http.HandleFunc("/ws", wsHandler)

	log.Println("Server listening on http://localhost:8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
