package session

import gonanoid "github.com/matoous/go-nanoid/v2"

func NewMarker() string {
	m, _ := gonanoid.New(12)
	return m
}

func DoneMarker() string {
	return "done"
}
