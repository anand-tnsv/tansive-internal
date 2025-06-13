package cli

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/fatih/color"
)

type sessionState struct {
	startTime   int64
	endTime     int64
	initialized bool
	skillsSeen  map[string]bool
}

var sessions = map[string]*sessionState{}
var skillColors = map[string]*color.Color{}

var sessionLabel = color.New(color.FgHiMagenta, color.Bold)

// var timeLabel = color.New(color.FgWhite)
var startLabel = color.New(color.FgGreen).Add(color.Bold)
var endLabel = color.New(color.FgRed).Add(color.Bold)

// Predefined palette of distinct colors for skills
var colorPalette = []*color.Color{
	color.New(color.FgBlue),
	color.New(color.FgGreen),
	color.New(color.FgCyan),
	color.New(color.FgMagenta),
	color.New(color.FgYellow),
	color.New(color.FgRed),
}

var colorIndex = 0

func PrettyPrintNDJSONLine(line []byte) {
	var m map[string]any
	if err := json.Unmarshal(line, &m); err != nil {
		fmt.Printf("⚠️  Invalid JSON: %s\n", string(line))
		return
	}

	sessionID := str(m["session_id"])
	skill := str(m["skill"])
	msg := str(m["message"])
	source := str(m["source"])
	t := int64From(m["time"]) // milliseconds since epoch

	// Initialize session if needed
	sess := sessions[sessionID]
	if sess == nil {
		sess = &sessionState{
			startTime:   t,
			endTime:     t,
			initialized: true,
			skillsSeen:  make(map[string]bool),
		}
		sessions[sessionID] = sess
		startTime := time.UnixMilli(t).Local()

		sessionLabel.Printf("\nSession ID: %s\n", sessionID)
		startLabel.Printf("    Start: %s\n\n", startTime.Format("2006-01-02 15:04:05.000 MST"))
	} else {
		if t > sess.endTime {
			sess.endTime = t
		}
	}

	// Assign color to skill if new
	if skill != "" && skillColors[skill] == nil {
		skillColors[skill] = colorPalette[colorIndex%len(colorPalette)]
		colorIndex++
	}
	skillColor := skillColors[skill]

	// Compute relative timestamp
	relative := time.Duration(t-sess.startTime) * time.Millisecond
	timestamp := fmt.Sprintf("[%02d:%02d.%03d]",
		int(relative.Minutes()),
		int(relative.Seconds())%60,
		relative.Milliseconds()%1000,
	)

	msg = indentMultiline(msg, "                                   ")

	// Print timestamp and skill name first
	fmt.Print("  " + timestamp + " ")
	skillColor.Printf("%s", skill)

	// stderr: only ❗ and message in red
	if source == "stderr" {
		fmt.Print(" ")
		color.New(color.FgHiRed).Print("❗ ")
		color.New(color.FgHiRed).Println(msg)
	} else {
		fmt.Print(" ▶ ")
		fmt.Println(msg)
	}

	// Print end time if message indicates session completion
	if strings.Contains(msg, "Interactive skill completed successfully") {
		endTime := time.UnixMilli(sess.endTime).Local()
		endLabel.Printf("\n    End:   %s\n", endTime.Format("2006-01-02 15:04:05.000 MST"))
	}
}

func str(v any) string {
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}

func int64From(v any) int64 {
	switch x := v.(type) {
	case float64:
		return int64(x)
	case int64:
		return x
	default:
		return 0
	}
}

func indentMultiline(text, indent string) string {
	lines := strings.Split(text, "\n")
	if len(lines) <= 1 {
		return text
	}
	for i := 1; i < len(lines); i++ {
		lines[i] = indent + lines[i]
	}
	return strings.Join(lines, "\n")
}
