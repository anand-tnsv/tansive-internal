package hashlog

import (
	"bufio"
	"crypto/hmac"
	"crypto/sha256"
	"fmt"
	"io"
)

func VerifyHashedLog(r io.Reader, hmacKey []byte) error {
	scanner := bufio.NewScanner(r)
	lineNum := 0
	expectedPrevHash := ""

	for scanner.Scan() {
		lineNum++
		line := scanner.Bytes()

		var entry HashedLogEntry
		if err := json.Unmarshal(line, &entry); err != nil {
			return fmt.Errorf("line %d: invalid JSON: %w", lineNum, err)
		}

		// Verify hash
		hashInput := struct {
			Payload  map[string]any `json:"payload"`
			PrevHash string         `json:"prevHash"`
		}{
			Payload:  entry.Payload,
			PrevHash: entry.PrevHash,
		}
		hashData, err := json.Marshal(hashInput)
		if err != nil {
			return fmt.Errorf("line %d: failed to marshal hash input: %w", lineNum, err)
		}
		computedHash := fmt.Sprintf("%x", sha256.Sum256(hashData))
		if entry.Hash != computedHash {
			return fmt.Errorf("line %d: hash mismatch", lineNum)
		}

		// Verify hash chain
		if entry.PrevHash != expectedPrevHash {
			return fmt.Errorf("line %d: prevHash mismatch", lineNum)
		}

		// Verify HMAC
		hmacInput := struct {
			Payload  map[string]any `json:"payload"`
			PrevHash string         `json:"prevHash"`
			Hash     string         `json:"hash"`
		}{
			Payload:  entry.Payload,
			PrevHash: entry.PrevHash,
			Hash:     entry.Hash,
		}
		hmacData, err := json.Marshal(hmacInput)
		if err != nil {
			return fmt.Errorf("line %d: failed to marshal HMAC input: %w", lineNum, err)
		}
		mac := hmac.New(sha256.New, hmacKey)
		mac.Write(hmacData)
		expectedHMAC := fmt.Sprintf("%x", mac.Sum(nil))
		if entry.HMAC != expectedHMAC {
			return fmt.Errorf("line %d: HMAC mismatch", lineNum)
		}

		expectedPrevHash = entry.Hash
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("failed to read stream: %w", err)
	}

	return nil
}

// func RenderHashedLogToHTML(path string) error {
// 	f, err := os.Open(path)
// 	if err != nil {
// 		return fmt.Errorf("failed to open log file: %w", err)
// 	}
// 	defer f.Close()

// 	// Read session_id from first line
// 	reader := bufio.NewReader(f)
// 	firstLine, err := reader.ReadBytes('\n')
// 	if err != nil && err != io.EOF {
// 		return fmt.Errorf("failed to read first line: %w", err)
// 	}

// 	var firstEntry HashedLogEntry
// 	if err := json.Unmarshal(firstLine, &firstEntry); err != nil {
// 		return fmt.Errorf("failed to parse first line for session ID: %w", err)
// 	}
// 	sessionID := str(firstEntry.Payload["session_id"])

// 	// Reset stream to beginning
// 	if _, err := f.Seek(0, io.SeekStart); err != nil {
// 		return fmt.Errorf("failed to rewind file: %w", err)
// 	}
// 	scanner := bufio.NewScanner(f)

// 	htmlPath := strings.TrimSuffix(path, filepath.Ext(path)) + ".html"
// 	out, err := os.Create(htmlPath)
// 	if err != nil {
// 		return fmt.Errorf("failed to create html output: %w", err)
// 	}
// 	defer out.Close()

// 	fmt.Fprint(out, `<html><head><meta charset="UTF-8"><title>Tansive™ Session Log</title>
// <style>
// body {
//   font-family: sans-serif;
//   margin: 2em;
//   background: #fff;
//   color: #000;
// }
// h1 {
//   font-size: 1.6em;
//   margin-bottom: 0.3em;
// }
// h2 {
//   font-weight: normal;
//   color: #888;
//   font-size: 1em;
//   margin-bottom: 1.5em;
// }
// .entry {
//   border-left: 4px solid #ccc;
//   margin: 1em 0;
//   padding: 1em;
//   box-shadow: 0 1px 2px rgba(0,0,0,0.05);
//   background: #fefefe;
// }

// @media (prefers-color-scheme: dark) {
//   body { background: #111; color: #ccc; }
//   .entry {
//     background: #1b1b1b;
//     border-left-color: #444;
//     box-shadow: none;
//   }
// }

// .time {
//   font-size: 0.9em;
//   color: #888;
//   margin-bottom: 0.5em;
// }
// .label {
//   font-weight: 600;
//   margin-top: 0.3em;
// }
// .value {
//   margin-left: 0.5em;
//   color: #bbb;
//   font-weight: normal;
// }
// .level {
//   font-size: 0.75em;
//   padding: 2px 6px;
//   border-radius: 4px;
//   margin-left: 6px;
//   display: inline-block;
//   font-weight: bold;
// }
// .level-info {
//   background: #eaf5ff;
//   color: #0366d6;
// }
// .level-error {
//   background: #ffeef0;
//   color: #d73a49;
// }
// .input, .actions, .error, .basis {
//   margin-top: 0.75em;
//   font-family: monospace;
//   white-space: pre-wrap;
// }
// pre {
//   background: #222;
//   color: #ddd;
//   padding: 0.5em;
//   border-radius: 4px;
//   overflow-x: auto;
// }
// @media (prefers-color-scheme: light) {
//   pre {
//     background: #f6f8fa;
//     color: #333;
//   }
// }
// details summary {
//   font-size: 1em;
//   font-weight: bold;
//   cursor: pointer;
//   margin-bottom: 0.5em;
// }
// </style>
// </head><body>
// <h1>Tansive™ Session Log</h1>
// <h2>Session: `+html.EscapeString(sessionID)+`</h2>
// `)

// 	var buffer []HashedLogEntry
// 	var currentSkill string
// 	var collecting bool

// 	flushSkillBlock := func() {
// 		if len(buffer) == 0 {
// 			return
// 		}
// 		title := "General"
// 		if currentSkill != "" {
// 			title = currentSkill
// 		}
// 		fmt.Fprintf(out, `<details open><summary>🧠 %s</summary><div class="skill-group">`, html.EscapeString(title))
// 		for _, entry := range buffer {
// 			p := entry.Payload
// 			event := strings.ToUpper(str(p["event"]))
// 			decision := strings.ToUpper(str(p["decision"]))
// 			level := str(p["level"])
// 			levelClass := "level"
// 			switch level {
// 			case "info":
// 				levelClass += " level-info"
// 			case "error":
// 				levelClass += " level-error"
// 			}

// 			fmt.Fprint(out, `<div class="entry">`)
// 			if t := str(p["time"]); t != "" {
// 				if parsed, err := time.Parse(time.RFC3339, t); err == nil {
// 					localTime := parsed.Local().Format("2006-01-02 15:04:05 MST")
// 					fmt.Fprintf(out, `<div class="time">%s</div>`, html.EscapeString(localTime))
// 				} else {
// 					fmt.Fprintf(out, `<div class="time">%s</div>`, html.EscapeString(t))
// 				}
// 			}
// 			if event != "" {
// 				fmt.Fprintf(out, `<div class="field"><span class="label">Event:</span><span class="value">%s</span></div>`, html.EscapeString(event))
// 			}
// 			for _, k := range []string{"skill_name", "actor", "runner", "message", "view", "session_id"} {
// 				if v := str(p[k]); v != "" {
// 					fmt.Fprintf(out, `<div class="field"><span class="label">%s:</span><span class="value">%s</span></div>`, strings.Title(k), html.EscapeString(v))
// 				}
// 			}

// 			if decision != "" {
// 				fmt.Fprintf(out, `<div class="field"><span class="label">Decision:</span><span class="value">%s</span></div>`, html.EscapeString(decision))
// 			}
// 			if level != "" {
// 				fmt.Fprintf(out, `<span class="%s">%s</span>`, levelClass, html.EscapeString(level))
// 			}
// 			if status := str(p["status"]); status != "" {
// 				fmt.Fprintf(out, `<span class="field">Status: %s</span>`, html.EscapeString(status))
// 			}
// 			if errVal, ok := p["error"]; ok {
// 				fmt.Fprintf(out, `<div class="error"><strong>Error:</strong> %s</div>`, html.EscapeString(fmt.Sprintf("%v", errVal)))
// 			}
// 			if args, ok := p["input_args"]; ok {
// 				if b, err := json.MarshalIndent(args, "", "  "); err == nil {
// 					fmt.Fprintf(out, `<div class="input"><strong>Input Args:</strong><br>%s</div>`, html.EscapeString(string(b)))
// 				}
// 			}
// 			if basis, ok := p["basis"]; ok {
// 				if b, err := json.MarshalIndent(basis, "", "  "); err == nil {
// 					fmt.Fprintf(out, `<div class="basis"><strong>Policy Basis:</strong><br><pre>%s</pre></div>`, html.EscapeString(string(b)))
// 				}
// 			}
// 			if acts, ok := p["actions"]; ok {
// 				if b, err := json.MarshalIndent(acts, "", "  "); err == nil {
// 					fmt.Fprintf(out, `<div class="actions"><strong>Actions:</strong><br><pre>%s</pre></div>`, html.EscapeString(string(b)))
// 				}
// 			}
// 			fmt.Fprint(out, `</div>`)
// 		}
// 		fmt.Fprint(out, `</div></details>`)
// 		buffer = nil
// 		currentSkill = ""
// 		collecting = false
// 	}

// 	for scanner.Scan() {
// 		var entry HashedLogEntry
// 		if err := json.Unmarshal(scanner.Bytes(), &entry); err != nil {
// 			continue
// 		}
// 		event := str(entry.Payload["event"])
// 		skill := str(entry.Payload["skill"])

// 		switch event {
// 		case "skill_start":
// 			flushSkillBlock()
// 			collecting = true
// 			currentSkill = skill
// 			buffer = append(buffer, entry)
// 		case "skill_completed":
// 			buffer = append(buffer, entry)
// 			flushSkillBlock()
// 		default:
// 			if collecting {
// 				buffer = append(buffer, entry)
// 			} else {
// 				buffer = append(buffer, entry)
// 				flushSkillBlock()
// 			}
// 		}
// 	}
// 	flushSkillBlock()

// 	fmt.Fprint(out, `</body></html>`)
// 	return nil
// }

// func str(v any) string {
// 	if v == nil {
// 		return ""
// 	}
// 	return fmt.Sprintf("%v", v)
// }
