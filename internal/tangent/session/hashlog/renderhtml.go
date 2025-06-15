package hashlog

import (
	"bufio"
	"fmt"
	"html"
	"os"
	"path/filepath"
	"strings"
	"time"
)

func RenderHashedLogToHTML(path string) error {
	type SkillNode struct {
		ID        string
		InvokerID string
		Entries   []HashedLogEntry
		Children  []*SkillNode
	}

	// Read and group entries by invocation_id
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("failed to open log file: %w", err)
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	invocationMap := make(map[string][]HashedLogEntry)
	invokerMap := make(map[string]string)
	firstSessionID := ""

	for scanner.Scan() {
		var entry HashedLogEntry
		if err := json.Unmarshal(scanner.Bytes(), &entry); err != nil {
			continue
		}
		p := entry.Payload
		invID := str(p["invocation_id"])
		invokerID := str(p["invoker_id"])
		if invID == "" {
			invID = "__general__"
		}
		invocationMap[invID] = append(invocationMap[invID], entry)
		if _, ok := invokerMap[invID]; !ok {
			invokerMap[invID] = invokerID
		}
		if firstSessionID == "" {
			firstSessionID = str(p["session_id"])
		}
	}

	// Build invocation tree
	nodes := make(map[string]*SkillNode)
	var roots []*SkillNode

	for id, entries := range invocationMap {
		node := &SkillNode{
			ID:        id,
			InvokerID: invokerMap[id],
			Entries:   entries,
		}
		nodes[id] = node
	}
	for _, node := range nodes {
		if node.InvokerID == "" || nodes[node.InvokerID] == nil {
			roots = append(roots, node)
		} else {
			nodes[node.InvokerID].Children = append(nodes[node.InvokerID].Children, node)
		}
	}

	// Start HTML output
	htmlPath := strings.TrimSuffix(path, filepath.Ext(path)) + ".html"
	out, err := os.Create(htmlPath)
	if err != nil {
		return fmt.Errorf("failed to create html output: %w", err)
	}
	defer out.Close()

	fmt.Fprint(out, `<html><head><meta charset="UTF-8"><title>Tansive™ Session Log</title>
<style>
:root {
  --entry-bg: #fefefe;
}
@media (prefers-color-scheme: dark) {
  :root {
    --entry-bg: #1b1b1b;
  }
}
body {
  font-family: sans-serif;
  margin: 2em;
  background: #fff;
  color: #000;
}
@media (prefers-color-scheme: dark) {
  body { background: #111; color: #ccc; }
}
h1 { font-size: 1.6em; margin-bottom: 0.3em; }
h2 { font-weight: normal; color: #888; font-size: 1em; margin-bottom: 1.5em; }
.entry {
  border-left: 4px solid #ccc;
  margin: 1em 0;
  padding: 1em;
  background: var(--entry-bg);
}
.time { font-size: 0.9em; color: #888; margin-bottom: 0.5em; }
.label { font-weight: 600; margin-top: 0.3em; }
.value { margin-left: 0.5em; color: #bbb; font-weight: normal; }
.level {
  font-size: 0.75em;
  padding: 2px 6px;
  border-radius: 4px;
  margin-left: 6px;
  display: inline-block;
  font-weight: bold;
}
.level-info { background: #eaf5ff; color: #0366d6; }
.level-error { background: #ffeef0; color: #d73a49; }
.input, .actions, .error, .basis {
  margin-top: 0.75em;
  font-family: monospace;
  white-space: pre-wrap;
}
pre {
  background: #222;
  color: #ddd;
  padding: 0.5em;
  border-radius: 4px;
  overflow-x: auto;
}
@media (prefers-color-scheme: light) {
  pre { background: #f6f8fa; color: #333; }
}
details summary {
  font-size: 1em;
  font-weight: bold;
  cursor: pointer;
  margin-bottom: 0.5em;
}
.indent { margin-left: 2em; }
</style></head><body>
<h1>Tansive™ Session Log</h1>
<h2>Session: `+html.EscapeString(firstSessionID)+`</h2>
`)

	var renderNode func(node *SkillNode, depth int)
	renderNode = func(node *SkillNode, depth int) {
		skillName := "Skill Invocation"
		for _, e := range node.Entries {
			if name := str(e.Payload["skill"]); name != "" {
				skillName = name
				break
			}
		}
		prefix := strings.Repeat("↳ ", depth)
		fmt.Fprintf(out, `<details open><summary>%s🧠 %s</summary><div class="indent">`, prefix, html.EscapeString(skillName))

		for _, entry := range node.Entries {
			p := entry.Payload
			event := strings.ToUpper(str(p["event"]))
			decision := strings.ToUpper(str(p["decision"]))
			level := str(p["level"])
			levelClass := "level"
			switch level {
			case "info":
				levelClass += " level-info"
			case "error":
				levelClass += " level-error"
			}

			fmt.Fprint(out, `<div class="entry">`)
			if rawTime, ok := p["time"]; ok {
				switch v := rawTime.(type) {
				case float64:
					ts := time.UnixMilli(int64(v)).Local().Format("2006-01-02 15:04:05 MST")
					fmt.Fprintf(out, `<div class="time">%s</div>`, html.EscapeString(ts))
				case int64:
					ts := time.UnixMilli(v).Local().Format("2006-01-02 15:04:05 MST")
					fmt.Fprintf(out, `<div class="time">%s</div>`, html.EscapeString(ts))
				case string:
					// Fallback: maybe it's already a formatted string
					if parsed, err := time.Parse(time.RFC3339, v); err == nil {
						fmt.Fprintf(out, `<div class="time">%s</div>`, html.EscapeString(parsed.Local().Format("2006-01-02 15:04:05 MST")))
					} else {
						fmt.Fprintf(out, `<div class="time">%s</div>`, html.EscapeString(v))
					}
				default:
					fmt.Fprintf(out, `<div class="time">%s</div>`, html.EscapeString(str(rawTime)))
				}
			}

			if event != "" {
				fmt.Fprintf(out, `<div class="field"><span class="label">Event:</span><span class="value">%s</span></div>`, html.EscapeString(event))
			}
			for _, k := range []string{"actor", "runner", "message", "view"} {
				if v := str(p[k]); v != "" {
					fmt.Fprintf(out, `<div class="field"><span class="label">%s:</span><span class="value">%s</span></div>`, strings.Title(k), html.EscapeString(v))
				}
			}
			if decision != "" {
				fmt.Fprintf(out, `<div class="field"><span class="label">Decision:</span><span class="value">%s</span></div>`, html.EscapeString(decision))
			}
			if level != "" {
				fmt.Fprintf(out, `<span class="%s">%s</span>`, levelClass, html.EscapeString(level))
			}
			if status := str(p["status"]); status != "" {
				fmt.Fprintf(out, `<div class="field"><span class="label">Status:</span><span class="value">%s</span></div>`, html.EscapeString(status))
			}
			if errVal, ok := p["error"]; ok {
				fmt.Fprintf(out, `<div class="error"><strong>Error:</strong> %s</div>`, html.EscapeString(fmt.Sprintf("%v", errVal)))
			}
			if args, ok := p["input_args"]; ok {
				if b, err := json.MarshalIndent(args, "", "  "); err == nil {
					fmt.Fprintf(out, `<div class="input"><strong>Input Args:</strong><br>%s</div>`, html.EscapeString(string(b)))
				}
			}
			if basis, ok := p["basis"]; ok {
				if b, err := json.MarshalIndent(basis, "", "  "); err == nil {
					fmt.Fprintf(out, `<div class="basis"><strong>Policy Basis:</strong><br><pre>%s</pre></div>`, html.EscapeString(string(b)))
				}
			}
			if acts, ok := p["actions"]; ok {
				if b, err := json.MarshalIndent(acts, "", "  "); err == nil {
					fmt.Fprintf(out, `<div class="actions"><strong>Actions:</strong><br><pre>%s</pre></div>`, html.EscapeString(string(b)))
				}
			}
			fmt.Fprint(out, `</div>`)
		}

		for _, child := range node.Children {
			renderNode(child, depth+1)
		}
		fmt.Fprint(out, `</div></details>`)
	}

	for _, root := range roots {
		renderNode(root, 0)
	}

	fmt.Fprint(out, `</body></html>`)
	return nil
}

func str(v any) string {
	if v == nil {
		return ""
	}
	return fmt.Sprintf("%v", v)
}
