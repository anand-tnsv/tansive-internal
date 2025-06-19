// Package toolgraph provides a call graph for tracking tool invocations.
// The implementation uses a simplistic DAG to track the ancestry of tool invocations and to prevent loops.
// It is also used to get the tool name for a given callID.

package toolgraph

import (
	"fmt"
	"sync"
)

type CallID string
type ToolName string

type CallGraph struct {
	mu        sync.RWMutex
	parents   map[CallID]CallID   // childID → parentID
	toolNames map[CallID]ToolName // callID → tool name
	maxDepth  int
}

func NewCallGraph(maxDepth int) *CallGraph {
	return &CallGraph{
		parents:   make(map[CallID]CallID),
		toolNames: make(map[CallID]ToolName),
		maxDepth:  maxDepth,
	}
}

// RegisterCall links parentCallID → toolName → newCallID.
// Returns error if this would cause a loop or exceed depthLimit.
func (g *CallGraph) RegisterCall(parentID CallID, toolName ToolName, newCallID CallID) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	depth := 0
	for id := parentID; id != ""; id = g.parents[id] {
		if g.toolNames[id] == toolName {
			return fmt.Errorf("loop detected: tool %s already in ancestry", toolName)
		}
		depth++
		if g.maxDepth > 0 && depth >= g.maxDepth {
			return fmt.Errorf("call depth limit exceeded: limit=%d", g.maxDepth)
		}
	}

	// Safe to register
	g.parents[newCallID] = parentID
	g.toolNames[newCallID] = toolName
	return nil
}

// GetToolName returns the tool name for a given callID.
func (g *CallGraph) GetToolName(callID CallID) ToolName {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.toolNames[callID]
}

// DebugGraph returns ancestry for a given callID.
func (g *CallGraph) DebugGraph(callID CallID) []string {
	g.mu.RLock()
	defer g.mu.RUnlock()

	var lineage []string
	for id := callID; id != ""; id = g.parents[id] {
		line := fmt.Sprintf("%s (%s)", id, g.toolNames[id])
		lineage = append([]string{line}, lineage...)
	}
	return lineage
}
