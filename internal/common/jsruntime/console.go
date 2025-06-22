package jsruntime

import (
	"log"

	"github.com/dop251/goja"
)

func bindConsole(vm *goja.Runtime) {
	console := vm.NewObject()
	_ = console.Set("log", func(call goja.FunctionCall) goja.Value {
		args := make([]any, len(call.Arguments))
		for i, arg := range call.Arguments {
			args[i] = arg.Export()
		}
		log.Println(args...)
		return goja.Undefined()
	})
	_ = vm.Set("console", console)
}
