package session

import (
	"fmt"

	"github.com/dop251/goja"
	"github.com/mugiliam/goja_nodejs/console"
	"github.com/mugiliam/goja_nodejs/util"
)

func gojatest() {
	vm := goja.New()

	// --- Provide console.log ---
	_ = util.SetGlobal(vm)
	err := console.SetGlobal(vm, nil)
	if err != nil {
		panic(fmt.Sprintf("Failed to set console: %v", err))
	}
	// --- Go function: greet(name) ---
	vm.Set("greet", func(call goja.FunctionCall) goja.Value {
		//	name := call.Argument(0).String()
		obj := call.Argument(0).ToObject(vm)
		if obj.ClassName() != "String" {
			panic("Expected a string")
		}
		name := obj.String() // Ensure we get the name property
		return vm.ToValue("Hello, " + name + "!")
	})

	// --- Go function: sumArray([1,2,3]) ---
	vm.Set("sumArray", func(call goja.FunctionCall) goja.Value {
		arg := call.Argument(0)

		obj := arg.ToObject(vm)
		if obj.ClassName() != "Array" {
			return vm.ToValue("Expected an array")
		}

		lengthVal := obj.Get("length")
		length := lengthVal.ToInteger()

		sum := int64(0)
		for i := int64(0); i < length; i++ {
			val := obj.Get(fmt.Sprintf("%d", i))
			sum += val.ToInteger()
		}

		return vm.ToValue(sum)
	})

	// --- Go function: printUser({ name: "...", age: ... }) ---
	vm.Set("printUser", func(call goja.FunctionCall) goja.Value {
		obj := call.Argument(0).ToObject(vm)
		name := obj.Get("name").String()
		age := obj.Get("age").ToInteger()
		something := obj.Get("something")
		if something != nil && !goja.IsUndefined(something) {
			fmt.Printf("Something: %s\n", something.Export())
		}
		fmt.Printf("User: %s, Age: %d\n", name, age)
		return goja.Undefined()
	})

	// --- Go function: getInfo() → returns JS object ---
	vm.Set("getInfo", func(call goja.FunctionCall) goja.Value {
		return vm.ToValue(map[string]interface{}{
			"status": "ok",
			"value":  42,
		})
	})

	// --- JS code that calls the Go functions ---
	jsCode := `
		console.log(greet("Anand"));
		console.log("Sum is:", sumArray([10, 20, 30]));
		printUser({ name: "Alice", age: 30 });
		let info = getInfo();
		console.log("Info from Go:", info.status, info.value);
	`

	if _, err := vm.RunString(jsCode); err != nil {
		panic(err)
	}
}
