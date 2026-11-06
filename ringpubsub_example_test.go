/*
   Copyright 2026 Joseph Cumines

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package bigbuff_test

import (
	"fmt"

	"bigbuff"
)

// One goroutine can consume multiple subscriptions: advancing one cursor never
// waits for another cursor to acknowledge the same publication.
func ExampleRingPubSub_select() {
	hub := bigbuff.NewRingPubSub[int](2)
	first, second := hub.Subscribe(), hub.Subscribe()
	defer first.Close()
	defer second.Close()

	for _, value := range []int{10, 20} {
		if _, err := hub.TrySend(value); err != nil {
			panic(err)
		}
	}
	hub.Close() // Existing subscriptions still receive both queued values.

	firstReady, secondReady := first.Ready(), second.Ready()
	var firstValues, secondValues []int
	for firstReady != nil || secondReady != nil {
		select {
		case _, open := <-firstReady:
			if !open {
				first.Close()
				firstReady = nil // Disable this case after EOF.
				continue
			}
			value, ok := first.TryRecv()
			if !ok {
				panic("ready subscription has no value")
			}
			firstValues = append(firstValues, value)
			firstReady = first.Ready() // The cursor's next lane has changed.

		case _, open := <-secondReady:
			if !open {
				second.Close()
				secondReady = nil // Close alone would leave a ready EOF case.
				continue
			}
			value, ok := second.TryRecv()
			if !ok {
				panic("ready subscription has no value")
			}
			secondValues = append(secondValues, value)
			secondReady = second.Ready()
		}
	}

	// Select order is unspecified; each subscription's value order is fixed.
	fmt.Println("first:", firstValues)
	fmt.Println("second:", secondValues)
	// Output:
	// first: [10 20]
	// second: [10 20]
}
