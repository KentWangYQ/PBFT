package events

type Event interface{}

type Receiver interface {
	ProcessEvent(e Event) Event
}

func SendEvent(receiver Receiver, event Event) {
	next := event
	for {
		next = receiver.ProcessEvent(next)
		if next == nil {
			break
		}
	}
}
