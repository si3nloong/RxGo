package rxgo

type emptySubscription struct{}

func (*emptySubscription) Unsubscribe() {}
