package rxgo

type BehaviorSubject[T any] interface {
	Subscribe(OnNextFunc[T], OnErrorFunc, OnCompleteFunc) Subscription
	Next(value T)
	Error(err error)
	Complete()
	Value() T
}

type behaviorSubject[T any] struct {
	subject[T]
	value T
}

func (s *behaviorSubject[T]) Next(value T) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.value = value
	s.mu.Unlock()
	for _, obs := range s.observers {
		obs.Next(value)
	}
}

func (s *behaviorSubject[T]) Error(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	s.err = err
	for len(s.observers) > 0 {
		s.observers[0].Error(err)
		s.observers = s.observers[1:]
	}
}

func (s *behaviorSubject[T]) Complete() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	s.closed = true
	for len(s.observers) > 0 {
		s.observers[0].Complete()
		s.observers = s.observers[1:]
	}
}

func (s *behaviorSubject[T]) Value() T {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.value
}

func (s *behaviorSubject[T]) Subscribe(onNext OnNextFunc[T], onError OnErrorFunc, onComplete OnCompleteFunc) Subscription {
	s.mu.Lock()
	defer s.mu.Unlock()
	// if closed, we just return the empty subscription because nothing will emit
	if s.closed {
		return &emptySubscription{}
	}
	obs := NewObserver(onNext, onError, onComplete)
	obs.Next(s.value)
	s.observers = append(s.observers, obs)
	return nil
}

func NewBehaviorSubject[T any](value ...T) BehaviorSubject[T] {
	var val T
	if len(value) > 0 {
		val = value[0]
	}
	return &behaviorSubject[T]{value: val}
}
