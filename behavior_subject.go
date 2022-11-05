package rxgo

import (
	"sync"
)

type BehaviorSubject[T any] interface {
	Next(value T)
	Error(err error)
	Complete()
	Value() T
}

type subject[T any] struct {
	mu        sync.RWMutex
	closed    bool
	err       error
	observers []Observer[T]
}

func (s *subject[T]) Closed() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.closed
}

func (s *subject[T]) Unsubscribe() {
	s.mu.Lock()
	s.closed = true
	s.observers = []Observer[T]{}
	s.mu.Unlock()
}

type behaviorSubject[T any] struct {
	subject[T]
	v T
}

func (s *behaviorSubject[T]) Next(value T) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.v = value
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
	return s.v
}

func (s *behaviorSubject[T]) Subscribe(onNext OnNextFunc[T], onError OnErrorFunc, onComplete OnCompleteFunc) Subscription {
	s.mu.Lock()
	defer s.mu.Unlock()
	// if closed, we just return the empty subscription because nothing will emit
	if s.closed {
		return &emptySubscription{}
	}
	s.observers = append(s.observers, NewObserver(onNext, onError, onComplete))
	return nil
}

func NewBehaviorSubject[T any](value ...T) BehaviorSubject[T] {
	var v T
	if len(value) > 0 {
		v = value[0]
	}
	return &behaviorSubject[T]{v: v}
}
