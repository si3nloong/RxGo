package rxgo

import "sync"

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
