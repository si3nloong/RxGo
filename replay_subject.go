package rxgo

import "time"

type ReplaySubject[T any] interface {
	Subscribe(OnNextFunc[T], OnErrorFunc, OnCompleteFunc) Subscription
	Next(value T)
	Error(err error)
	Complete()
}

type replayItem[T any] struct {
	t time.Time
	v T
}

type replaySubject[T any] struct {
	subject[T]
	bufferSize uint64
	queue      []replayItem[T]
	scheduler  any
}

func (s *replaySubject[T]) Next(value T) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	s.queue = append(s.queue, replayItem[T]{t: time.Now(), v: value})
	s.trim()
	for _, obs := range s.observers {
		obs.Next(value)
	}
}

func (s *replaySubject[T]) Error(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	s.err = err
	for _, obs := range s.observers {
		obs.Error(err)
	}
	s.observers = []Observer[T]{}
}

func (s *replaySubject[T]) Complete() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	s.closed = true
	for _, obs := range s.observers {
		obs.Complete()
	}
	s.observers = []Observer[T]{}
}

func (s *replaySubject[T]) Subscribe(onNext OnNextFunc[T], onError OnErrorFunc, onComplete OnCompleteFunc) Subscription {
	s.mu.Lock()
	defer s.mu.Unlock()
	// if closed, we just return the empty subscription because nothing will emit
	if s.closed {
		return &emptySubscription{}
	}
	obs := NewObserver(onNext, onError, onComplete)
	s.observers = append(s.observers, obs)
	for _, item := range s.queue {
		obs.Next(item.v)
	}
	if s.err != nil {
		obs.Error(s.err)
	} else if s.closed {
		obs.Complete()
	}
	return nil
}

func (s *replaySubject[T]) trim() {
	if uint64(len(s.queue)) > s.bufferSize {
		s.queue = s.queue[1:]
	}
	if s.scheduler != nil {

	}
}

func NewReplaySubject[T any]() ReplaySubject[T] {
	return &replaySubject[T]{}
}
