package rxgo

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBehaviorSubject(t *testing.T) {
	t.Run("BehaviorSubject", func(t *testing.T) {
		s := NewBehaviorSubject[uint]()
		require.Equal(t, uint(0), s.Value())
		s.Next(188)
		require.Equal(t, uint(188), s.Value())

		var latestValue uint
		s.Next(111)
		s.Subscribe(func(u uint) {
			latestValue = u
		}, nil, nil)
		require.Equal(t, latestValue, uint(111))
	})
}
