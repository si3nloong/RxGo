package rxgo

import (
	"errors"
	"fmt"
	"testing"
	"time"
)

func TestCombineLatestWith(t *testing.T) {
	// t.Run("CombineLatestWith Empty", func(t *testing.T) {
	// 	checkObservableResult(t, Pipe1(
	// 		Empty[any](),
	// 		CombineLatestWith(
	// 			Scheduled[any]("end"),
	// 			Pipe2(
	// 				Interval(time.Millisecond*100),
	// 				Map(func(v, _ uint) (any, error) {
	// 					return v, nil
	// 				}),
	// 				Take[any](10),
	// 			),
	// 		),
	// 	), nil, nil, true)
	// })

	// t.Run("CombineLatestWith with values", func(t *testing.T) {
	// 	checkObservableResults(t, Pipe2(
	// 		Interval(time.Millisecond*500),
	// 		CombineLatestWith(
	// 			Range[uint](1, 10),
	// 			Scheduled[uint](88),
	// 		),
	// 		Take[[]uint](1),
	// 	), [][]uint{{0, 10, 88}}, nil, true)
	// })
}

func TestForkJoin(t *testing.T) {
	// t.Run("ForkJoin with one Empty", func(t *testing.T) {
	// 	// ForkJoin only capture all latest value from every stream
	// 	checkObservableResult(t, ForkJoin(
	// 		Empty[any](),
	// 		Scheduled[any]("j", "k", "end"),
	// 		Pipe1(Range[uint](1, 10), Map(func(v, _ uint) (any, error) {
	// 			return v, nil
	// 		})),
	// 	), []any{nil, "end", uint(10)}, nil, true)
	// })

	// t.Run("ForkJoin with all Empty", func(t *testing.T) {
	// 	checkObservableResult(t, ForkJoin(
	// 		Empty[uint](),
	// 		Empty[uint](),
	// 		Empty[uint](),
	// 	), []uint{0, 0, 0}, nil, true)
	// })

	// t.Run("ForkJoin with error observable", func(t *testing.T) {
	// 	var err = fmt.Errorf("failed")
	// 	checkObservableResult(t, ForkJoin(
	// 		Scheduled[uint](1, 88, 2, 7215251),
	// 		Pipe1(Interval(time.Millisecond*10), Map(func(v, _ uint) (uint, error) {
	// 			return v, err
	// 		})),
	// 		Interval(time.Millisecond*100),
	// 	), nil, err, false)
	// })

	// t.Run("ForkJoin with multiple error", func(t *testing.T) {
	// 	createErr := func(index uint) error {
	// 		return fmt.Errorf("failed at %d", index)
	// 	}
	// 	checkObservableResultWithAnyError(t, ForkJoin(
	// 		Throw[string](func() error {
	// 			return createErr(1)
	// 		}),
	// 		Throw[string](func() error {
	// 			return createErr(2)
	// 		}),
	// 		Throw[string](func() error {
	// 			return createErr(3)
	// 		}),
	// 		Scheduled("a"),
	// 	), nil, []error{createErr(1), createErr(2), createErr(3)}, false)
	// })

	// t.Run("ForkJoin with complete", func(t *testing.T) {
	// 	checkObservableResult(t, ForkJoin(
	// 		Scheduled[uint](1, 88, 2, 7215251),
	// 		Pipe1(Interval(time.Millisecond*10), Take[uint](3)),
	// 	), []uint{7215251, 2}, nil, true)
	// })
}

func TestConcatAll(t *testing.T) {
	t.Run("ConcatAll with Empty", func(t *testing.T) {
		checkObservableResults(t, Pipe2(
			Range[uint](1, 5),
			Map(func(v, _ uint) (Observable[string], error) {
				return Empty[string](), nil
			}),
			ConcatAll[string](),
		), []string{}, nil, true)
	})

	t.Run("ConcatAll with errors", func(t *testing.T) {
		var err = fmt.Errorf("concat failed")
		checkObservableResults(t, Pipe2(
			Range[uint](1, 5),
			Map(func(v, _ uint) (Observable[any], error) {
				return Throw[any](func() error {
					return err
				}), nil
			}),
			ConcatAll[any](),
		), []any{}, err, false)
	})

	t.Run("ConcatAll with Interval", func(t *testing.T) {
		checkObservableResults(t, Pipe2(
			Range[uint](1, 5),
			Map(func(v, _ uint) (Observable[uint], error) {
				return Pipe1(
					Interval(time.Millisecond),
					Take[uint](4),
				), nil
			}),
			ConcatAll[uint](),
		), []uint{
			0, 1, 2, 3,
			0, 1, 2, 3,
			0, 1, 2, 3,
			0, 1, 2, 3,
			0, 1, 2, 3,
		}, nil, true)
	})
}

func TestMergeWith(t *testing.T) {
	t.Run("MergeWith all EMTPY", func(t *testing.T) {
		// checkObservableResults(t, Pipe1(
		// 	Empty[any](),
		// 	MergeWith(
		// 		Empty[any](),
		// 		Empty[any](),
		// 	),
		// ), []any{}, nil, true)
	})

	t.Run("MergeWith multiple EMTPY", func(t *testing.T) {
		// checkObservableResults(t, Pipe1(
		// 	Of2[any]("a", "b", "q", "j", "z"),
		// 	MergeWith(
		// 		Empty[any](),
		// 		Empty[any](),
		// 	),
		// ), []any{"a", "b", "q", "j", "z"}, nil, true)
	})

	t.Run("MergeWith Interval", func(t *testing.T) {
		// checkObservableResults(t, Pipe1(
		// 	Pipe2(
		// 		Interval(time.Millisecond),
		// 		Take[uint](3),
		// 		Map(func(v uint, _ uint) (string, error) {
		// 			return fmt.Sprintf("a -> %v", v), nil
		// 		}),
		// 	),
		// 	MergeWith(
		// 		Pipe2(
		// 			Interval(time.Millisecond*500),
		// 			Take[uint](5),
		// 			Map(func(v uint, _ uint) (string, error) {
		// 				return fmt.Sprintf("b -> %v", v), nil
		// 			}),
		// 		),
		// 		Empty[string](),
		// 	),
		// ), []string{
		// 	"a -> 0", "a -> 1", "a -> 2",
		// 	"b -> 0", "b -> 1", "b -> 2", "b -> 3", "b -> 4",
		// }, nil, true)
	})

	t.Run("MergeWith Of", func(t *testing.T) {
		// checkObservableHasResults(t, Pipe1(
		// 	Of2[any]("a", "b", "q", "j", "z"),
		// 	MergeWith(Pipe1(
		// 		Range[uint](1, 10),
		// 		Map(func(v, _ uint) (any, error) {
		// 			return any(v), nil
		// 		}),
		// 	)),
		// ), true, nil, true)
	})

	// t.Run("MergeWith error", func(t *testing.T) {
	// 	var err = errors.New("cannot more than 5")
	// 	checkObservableHasResults(t, Pipe1(
	// 		Of2[any]("a", "b", "q", "j", "z"),
	// 		MergeWith(Pipe1(
	// 			Range[uint](1, 10),
	// 			Map(func(v, _ uint) (any, error) {
	// 				if v > 5 {
	// 					return nil, err
	// 				}
	// 				return any(v), nil
	// 			}),
	// 		)),
	// 	), true, err, false)
	// })

	t.Run("MergeWith all errors", func(t *testing.T) {
		// var err = errors.New("failed")
		// checkObservableHasResults(t, Pipe1(
		// 	Throw[any](func() error {
		// 		return err
		// 	}),
		// 	MergeWith(
		// 		Throw[any](func() error {
		// 			return err
		// 		}),
		// 		Throw[any](func() error {
		// 			return err
		// 		}),
		// 	),
		// ), false, err, false)
	})
}

func TestPartition(t *testing.T) {
	t.Run("Partition with Empty", func(t *testing.T) {})

	t.Run("Partition with error", func(t *testing.T) {})

	t.Run("Partition", func(t *testing.T) {})
}

func TestRaceWith(t *testing.T) {
	t.Run("RaceWith with Empty", func(t *testing.T) {
		// checkObservableResults(t, Pipe1(
		// 	Empty[any](),
		// 	RaceWith(Empty[any](), Empty[any]()),
		// ), nil, nil, true)
	})

	t.Run("RaceWith with error", func(t *testing.T) {})

	t.Run("RaceWith with Interval", func(t *testing.T) {
		//		checkObservableResults(t, Pipe2(
		//			Pipe1(Interval(time.Millisecond*7), Map(func(v uint, _ uint) (string, error) {
		//				return fmt.Sprintf("slowest -> %v", v), nil
		//			})),
		//			RaceWith(
		//				Pipe1(Interval(time.Millisecond*3), Map(func(v uint, _ uint) (string, error) {
		//					return fmt.Sprintf("fastest -> %v", v), nil
		//				})),
		//				Pipe1(Interval(time.Millisecond*5), Map(func(v uint, _ uint) (string, error) {
		//					return fmt.Sprintf("average -> %v", v), nil
		//				})),
		//			),
		//			Take[string](5),
		//		),
		//			[]string{"fastest -> 0"}, // "fastest -> 1", "fastest -> 2", "fastest -> 3", "fastest -> 4"
		//			nil, true)
	})
}

func TestZipWith(t *testing.T) {
	t.Run("Zip with all Empty", func(t *testing.T) {
		checkObservableResults(t, Pipe1(
			Empty[any](),
			ZipWith(
				Empty[any](),
				Empty[any](),
			),
		), [][]any{}, nil, true)
	})

	t.Run("Zip with Throw", func(t *testing.T) {
		var err = errors.New("stop")
		checkObservableResults(t, Pipe1(
			Throw[any](func() error {
				return err
			}),
			ZipWith(
				Of2[any]("Foo", "Bar", "Beer"),
				Of2[any](true, true, false),
			),
		), [][]any{}, err, false)
	})

	t.Run("Zip with error", func(t *testing.T) {
		var err = errors.New("stop")
		checkObservableResults(t, Pipe2(
			Of2[any](27, 25, 29),
			ZipWith(
				Of2[any]("Foo", "Bar", "Beer"),
				Of2[any](true, true, false),
			),
			Map(func(v []any, i uint) ([]any, error) {
				if i >= 2 {
					return nil, err
				}
				return v, nil
			}),
		), [][]any{
			{27, "Foo", true},
			{25, "Bar", true},
		}, err, false)
	})

	t.Run("Zip with Empty and Of", func(t *testing.T) {
		checkObservableResults(t, Pipe1(
			Empty[any](),
			ZipWith(
				Of2[any]("Foo", "Bar", "Beer"),
				Of2[any](true, true, false),
			),
		), [][]any{}, nil, true)
	})

	t.Run("Zip with Of (not tally)", func(t *testing.T) {
		checkObservableResults(t, Pipe1(
			Of2[any](27, 25, 29),
			ZipWith(
				Of2[any]("Foo", "Beer"),
				Of2[any](true, true, false),
			),
		), [][]any{
			{27, "Foo", true},
			{25, "Beer", true},
		}, nil, true)
	})

	t.Run("Zip with Of (tally)", func(t *testing.T) {
		checkObservableResults(t, Pipe1(
			Scheduled[any](27, 25, 29),
			ZipWith(
				Scheduled[any]("Foo", "Bar", "Beer"),
				Scheduled[any](true, true, false),
			),
		), [][]any{
			{27, "Foo", true},
			{25, "Bar", true},
			{29, "Beer", false},
		}, nil, true)
	})
}

func TestZipAll(t *testing.T) {
	t.Run("ZipAll with Empty", func(t *testing.T) {
		checkObservableResults(t, Pipe2(
			Range[uint](1, 5),
			Map(func(v, _ uint) (Observable[string], error) {
				return Empty[string](), nil
			}),
			ZipAll[string](),
		), [][]string{}, nil, true)
	})

	t.Run("ZipAll with mutiple errors", func(t *testing.T) {
		var err = fmt.Errorf("ZipAll failed")
		checkObservableResults(t, Pipe2(
			Range[uint](1, 3),
			Map(func(v, _ uint) (Observable[any], error) {
				return Throw[any](func() error {
					return err
				}), nil
			}),
			ZipAll[any](),
		), [][]any{}, err, false)
	})

	t.Run("ZipAll with items (not tally)", func(t *testing.T) {
		checkObservableResults(t, Pipe2(
			Range[uint](1, 3),
			Map(func(v, _ uint) (Observable[string], error) {
				arr := []string{}
				for i := uint(0); i < v; i++ {
					arr = append(arr, fmt.Sprintf("a[%d][%d]", i, v))
				}
				return Of2(arr[0], arr[1:]...), nil
			}),
			ZipAll[string](),
		), [][]string{
			{"a[0][1]", "a[0][2]", "a[0][3]"},
		}, nil, true)
	})

	t.Run("ZipAll with items (tally)", func(t *testing.T) {
		checkObservableResults(t, Pipe2(
			Range[uint](1, 3),
			Map(func(v, _ uint) (Observable[string], error) {
				arr := []string{}
				for i := uint(0); i < 5; i++ {
					arr = append(arr, fmt.Sprintf("a[%d][%d]", i, v))
				}
				return Of2(arr[0], arr[1:]...), nil
			}),
			ZipAll[string](),
		), [][]string{
			{"a[0][1]", "a[0][2]", "a[0][3]"},
			{"a[1][1]", "a[1][2]", "a[1][3]"},
			{"a[2][1]", "a[2][2]", "a[2][3]"},
			{"a[3][1]", "a[3][2]", "a[3][3]"},
			{"a[4][1]", "a[4][2]", "a[4][3]"},
		}, nil, true)
	})
}
