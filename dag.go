package godag

import (
	"errors"
	"fmt"
	"strings"
)

// A Set is a unordered collection of unique elements
type Set[E comparable] struct {
	Elems map[E]struct{}
}

// NewSet Creates an empty set, optionally with a size to allocate
func NewSet[E comparable](size ...int) Set[E] {
	s := Set[E]{}
	if len(size) == 0 {
		s.Elems = make(map[E]struct{})
	} else {
		s.Elems = make(map[E]struct{}, size[0])
	}
	return s
}

// SetFrom creates a set from a slice
func SetFrom[E comparable](elems []E) Set[E] {
	s := NewSet[E](len(elems))
	for _, elem := range elems {
		s.Add(elem)
	}
	return s
}

// IsZero returns true if this is the zero set
func (s *Set[E]) IsZero() bool {
	return s.Elems == nil
}

// Add adds an element to the set
func (s *Set[E]) Add(elem E) {
	s.Elems[elem] = struct{}{}
}

// Remove removes an element from the set
func (s *Set[E]) Remove(elem E) {
	delete(s.Elems, elem)
}

// Contains returns true if an element is in the set
func (s *Set[E]) Contains(elem E) bool {
	_, ok := s.Elems[elem]
	return ok
}

// Len returns the number of elements in the set
func (s *Set[E]) Len() int {
	return len(s.Elems)
}

// A Semaphore tracks a number of available resources and blocks if none are available
type Semaphore chan struct{}

// NewSemaphore creates a new semaphore with a maximum number of resources.
// If nil is provided, a no-op semaphore with unlimited resources is returned.
func NewSemaphore(size *int) Semaphore {
	if size == nil {
		return nil
	}
	return make(chan struct{}, *size)
}

// Inc attempts to increase the number of resources in use, blocking if none are available
func (s Semaphore) Inc() {
	if s == nil {
		return
	}
	s <- struct{}{}
}

// Dec attempts to decrease the number of resource sin use, blocking if none are in use
func (s Semaphore) Dec() {
	if s == nil {
		return
	}
	<-s
}

// A Node is something that can be executed as a node of a DAG and identified as such
type Node[K comparable] interface {
	// DoDAGTask does this node of the dag, potentially failing
	DoDAGTask() error
	// GetID returns some unique identifier for this node
	GetID() K
	// GetDependencies returns IDs of nodes with edges incoming to this node, that is,
	// they must run and succeed before this node can run
	GetDependencies() Set[K]
}

// A DAG is a directect acylcic graph of nodes which perform work
// and may depend on other nodes to complete successfully first
type DAG[K comparable, E Node[K]] struct {
	Nodes map[K]E
}

// Build builds a dag from a slice of nodes, failing if any return a duplicate ID
func Build[K comparable, E Node[K]](elems []E) (DAG[K, E], error) {
	d := DAG[K, E]{
		Nodes: make(map[K]E, len(elems)),
	}
	for _, elem := range elems {
		id := elem.GetID()
		if _, existing := d.Nodes[id]; existing {
			return DAG[K, E]{}, &DuplicateIDError[K]{Duplicated: id}
		}
		d.Nodes[id] = elem
	}
	return d, nil
}

// BuildFunc builds a dag from a slice of nodes after mapping them with a provided function,
// failing if any mapped nodes return a duplicate ID
func BuildFunc[K comparable, E1 any, E2 Node[K]](elems []E1, f func(*E1) E2) (DAG[K, E2], error) {
	d := DAG[K, E2]{
		Nodes: make(map[K]E2, len(elems)),
	}
	for ix := range elems {
		elem := f(&elems[ix])
		id := elem.GetID()
		if _, existing := d.Nodes[id]; existing {
			return DAG[K, E2]{}, &DuplicateIDError[K]{Duplicated: id}
		}
		d.Nodes[id] = elem
	}
	return d, nil
}

// Len returns the number of nodes in the DAG
func (d *DAG[K, E]) Len() int {
	return len(d.Nodes)
}

// DAGOpts are options that effect how a DAG runs
type Options[K comparable] struct {
	// StartFrom are the IDs of nodes that are the only node that should run, along with their dependendents
	StartFrom Set[K]
	// EndAt are the IDs of nodes whose dependents should not run
	StopAt Set[K]
	// Skip are the IDs of nodes that should not run,
	// but whose dependencies and dependents should run
	Skip Set[K]

	// MaxInFlight is the maximum number of nodes to run concurrently, nil for no limit
	MaxInFlight *int
}

// An Executor can execute DAGs
type Executor[K comparable, E Node[K]] struct {
	// OnStart is called just before a node is executed
	OnStart func(K, E)
	// OnSuccess is called after a node succeeds
	OnSuccess func(K, E)
	// OnFailure is called after a node fails
	OnFailure func(K, E, error)
	// OnComplete is called after node succeeds or fails, after OnSuccess and OnFailure
	OnComplete func(K, E, error)
}

// DuplicateIDError indicates two nodes had the same ID
type DuplicateIDError[K any] struct {
	Duplicated K
}

var _ = error(&DuplicateIDError[string]{})

func (e *DuplicateIDError[K]) Error() string {
	return fmt.Sprintf("id %v was duplicated", e.Duplicated)
}

// UndefinedIDError indicates that a node had an unreferenced ID as a dependency
type UndefinedIDError[K any] struct {
	Undefined K
	Referee   K
}

var _ = error(&UndefinedIDError[string]{})

func (e *UndefinedIDError[K]) Error() string {
	return fmt.Sprintf("Node %v references undefined ID %v", e.Referee, e.Undefined)
}

// RunError collects all of the errors encountered while running a DAG
type RunError[K comparable] struct {
	Errors map[K]error
}

var _ = error(&RunError[string]{})

func (e *RunError[K]) Error() string {
	builder := strings.Builder{}
	first := true
	for k, err := range e.Errors {
		if first {
			first = false
		} else {
			builder.WriteString(", ")
		}
		builder.WriteString(fmt.Sprintf("%v", k))
		builder.WriteString(": ")
		builder.WriteString(err.Error())
	}
	return builder.String()
}

type nodeEvent[K any] struct {
	id  K
	err error
}

var errStopAt = errors.New("StopAt")

// Run executes the nodes of a DAG concurrency according to the provided options
func (e Executor[K, E]) Run(d DAG[K, E], opts Options[K]) error {
	// TODO: Detect cycle
	finished := NewSet[K](d.Len())
	failed := make(map[K]error, d.Len())
	waiting := NewSet[K](d.Len())
	running := NewSet[K](d.Len())
	var sem Semaphore
	sem = NewSemaphore(opts.MaxInFlight)
	if !opts.StartFrom.IsZero() {
		for id := range opts.StartFrom.Elems {
			waiting.Add(id)
		}
	} else {
		for id := range d.Nodes {
			waiting.Add(id)
		}
	}
	for id, node := range d.Nodes {
		for dependency := range node.GetDependencies().Elems {
			if _, ok := d.Nodes[dependency]; !ok {
				return &UndefinedIDError[K]{Referee: id, Undefined: dependency}
			}
		}
	}
	for id := range opts.Skip.Elems {
		finished.Add(id)
		waiting.Remove(id)
	}
	for id := range opts.StopAt.Elems {
		failed[id] = errStopAt
	}
	nodeEvents := make(chan nodeEvent[K])
	executeReadyNodes := func() bool {
		anyStarted := false
	nodes:
		for id := range waiting.Elems {
			node := d.Nodes[id]
			if !opts.Skip.IsZero() && opts.Skip.Contains(id) {
				continue nodes
			}
			if !opts.StopAt.IsZero() && opts.StopAt.Contains(id) {
				continue nodes
			}
			for dependency := range node.GetDependencies().Elems {
				if !finished.Contains(dependency) {
					continue nodes
				}

			}
			waiting.Remove(id)
			running.Add(id)
			if e.OnStart != nil {
				e.OnStart(id, node)
			}
			go func(id K, node E) {
				sem.Inc()
				var err error
				func() {
					defer func() {
						sem.Dec()
						r := recover()
						if r == nil {
							return
						}
						err = r.(error)
						if err == nil {
							err = fmt.Errorf("panic: %v", r)
						}
					}()
					err = node.DoDAGTask()
				}()
				nodeEvents <- nodeEvent[K]{
					id:  id,
					err: err,
				}
			}(id, node)
			anyStarted = true
		}
		return anyStarted
	}

	executeReadyNodes()
	for event := range nodeEvents {
		node := d.Nodes[event.id]
		running.Remove(event.id)
		if event.err == nil {
			finished.Add(event.id)
			if e.OnSuccess != nil {
				e.OnSuccess(event.id, node)
			}
		} else {
			failed[event.id] = event.err
			if e.OnFailure != nil {
				e.OnFailure(event.id, node, event.err)
			}
		}
		if e.OnComplete != nil {
			e.OnComplete(event.id, node, event.err)
		}
		anyStarted := executeReadyNodes()
		// We are done when either:
		// A. All nodes succeed, there are no more nodes running or waiting
		// B. No more nodes can run, all remaining waiting nodes have failed transitive dependencies
		if (!anyStarted && running.Len() == 0) || (running.Len() == 0 && waiting.Len() == 0) {
			close(nodeEvents)
		}
	}
	for id, err := range failed {
		if err == errStopAt {
			delete(failed, id)
		}
	}
	if len(failed) != 0 {
		return &RunError[K]{Errors: failed}
	}

	return nil
}
