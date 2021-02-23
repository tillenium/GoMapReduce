package mr

type KeyValue struct {
	Key string
	Value string
}

type SortKey []KeyValue

func (a SortKey) Len() int {
	return len(a)
}

func (a SortKey) Less(i, j int) bool {
	return a[i].Key < a[j].Key
}

func (a SortKey) Swap(i, j int) {
	a[i], a[j] = a[j], a[i]
}