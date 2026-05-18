package aerospike

// opKind identifies which underlying op a mixedOp carries.
type opKind uint8

const (
	opGet opKind = iota
	opSpend
	opCreate
	opOutpoint
	opIncrement
	opSetDAH
	opSetLocked
)

// mixedOp is a sum-type item queued into the merged ops batcher.
// Exactly one of the pointer fields is non-nil, indicated by kind.
type mixedOp struct {
	kind      opKind
	get       *batchGetItem
	spend     *batchSpend
	create    *BatchStoreItem
	outpoint  *batchOutpoint
	increment *batchIncrement
	setDAH    *batchDAH
	setLocked *batchLocked
}
