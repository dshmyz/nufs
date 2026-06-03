package s3fs

// Operation is the base type for async S3 operations.
type Operation struct {
	Error chan error
}

// MoveOperation copies source to target then deletes source.
type MoveOperation struct {
	*Operation
	Source string
	Target string
}

// CopyOperation copies source to target.
type CopyOperation struct {
	*Operation
	Source string
	Target string
}

// PutOperation uploads a local file to S3.
type PutOperation struct {
	*Operation
	Source string
	Target string
	Length int64
}

func newMoveOp(source, target string) *MoveOperation {
	return &MoveOperation{
		Source:    source,
		Target:    target,
		Operation: &Operation{Error: make(chan error, 1)},
	}
}

func newCopyOp(source, target string) *CopyOperation {
	return &CopyOperation{
		Source:    source,
		Target:    target,
		Operation: &Operation{Error: make(chan error, 1)},
	}
}

func newPutOp(source, target string, length int64) *PutOperation {
	return &PutOperation{
		Source:    source,
		Target:    target,
		Length:    length,
		Operation: &Operation{Error: make(chan error, 1)},
	}
}
