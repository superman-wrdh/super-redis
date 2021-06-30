package redis

// Connection represents a connection with redis client
type Connection interface {
	Write([]byte) error
	SetPassword(string)
	GetPassword() string

	// used for `Multi` command
	InMultiState() bool
	SetMultiState(bool)
	GetQueuedCmdLine() [][][]byte
	EnqueueCmd([][]byte)
	ClearQueuedCmds()
	GetWatching() map[string]uint32
}
