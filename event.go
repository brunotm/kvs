package kvs

const (
	Get     = "get"
	Set     = "set"
	Delete  = "delete"
	Expired = "expired"
)

type Event struct {
	Action string
	Key    string
}
