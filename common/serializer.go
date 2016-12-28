package common

type Serializer interface {
	SerializeSize() int
	Serialize() []byte
}
