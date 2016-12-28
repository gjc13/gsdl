package core

type RowMeta struct {
	FieldMetas     []FieldMeta
	ClusterFieldId uint32
}

func (meta *RowMeta) checkRowSame(row []interface{}, values []FieldValue) bool {
	for _, v := range values {
		if !meta.FieldMetas[v.FieldId].isEqual(row[v.FieldId], v.Value) {
			return false
		}
	}
	return true
}
func (meta *RowMeta) size() int {
	size := meta.nullMapSize()
	for _, v := range meta.FieldMetas {
		size += int(v.FieldWidth)
	}
	return size
}

func (meta *RowMeta) nullMapSize() int {
	return (len(meta.FieldMetas) + 7) / 8
}
