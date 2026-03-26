package catalog

import (
	"encoding/json"
	"oxendb/server/internal/sql/types"
)

type ColumnDef struct {
	Ordinal       int            `json:"ordinal"`
	Name          string         `json:"name"`
	Type          types.DataType `json:"type"`
	VarCharLen    int            `json:"varchar_len,omitempty"`
	Nullable      bool           `json:"nullable"`
	HasDefault    bool           `json:"has_default"`
	DefaultValue  string         `json:"default_value,omitempty"`
	IsPrimaryKey  bool           `json:"is_primary_key"`
	IsUnique      bool           `json:"is_unique"`
	AutoIncrement bool           `json:"auto_increment"`
}

type TableDescriptor struct {
	TableID     uint32      `json:"table_id"`
	Name        string      `json:"name"`
	Columns     []ColumnDef `json:"columns"`
	PKColumns   []string    `json:"pk_columns"`
	NextIndexID uint32      `json:"next_index_id"`
	CreatedAt   int64       `json:"created_at"`
}

type IndexDescriptor struct {
	IndexID   uint32   `json:"index_id"`
	TableID   uint32   `json:"table_id"`
	TableName string   `json:"table_name"`
	Name      string   `json:"name"`
	Columns   []string `json:"columns"`
	IsUnique  bool     `json:"is_unique"`
}

func (td *TableDescriptor) Column(name string) *ColumnDef {
	for i := range td.Columns {
		if td.Columns[i].Name == name {
			return &td.Columns[i]
		}
	}
	return nil
}

func (td *TableDescriptor) PKColumn() *ColumnDef {
	for i := range td.Columns {
		if td.Columns[i].IsPrimaryKey {
			return &td.Columns[i]
		}
	}
	return nil
}

func (td *TableDescriptor) Marshal() ([]byte, error)   { return json.Marshal(td) }
func (td *TableDescriptor) Unmarshal(b []byte) error   { return json.Unmarshal(b, td) }
func (id *IndexDescriptor) Marshal() ([]byte, error)   { return json.Marshal(id) }
func (id *IndexDescriptor) Unmarshal(b []byte) error   { return json.Unmarshal(b, id) }
