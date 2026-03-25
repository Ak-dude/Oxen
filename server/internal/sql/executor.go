package sql

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"oxendb/server/internal/sql/catalog"
	"oxendb/server/internal/sql/codec"
	"oxendb/server/internal/sql/result"
	"oxendb/server/internal/sql/txn"
	"oxendb/server/internal/sql/types"
)

// execPlan dispatches a plan to the appropriate executor method.
// If t is non-nil the plan executes within that transaction; otherwise
// the individual method uses db directly for reads and auto-commits for writes.
func (e *SQLEngine) execPlan(ctx context.Context, plan Plan, t *txn.Txn, args []types.Value) (*result.ResultSet, error) {
	switch p := plan.(type) {
	case *CreateTablePlan:
		return e.execCreateTable(ctx, p)
	case *DropTablePlan:
		return e.execDropTable(ctx, p, t)
	case *CreateIndexPlan:
		return e.execCreateIndex(ctx, p)
	case *DropIndexPlan:
		return e.execDropIndex(ctx, p)
	case *InsertPlan:
		return e.execInsert(ctx, p, t, args)
	case *SelectPlan:
		return e.execSelect(ctx, p, t, args)
	case *UpdatePlan:
		return e.execUpdate(ctx, p, t, args)
	case *DeletePlan:
		return e.execDelete(ctx, p, t, args)
	case *TxnPlan:
		return &result.ResultSet{Tag: strings.ToUpper(p.Kind)}, nil
	case *SetPlan:
		return &result.ResultSet{Tag: "SET"}, nil
	case *ShowPlan:
		return e.execShow(ctx, p)
	case *DeallocatePlan:
		return &result.ResultSet{Tag: "DEALLOCATE"}, nil
	default:
		return nil, fmt.Errorf("executor: unsupported plan type %T", plan)
	}
}

// ---- Storage helpers ----

func (e *SQLEngine) scanRange(start, end []byte, t *txn.Txn) ([][2][]byte, error) {
	if t != nil {
		return t.Scan(start, end)
	}
	return e.db.Scan(start, end)
}

func (e *SQLEngine) getKey(key []byte, t *txn.Txn) ([]byte, error) {
	if t != nil {
		return t.Get(key)
	}
	return e.db.Get(key)
}

func (e *SQLEngine) putKey(key, value []byte, t *txn.Txn) error {
	if t != nil {
		return t.Put(key, value)
	}
	return e.db.Put(key, value)
}

func (e *SQLEngine) deleteKey(key []byte, t *txn.Txn) error {
	if t != nil {
		return t.Delete(key)
	}
	return e.db.Delete(key)
}

// ---- CREATE TABLE ----

func (e *SQLEngine) execCreateTable(_ context.Context, p *CreateTablePlan) (*result.ResultSet, error) {
	err := e.cat.CreateTable(p.Desc)
	if err != nil {
		if p.IfNotExists && errors.Is(err, catalog.ErrTableExists) {
			return &result.ResultSet{Tag: "CREATE TABLE"}, nil
		}
		return nil, err
	}
	return &result.ResultSet{Tag: "CREATE TABLE"}, nil
}

// ---- DROP TABLE ----

func (e *SQLEngine) execDropTable(_ context.Context, p *DropTablePlan, t *txn.Txn) (*result.ResultSet, error) {
	td, err := e.cat.GetTable(p.TableName)
	if err != nil {
		if p.IfExists {
			return &result.ResultSet{Tag: "DROP TABLE"}, nil
		}
		return nil, err
	}

	// Delete all rows
	prefix := codec.DataRowPrefix(td.TableID)
	prefixEnd := codec.DataRowPrefixEnd(td.TableID)
	pairs, err := e.scanRange(prefix, prefixEnd, t)
	if err != nil {
		return nil, err
	}
	for _, pair := range pairs {
		if err := e.deleteKey(pair[0], t); err != nil {
			return nil, err
		}
	}

	if err := e.cat.DropTable(p.TableName); err != nil {
		return nil, err
	}
	return &result.ResultSet{Tag: "DROP TABLE"}, nil
}

// ---- CREATE INDEX ----

func (e *SQLEngine) execCreateIndex(_ context.Context, p *CreateIndexPlan) (*result.ResultSet, error) {
	td, err := e.cat.GetTable(p.Desc.TableName)
	if err != nil {
		return nil, err
	}
	p.Desc.TableID = td.TableID
	p.Desc.IndexID = td.NextIndexID
	td.NextIndexID++
	if err := e.cat.CreateIndex(p.Desc); err != nil {
		return nil, err
	}
	return &result.ResultSet{Tag: "CREATE INDEX"}, nil
}

// ---- DROP INDEX ----

func (e *SQLEngine) execDropIndex(_ context.Context, p *DropIndexPlan) (*result.ResultSet, error) {
	if p.TableName == "" {
		tables, err := e.cat.ListTables()
		if err != nil {
			return nil, err
		}
		for _, td := range tables {
			idx, err := e.cat.GetIndex(td.Name, p.IndexName)
			if err == nil && idx != nil {
				p.TableName = td.Name
				break
			}
		}
	}
	err := e.cat.DropIndex(p.TableName, p.IndexName)
	if err != nil {
		if p.IfExists {
			return &result.ResultSet{Tag: "DROP INDEX"}, nil
		}
		return nil, err
	}
	return &result.ResultSet{Tag: "DROP INDEX"}, nil
}

// ---- INSERT ----

func (e *SQLEngine) execInsert(_ context.Context, p *InsertPlan, t *txn.Txn, args []types.Value) (*result.ResultSet, error) {
	td, err := e.cat.GetTable(p.TableName)
	if err != nil {
		return nil, err
	}

	var lastID int64
	inserted := int64(0)

	for _, rowExprs := range p.Rows {
		rowVals := make([]types.Value, len(td.Columns))
		for i, col := range td.Columns {
			if col.HasDefault && col.DefaultValue != "" && !col.AutoIncrement {
				v, err := types.CoerceValue(col.DefaultValue, col.Type)
				if err == nil {
					rowVals[i] = v
				} else {
					rowVals[i] = types.Null
				}
			} else {
				rowVals[i] = types.Null
			}
		}

		// Map column names to positions
		colPositions := make([]int, len(p.ColNames))
		for i, name := range p.ColNames {
			found := false
			for j, col := range td.Columns {
				if strings.EqualFold(col.Name, name) {
					colPositions[i] = j
					found = true
					break
				}
			}
			if !found {
				return nil, fmt.Errorf("column %q not found in table %q", name, p.TableName)
			}
		}

		// Evaluate and assign values
		if len(p.ColNames) == 0 {
			for i, expr := range rowExprs {
				if i >= len(td.Columns) {
					break
				}
				v, err := evalExpr(expr, nil, nil, args)
				if err != nil {
					return nil, err
				}
				coerced, err := coerceToColumn(v, td.Columns[i])
				if err != nil {
					return nil, err
				}
				rowVals[i] = coerced
			}
		} else {
			for i, expr := range rowExprs {
				if i >= len(colPositions) {
					break
				}
				pos := colPositions[i]
				v, err := evalExpr(expr, nil, nil, args)
				if err != nil {
					return nil, err
				}
				coerced, err := coerceToColumn(v, td.Columns[pos])
				if err != nil {
					return nil, err
				}
				rowVals[pos] = coerced
			}
		}

		// Auto-increment
		for i, col := range td.Columns {
			if col.AutoIncrement && rowVals[i].IsNull {
				seqKey := codec.SequenceKey(td.TableID)
				nextVal, err := e.nextSeqVal(seqKey, t)
				if err != nil {
					return nil, err
				}
				rowVals[i] = types.Value{Type: col.Type, IntVal: nextVal}
				lastID = nextVal
			}
		}

		// NOT NULL check
		for i, col := range td.Columns {
			if !col.Nullable && rowVals[i].IsNull && !col.AutoIncrement {
				return nil, fmt.Errorf("null value violates NOT NULL constraint for column %q", col.Name)
			}
		}

		pkVals, err := e.pkValues(td, rowVals)
		if err != nil {
			return nil, err
		}
		key := codec.DataRowKey(td.TableID, pkVals)
		val, err := codec.EncodeRowValue(td.Columns, rowVals)
		if err != nil {
			return nil, err
		}
		if err := e.putKey(key, val, t); err != nil {
			return nil, err
		}
		inserted++
	}

	return &result.ResultSet{
		Tag:          fmt.Sprintf("INSERT 0 %d", inserted),
		RowsAffected: inserted,
		LastInsertID: lastID,
	}, nil
}

func (e *SQLEngine) nextSeqVal(seqKey []byte, t *txn.Txn) (int64, error) {
	data, err := e.getKey(seqKey, t)
	var cur int64
	if err == nil && len(data) >= 8 {
		cur = int64(binary.BigEndian.Uint64(data))
	}
	cur++
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(cur))
	return cur, e.putKey(seqKey, buf, t)
}

func (e *SQLEngine) pkValues(td *catalog.TableDescriptor, row []types.Value) ([]types.Value, error) {
	if len(td.PKColumns) == 0 {
		if len(row) > 0 {
			return []types.Value{row[0]}, nil
		}
		return nil, fmt.Errorf("table %q has no primary key", td.Name)
	}
	var pkVals []types.Value
	for _, pkColName := range td.PKColumns {
		for i, col := range td.Columns {
			if col.Name == pkColName {
				pkVals = append(pkVals, row[i])
				break
			}
		}
	}
	return pkVals, nil
}

// ---- SELECT ----

func (e *SQLEngine) execSelect(ctx context.Context, p *SelectPlan, t *txn.Txn, args []types.Value) (*result.ResultSet, error) {
	if p.TableName == "" {
		return e.execExprSelect(p, args)
	}

	td, err := e.cat.GetTable(p.TableName)
	if err != nil {
		return nil, err
	}

	prefix := codec.DataRowPrefix(td.TableID)
	prefixEnd := codec.DataRowPrefixEnd(td.TableID)
	pairs, err := e.scanRange(prefix, prefixEnd, t)
	if err != nil {
		return nil, err
	}

	colIdx := buildColIdx(td)

	var rows []result.Row
	for _, pair := range pairs {
		rowVals, err := codec.DecodeRowValue(td.Columns, pair[1])
		if err != nil {
			continue
		}
		if p.Filter != nil {
			match, err := evalBool(p.Filter, rowVals, colIdx, args)
			if err != nil {
				return nil, err
			}
			if !match {
				continue
			}
		}
		rows = append(rows, result.Row(rowVals))
	}

	// JOINs
	currentTD := td
	if len(p.Joins) > 0 {
		rows, colIdx, currentTD, err = e.applyJoins(ctx, p.Joins, rows, colIdx, td, t, args)
		if err != nil {
			return nil, err
		}
	}

	// GROUP BY + aggregates
	if len(p.GroupBy) > 0 || len(p.Aggregates) > 0 {
		return e.execGroupBy(p, rows, colIdx, currentTD, args)
	}

	// ORDER BY
	if len(p.OrderBy) > 0 {
		rows = applySortRows(rows, p.OrderBy, colIdx)
	}

	// OFFSET
	if p.Offset > 0 {
		if int64(len(rows)) <= p.Offset {
			rows = nil
		} else {
			rows = rows[p.Offset:]
		}
	}

	// LIMIT
	if p.Limit >= 0 && int64(len(rows)) > p.Limit {
		rows = rows[:p.Limit]
	}

	cols, projRows, err := e.projectRows(p, rows, colIdx, currentTD, args)
	if err != nil {
		return nil, err
	}

	return &result.ResultSet{
		Columns:      cols,
		Rows:         projRows,
		Tag:          fmt.Sprintf("SELECT %d", len(projRows)),
		RowsAffected: int64(len(projRows)),
	}, nil
}

func (e *SQLEngine) execExprSelect(p *SelectPlan, args []types.Value) (*result.ResultSet, error) {
	var cols []result.ColMeta
	var rowVals result.Row

	for _, ce := range p.ColExprs {
		v, err := evalExpr(ce.Expr, nil, nil, args)
		if err != nil {
			return nil, err
		}
		alias := ce.Alias
		if alias == "" {
			alias = "?column?"
		}
		cols = append(cols, result.ColMeta{
			Name:     alias,
			DataType: v.Type,
			Nullable: true,
		})
		rowVals = append(rowVals, v)
	}

	if len(cols) == 0 {
		return &result.ResultSet{Tag: "SELECT 0"}, nil
	}

	return &result.ResultSet{
		Columns:      cols,
		Rows:         []result.Row{rowVals},
		Tag:          "SELECT 1",
		RowsAffected: 1,
	}, nil
}

func (e *SQLEngine) projectRows(p *SelectPlan, rows []result.Row, colIdx map[string]int, td *catalog.TableDescriptor, args []types.Value) ([]result.ColMeta, []result.Row, error) {
	// SELECT *
	if len(p.Columns) == 0 && len(p.ColExprs) == 0 {
		cols := make([]result.ColMeta, len(td.Columns))
		for i, col := range td.Columns {
			cols[i] = result.ColMeta{
				Name:     col.Name,
				DataType: col.Type,
				Nullable: col.Nullable,
				TableID:  td.TableID,
				ColIdx:   i,
			}
		}
		return cols, rows, nil
	}

	// SELECT with column expressions
	if len(p.ColExprs) > 0 {
		cols := make([]result.ColMeta, len(p.ColExprs))
		projRows := make([]result.Row, len(rows))
		for i, ce := range p.ColExprs {
			alias := ce.Alias
			if alias == "" {
				alias = "?column?"
			}
			cols[i] = result.ColMeta{Name: alias, Nullable: true}
		}
		for ri, row := range rows {
			rowData := make(result.Row, len(p.ColExprs))
			for ci, ce := range p.ColExprs {
				v, err := evalExpr(ce.Expr, []types.Value(row), colIdx, args)
				if err != nil {
					return nil, nil, err
				}
				rowData[ci] = v
				if cols[ci].DataType == types.TypeNull {
					cols[ci].DataType = v.Type
				}
			}
			projRows[ri] = rowData
		}
		return cols, projRows, nil
	}

	// SELECT col1, col2, ...
	projCols := make([]result.ColMeta, 0, len(p.Columns))
	projIdxs := make([]int, 0, len(p.Columns))
	for _, colName := range p.Columns {
		idx, ok := colIdx[colName]
		if !ok {
			for k, v := range colIdx {
				if strings.EqualFold(k, colName) {
					idx = v
					ok = true
					break
				}
			}
		}
		if !ok {
			return nil, nil, fmt.Errorf("column %q not found", colName)
		}
		projIdxs = append(projIdxs, idx)
		if idx < len(td.Columns) {
			col := td.Columns[idx]
			projCols = append(projCols, result.ColMeta{
				Name:     col.Name,
				DataType: col.Type,
				Nullable: col.Nullable,
				TableID:  td.TableID,
				ColIdx:   idx,
			})
		} else {
			projCols = append(projCols, result.ColMeta{Name: colName, Nullable: true})
		}
	}

	projRows := make([]result.Row, len(rows))
	for ri, row := range rows {
		rowData := make(result.Row, len(projIdxs))
		for ci, idx := range projIdxs {
			if idx < len(row) {
				rowData[ci] = row[idx]
			} else {
				rowData[ci] = types.Null
			}
		}
		projRows[ri] = rowData
	}
	return projCols, projRows, nil
}

func (e *SQLEngine) applyJoins(ctx context.Context, joins []Join, leftRows []result.Row, leftColIdx map[string]int, leftTD *catalog.TableDescriptor, t *txn.Txn, args []types.Value) ([]result.Row, map[string]int, *catalog.TableDescriptor, error) {
	currentRows := leftRows
	currentColIdx := leftColIdx
	currentTD := leftTD

	for _, join := range joins {
		rightTD, err := e.cat.GetTable(join.TableName)
		if err != nil {
			return nil, nil, nil, err
		}
		prefix := codec.DataRowPrefix(rightTD.TableID)
		prefixEnd := codec.DataRowPrefixEnd(rightTD.TableID)
		rightPairs, err := e.scanRange(prefix, prefixEnd, t)
		if err != nil {
			return nil, nil, nil, err
		}

		rightRows := make([]result.Row, 0, len(rightPairs))
		for _, pair := range rightPairs {
			rv, err := codec.DecodeRowValue(rightTD.Columns, pair[1])
			if err != nil {
				continue
			}
			rightRows = append(rightRows, result.Row(rv))
		}

		mergedColIdx := make(map[string]int, len(currentColIdx)+len(rightTD.Columns))
		for k, v := range currentColIdx {
			mergedColIdx[k] = v
		}
		offset := len(currentTD.Columns)
		for i, col := range rightTD.Columns {
			mergedColIdx[col.Name] = offset + i
			mergedColIdx[join.TableName+"."+col.Name] = offset + i
		}

		var joinedRows []result.Row
		for _, leftRow := range currentRows {
			matched := false
			for _, rightRow := range rightRows {
				combined := append(append(result.Row{}, leftRow...), rightRow...)
				if join.On != nil {
					match, err := evalBool(join.On, []types.Value(combined), mergedColIdx, args)
					if err != nil {
						continue
					}
					if !match {
						continue
					}
				}
				joinedRows = append(joinedRows, combined)
				matched = true
			}
			if !matched && join.Kind == "left" {
				nullRight := make(result.Row, len(rightTD.Columns))
				for i := range nullRight {
					nullRight[i] = types.Null
				}
				joinedRows = append(joinedRows, append(append(result.Row{}, leftRow...), nullRight...))
			}
		}

		currentRows = joinedRows
		currentColIdx = mergedColIdx
		mergedTD := &catalog.TableDescriptor{
			TableID: currentTD.TableID,
			Name:    currentTD.Name,
			Columns: append(append([]catalog.ColumnDef{}, currentTD.Columns...), rightTD.Columns...),
		}
		currentTD = mergedTD
	}
	return currentRows, currentColIdx, currentTD, nil
}

func (e *SQLEngine) execGroupBy(p *SelectPlan, rows []result.Row, colIdx map[string]int, td *catalog.TableDescriptor, args []types.Value) (*result.ResultSet, error) {
	type group struct {
		keyVals []types.Value
		rows    []result.Row
	}
	groups := make(map[string]*group)
	groupOrder := []string{}

	for _, row := range rows {
		keyParts := make([]string, 0, len(p.GroupBy))
		keyVals := make([]types.Value, 0, len(p.GroupBy))
		for _, colName := range p.GroupBy {
			idx, ok := colIdx[colName]
			v := types.Null
			if ok && idx < len(row) {
				v = row[idx]
			}
			keyParts = append(keyParts, fmt.Sprintf("%v", v.NativeValue()))
			keyVals = append(keyVals, v)
		}
		k := strings.Join(keyParts, "\x00")
		if _, exists := groups[k]; !exists {
			groups[k] = &group{keyVals: keyVals}
			groupOrder = append(groupOrder, k)
		}
		groups[k].rows = append(groups[k].rows, row)
	}

	if len(p.GroupBy) == 0 && len(p.Aggregates) > 0 {
		groups[""] = &group{rows: rows}
		groupOrder = []string{""}
	}

	var cols []result.ColMeta
	for _, colName := range p.GroupBy {
		idx := colIdx[colName]
		var dt types.DataType
		if idx < len(td.Columns) {
			dt = td.Columns[idx].Type
		}
		cols = append(cols, result.ColMeta{Name: colName, DataType: dt})
	}
	for _, agg := range p.Aggregates {
		alias := agg.Alias
		if alias == "" {
			alias = agg.Fn
		}
		dt := types.TypeBigInt
		if agg.Fn == "avg" || agg.Fn == "sum" {
			dt = types.TypeDouble
		}
		cols = append(cols, result.ColMeta{Name: alias, DataType: dt, Nullable: true})
	}

	var resultRows []result.Row
	for _, k := range groupOrder {
		g := groups[k]
		row := make(result.Row, 0, len(p.GroupBy)+len(p.Aggregates))
		row = append(row, g.keyVals...)
		for _, agg := range p.Aggregates {
			v := computeAggregate(agg, g.rows, colIdx)
			row = append(row, v)
		}
		resultRows = append(resultRows, row)
	}

	return &result.ResultSet{
		Columns:      cols,
		Rows:         resultRows,
		Tag:          fmt.Sprintf("SELECT %d", len(resultRows)),
		RowsAffected: int64(len(resultRows)),
	}, nil
}

func computeAggregate(agg AggFunc, rows []result.Row, colIdx map[string]int) types.Value {
	switch strings.ToLower(agg.Fn) {
	case "count":
		if agg.Col == "*" {
			return types.BigIntValue(int64(len(rows)))
		}
		idx, ok := colIdx[agg.Col]
		if !ok {
			return types.BigIntValue(int64(len(rows)))
		}
		count := int64(0)
		for _, row := range rows {
			if idx < len(row) && !row[idx].IsNull {
				count++
			}
		}
		return types.BigIntValue(count)
	case "sum":
		idx, ok := colIdx[agg.Col]
		if !ok {
			return types.Null
		}
		sum := 0.0
		for _, row := range rows {
			if idx < len(row) && !row[idx].IsNull {
				switch row[idx].Type {
				case types.TypeInteger, types.TypeBigInt:
					sum += float64(row[idx].IntVal)
				case types.TypeFloat, types.TypeDouble:
					sum += row[idx].FloatVal
				}
			}
		}
		return types.DoubleValue(sum)
	case "avg":
		idx, ok := colIdx[agg.Col]
		if !ok {
			return types.Null
		}
		sum := 0.0
		count := 0
		for _, row := range rows {
			if idx < len(row) && !row[idx].IsNull {
				switch row[idx].Type {
				case types.TypeInteger, types.TypeBigInt:
					sum += float64(row[idx].IntVal)
					count++
				case types.TypeFloat, types.TypeDouble:
					sum += row[idx].FloatVal
					count++
				}
			}
		}
		if count == 0 {
			return types.Null
		}
		return types.DoubleValue(sum / float64(count))
	case "min":
		idx, ok := colIdx[agg.Col]
		if !ok {
			return types.Null
		}
		var minVal *types.Value
		for _, row := range rows {
			if idx < len(row) && !row[idx].IsNull {
				v := row[idx]
				if minVal == nil {
					minVal = &v
				} else {
					cmp, _ := types.CompareValues(*minVal, v)
					if cmp > 0 {
						minVal = &v
					}
				}
			}
		}
		if minVal == nil {
			return types.Null
		}
		return *minVal
	case "max":
		idx, ok := colIdx[agg.Col]
		if !ok {
			return types.Null
		}
		var maxVal *types.Value
		for _, row := range rows {
			if idx < len(row) && !row[idx].IsNull {
				v := row[idx]
				if maxVal == nil {
					maxVal = &v
				} else {
					cmp, _ := types.CompareValues(*maxVal, v)
					if cmp < 0 {
						maxVal = &v
					}
				}
			}
		}
		if maxVal == nil {
			return types.Null
		}
		return *maxVal
	}
	return types.Null
}

// ---- UPDATE ----

func (e *SQLEngine) execUpdate(_ context.Context, p *UpdatePlan, t *txn.Txn, args []types.Value) (*result.ResultSet, error) {
	td, err := e.cat.GetTable(p.TableName)
	if err != nil {
		return nil, err
	}

	prefix := codec.DataRowPrefix(td.TableID)
	prefixEnd := codec.DataRowPrefixEnd(td.TableID)
	pairs, err := e.scanRange(prefix, prefixEnd, t)
	if err != nil {
		return nil, err
	}

	colIdx := buildColIdx(td)
	updated := int64(0)

	for _, pair := range pairs {
		rowVals, err := codec.DecodeRowValue(td.Columns, pair[1])
		if err != nil {
			continue
		}
		if p.Filter != nil {
			match, err := evalBool(p.Filter, rowVals, colIdx, args)
			if err != nil {
				return nil, err
			}
			if !match {
				continue
			}
		}

		newVals := append([]types.Value{}, rowVals...)
		for _, assign := range p.Assignments {
			idx, ok := colIdx[assign.Col]
			if !ok {
				return nil, fmt.Errorf("column %q not found in table %q", assign.Col, p.TableName)
			}
			v, err := evalExpr(assign.Expr, rowVals, colIdx, args)
			if err != nil {
				return nil, err
			}
			coerced, err := coerceToColumn(v, td.Columns[idx])
			if err != nil {
				return nil, err
			}
			newVals[idx] = coerced
		}

		oldPK, _ := e.pkValues(td, rowVals)
		newPK, _ := e.pkValues(td, newVals)
		oldKey := codec.DataRowKey(td.TableID, oldPK)
		newKey := codec.DataRowKey(td.TableID, newPK)

		if string(oldKey) != string(newKey) {
			if err := e.deleteKey(oldKey, t); err != nil {
				return nil, err
			}
		}

		encoded, err := codec.EncodeRowValue(td.Columns, newVals)
		if err != nil {
			return nil, err
		}
		if err := e.putKey(newKey, encoded, t); err != nil {
			return nil, err
		}
		updated++
	}

	return &result.ResultSet{
		Tag:          fmt.Sprintf("UPDATE %d", updated),
		RowsAffected: updated,
	}, nil
}

// ---- DELETE ----

func (e *SQLEngine) execDelete(_ context.Context, p *DeletePlan, t *txn.Txn, args []types.Value) (*result.ResultSet, error) {
	td, err := e.cat.GetTable(p.TableName)
	if err != nil {
		return nil, err
	}

	prefix := codec.DataRowPrefix(td.TableID)
	prefixEnd := codec.DataRowPrefixEnd(td.TableID)
	pairs, err := e.scanRange(prefix, prefixEnd, t)
	if err != nil {
		return nil, err
	}

	colIdx := buildColIdx(td)
	deleted := int64(0)

	for _, pair := range pairs {
		rowVals, err := codec.DecodeRowValue(td.Columns, pair[1])
		if err != nil {
			continue
		}
		if p.Filter != nil {
			match, err := evalBool(p.Filter, rowVals, colIdx, args)
			if err != nil {
				return nil, err
			}
			if !match {
				continue
			}
		}
		if err := e.deleteKey(pair[0], t); err != nil {
			return nil, err
		}
		deleted++
	}

	return &result.ResultSet{
		Tag:          fmt.Sprintf("DELETE %d", deleted),
		RowsAffected: deleted,
	}, nil
}

// ---- SHOW ----

func (e *SQLEngine) execShow(_ context.Context, p *ShowPlan) (*result.ResultSet, error) {
	var val string
	switch strings.ToLower(p.Name) {
	case "server_version":
		val = "14.0"
	case "client_encoding":
		val = "UTF8"
	case "datestyle", "date_style":
		val = "ISO, MDY"
	case "timezone":
		val = "UTC"
	case "integer_datetimes":
		val = "on"
	case "standard_conforming_strings":
		val = "on"
	case "transaction_isolation":
		val = "read committed"
	case "max_connections":
		val = "100"
	case "search_path":
		val = "\"$user\", public"
	default:
		val = ""
	}
	return &result.ResultSet{
		Columns: []result.ColMeta{{Name: p.Name, DataType: types.TypeText}},
		Rows:    []result.Row{{types.TextValue(val)}},
		Tag:     "SHOW",
	}, nil
}

// ---- Expression evaluator ----

func evalExpr(expr Expr, row []types.Value, colIdx map[string]int, args []types.Value) (types.Value, error) {
	if expr == nil {
		return types.Null, nil
	}
	switch e := expr.(type) {
	case *LiteralExpr:
		return e.Val, nil

	case *ColRefExpr:
		if colIdx == nil {
			return types.Null, nil
		}
		key := e.Col
		if e.Table != "" {
			if idx, ok := colIdx[e.Table+"."+e.Col]; ok {
				if idx < len(row) {
					return row[idx], nil
				}
			}
		}
		idx, ok := colIdx[key]
		if !ok {
			for k, v := range colIdx {
				if strings.EqualFold(k, key) {
					idx = v
					ok = true
					break
				}
			}
		}
		if !ok {
			return types.Null, nil
		}
		if idx < len(row) {
			return row[idx], nil
		}
		return types.Null, nil

	case *ParamExpr:
		if args != nil && e.Idx >= 1 && e.Idx <= len(args) {
			return args[e.Idx-1], nil
		}
		return types.Null, nil

	case *BinOpExpr:
		return evalBinOp(e, row, colIdx, args)

	case *UnaryExpr:
		v, err := evalExpr(e.E, row, colIdx, args)
		if err != nil {
			return types.Null, err
		}
		switch strings.ToUpper(e.Op) {
		case "NOT":
			if v.IsNull {
				return types.Null, nil
			}
			return types.BoolValue(!v.BoolVal), nil
		case "-":
			switch v.Type {
			case types.TypeInteger, types.TypeBigInt:
				return types.Value{Type: v.Type, IntVal: -v.IntVal}, nil
			case types.TypeFloat, types.TypeDouble:
				return types.Value{Type: v.Type, FloatVal: -v.FloatVal}, nil
			}
		}
		return v, nil

	case *LikeExpr:
		val, err := evalExpr(e.E, row, colIdx, args)
		if err != nil {
			return types.Null, err
		}
		pattern, err := evalExpr(e.Pattern, row, colIdx, args)
		if err != nil {
			return types.Null, err
		}
		if val.IsNull || pattern.IsNull {
			return types.Null, nil
		}
		matched := types.LikeMatch(val.StrVal, pattern.StrVal)
		if e.Negate {
			matched = !matched
		}
		return types.BoolValue(matched), nil

	case *InExpr:
		val, err := evalExpr(e.E, row, colIdx, args)
		if err != nil {
			return types.Null, err
		}
		if val.IsNull {
			return types.Null, nil
		}
		for _, itemExpr := range e.List {
			item, err := evalExpr(itemExpr, row, colIdx, args)
			if err != nil {
				return types.Null, err
			}
			if types.EqualValues(val, item) {
				if e.Negate {
					return types.BoolValue(false), nil
				}
				return types.BoolValue(true), nil
			}
		}
		if e.Negate {
			return types.BoolValue(true), nil
		}
		return types.BoolValue(false), nil

	case *IsNullExpr:
		val, err := evalExpr(e.E, row, colIdx, args)
		if err != nil {
			return types.Null, err
		}
		if e.IsNull {
			return types.BoolValue(val.IsNull), nil
		}
		return types.BoolValue(!val.IsNull), nil

	case *FuncExpr:
		return evalFunc(e, row, colIdx, args)

	case *CastExpr:
		val, err := evalExpr(e.E, row, colIdx, args)
		if err != nil {
			return types.Null, err
		}
		if val.IsNull {
			return types.Null, nil
		}
		return types.CoerceValue(val.NativeValue(), e.To)

	case *StarExpr:
		return types.IntValue(1), nil

	case *BetweenExpr:
		val, err := evalExpr(e.E, row, colIdx, args)
		if err != nil {
			return types.Null, err
		}
		low, err := evalExpr(e.Low, row, colIdx, args)
		if err != nil {
			return types.Null, err
		}
		high, err := evalExpr(e.High, row, colIdx, args)
		if err != nil {
			return types.Null, err
		}
		if val.IsNull || low.IsNull || high.IsNull {
			return types.Null, nil
		}
		cmpLow, _ := types.CompareValues(val, low)
		cmpHigh, _ := types.CompareValues(val, high)
		inRange := cmpLow >= 0 && cmpHigh <= 0
		if e.Negate {
			inRange = !inRange
		}
		return types.BoolValue(inRange), nil
	}

	return types.Null, fmt.Errorf("unsupported expression type %T", expr)
}

func evalBool(expr Expr, row []types.Value, colIdx map[string]int, args []types.Value) (bool, error) {
	v, err := evalExpr(expr, row, colIdx, args)
	if err != nil {
		return false, err
	}
	if v.IsNull {
		return false, nil
	}
	return v.BoolVal, nil
}

func evalBinOp(e *BinOpExpr, row []types.Value, colIdx map[string]int, args []types.Value) (types.Value, error) {
	op := strings.ToUpper(e.Op)

	switch op {
	case "AND":
		left, err := evalBool(e.Left, row, colIdx, args)
		if err != nil {
			return types.Null, err
		}
		if !left {
			return types.BoolValue(false), nil
		}
		right, err := evalBool(e.Right, row, colIdx, args)
		if err != nil {
			return types.Null, err
		}
		return types.BoolValue(right), nil
	case "OR":
		left, err := evalBool(e.Left, row, colIdx, args)
		if err != nil {
			return types.Null, err
		}
		if left {
			return types.BoolValue(true), nil
		}
		right, err := evalBool(e.Right, row, colIdx, args)
		if err != nil {
			return types.Null, err
		}
		return types.BoolValue(right), nil
	}

	left, err := evalExpr(e.Left, row, colIdx, args)
	if err != nil {
		return types.Null, err
	}
	right, err := evalExpr(e.Right, row, colIdx, args)
	if err != nil {
		return types.Null, err
	}

	switch op {
	case "+", "-", "*", "/", "%":
		return evalArith(op, left, right)
	case "||":
		return types.TextValue(valueToString(left) + valueToString(right)), nil
	}

	if left.IsNull || right.IsNull {
		return types.BoolValue(false), nil
	}

	// Type coercion for comparison
	if left.Type != right.Type {
		coerced, err := types.CoerceValue(right.NativeValue(), left.Type)
		if err == nil {
			right = coerced
		}
	}

	cmp, err := types.CompareValues(left, right)
	if err != nil {
		ls := valueToString(left)
		rs := valueToString(right)
		if ls < rs {
			cmp = -1
		} else if ls > rs {
			cmp = 1
		} else {
			cmp = 0
		}
	}

	switch op {
	case "=":
		return types.BoolValue(cmp == 0), nil
	case "!=", "<>":
		return types.BoolValue(cmp != 0), nil
	case "<":
		return types.BoolValue(cmp < 0), nil
	case "<=":
		return types.BoolValue(cmp <= 0), nil
	case ">":
		return types.BoolValue(cmp > 0), nil
	case ">=":
		return types.BoolValue(cmp >= 0), nil
	}

	return types.Null, fmt.Errorf("unsupported operator %q", e.Op)
}

func evalArith(op string, left, right types.Value) (types.Value, error) {
	if left.IsNull || right.IsNull {
		return types.Null, nil
	}
	if left.Type == types.TypeFloat || left.Type == types.TypeDouble ||
		right.Type == types.TypeFloat || right.Type == types.TypeDouble {
		lf := toFloat(left)
		rf := toFloat(right)
		dt := left.Type
		if right.Type == types.TypeDouble || right.Type == types.TypeFloat {
			dt = right.Type
		}
		switch op {
		case "+":
			return types.Value{Type: dt, FloatVal: lf + rf}, nil
		case "-":
			return types.Value{Type: dt, FloatVal: lf - rf}, nil
		case "*":
			return types.Value{Type: dt, FloatVal: lf * rf}, nil
		case "/":
			if rf == 0 {
				return types.Null, fmt.Errorf("division by zero")
			}
			return types.Value{Type: dt, FloatVal: lf / rf}, nil
		}
	}
	li := toInt(left)
	ri := toInt(right)
	dt := left.Type
	switch op {
	case "+":
		return types.Value{Type: dt, IntVal: li + ri}, nil
	case "-":
		return types.Value{Type: dt, IntVal: li - ri}, nil
	case "*":
		return types.Value{Type: dt, IntVal: li * ri}, nil
	case "/":
		if ri == 0 {
			return types.Null, fmt.Errorf("division by zero")
		}
		return types.Value{Type: dt, IntVal: li / ri}, nil
	case "%":
		if ri == 0 {
			return types.Null, fmt.Errorf("division by zero")
		}
		return types.Value{Type: dt, IntVal: li % ri}, nil
	}
	return types.Null, fmt.Errorf("unknown arithmetic operator %q", op)
}

func evalFunc(e *FuncExpr, row []types.Value, colIdx map[string]int, args []types.Value) (types.Value, error) {
	name := strings.ToLower(e.Name)

	switch name {
	case "now", "current_timestamp", "pg_catalog.now":
		return types.TimeValue(time.Now().UTC()), nil
	case "current_date":
		now := time.Now().UTC()
		return types.TimeValue(time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)), nil
	case "version":
		return types.TextValue("OxenDB 0.1.0 (PostgreSQL 14.0 compatible)"), nil
	case "current_database":
		return types.TextValue("oxendb"), nil
	case "current_schema", "current_schemas", "pg_catalog.current_schema":
		return types.TextValue("public"), nil
	case "current_user", "user", "session_user":
		return types.TextValue("oxen"), nil
	case "pg_backend_pid":
		return types.IntValue(1), nil
	case "pg_postmaster_start_time":
		return types.TimeValue(time.Now().UTC()), nil
	case "inet_server_addr":
		return types.TextValue("127.0.0.1"), nil
	case "inet_server_port":
		return types.IntValue(5432), nil

	case "lower":
		if len(e.Args) > 0 {
			v, err := evalExpr(e.Args[0], row, colIdx, args)
			if err != nil {
				return types.Null, err
			}
			if v.IsNull {
				return types.Null, nil
			}
			return types.TextValue(strings.ToLower(v.StrVal)), nil
		}
	case "upper":
		if len(e.Args) > 0 {
			v, err := evalExpr(e.Args[0], row, colIdx, args)
			if err != nil {
				return types.Null, err
			}
			if v.IsNull {
				return types.Null, nil
			}
			return types.TextValue(strings.ToUpper(v.StrVal)), nil
		}
	case "length", "char_length", "character_length":
		if len(e.Args) > 0 {
			v, err := evalExpr(e.Args[0], row, colIdx, args)
			if err != nil {
				return types.Null, err
			}
			if v.IsNull {
				return types.Null, nil
			}
			return types.IntValue(int64(len([]rune(v.StrVal)))), nil
		}
	case "substr", "substring":
		if len(e.Args) >= 2 {
			strVal, _ := evalExpr(e.Args[0], row, colIdx, args)
			startVal, _ := evalExpr(e.Args[1], row, colIdx, args)
			if strVal.IsNull {
				return types.Null, nil
			}
			runes := []rune(strVal.StrVal)
			start := int(startVal.IntVal) - 1
			if start < 0 {
				start = 0
			}
			if start >= len(runes) {
				return types.TextValue(""), nil
			}
			if len(e.Args) >= 3 {
				lenVal, _ := evalExpr(e.Args[2], row, colIdx, args)
				end := start + int(lenVal.IntVal)
				if end > len(runes) {
					end = len(runes)
				}
				return types.TextValue(string(runes[start:end])), nil
			}
			return types.TextValue(string(runes[start:])), nil
		}
	case "trim":
		if len(e.Args) > 0 {
			v, err := evalExpr(e.Args[0], row, colIdx, args)
			if err != nil {
				return types.Null, err
			}
			if v.IsNull {
				return types.Null, nil
			}
			return types.TextValue(strings.TrimSpace(v.StrVal)), nil
		}
	case "concat":
		var sb strings.Builder
		for _, arg := range e.Args {
			v, _ := evalExpr(arg, row, colIdx, args)
			if !v.IsNull {
				sb.WriteString(valueToString(v))
			}
		}
		return types.TextValue(sb.String()), nil
	case "coalesce":
		for _, arg := range e.Args {
			v, err := evalExpr(arg, row, colIdx, args)
			if err != nil {
				return types.Null, err
			}
			if !v.IsNull {
				return v, nil
			}
		}
		return types.Null, nil
	case "nullif":
		if len(e.Args) == 2 {
			a, _ := evalExpr(e.Args[0], row, colIdx, args)
			b, _ := evalExpr(e.Args[1], row, colIdx, args)
			if types.EqualValues(a, b) {
				return types.Null, nil
			}
			return a, nil
		}
	case "abs":
		if len(e.Args) > 0 {
			v, _ := evalExpr(e.Args[0], row, colIdx, args)
			if v.IsNull {
				return types.Null, nil
			}
			switch v.Type {
			case types.TypeInteger, types.TypeBigInt:
				if v.IntVal < 0 {
					return types.Value{Type: v.Type, IntVal: -v.IntVal}, nil
				}
				return v, nil
			case types.TypeFloat, types.TypeDouble:
				if v.FloatVal < 0 {
					return types.Value{Type: v.Type, FloatVal: -v.FloatVal}, nil
				}
				return v, nil
			}
		}
	case "to_char":
		if len(e.Args) >= 1 {
			v, _ := evalExpr(e.Args[0], row, colIdx, args)
			if v.IsNull {
				return types.Null, nil
			}
			if v.Type == types.TypeTimestamp {
				return types.TextValue(v.TimeVal.UTC().Format("2006-01-02 15:04:05")), nil
			}
			return types.TextValue(valueToString(v)), nil
		}
	case "case":
		n := len(e.Args)
		for i := 0; i+1 < n; i += 2 {
			if i+1 >= n-1 {
				break
			}
			cond, err := evalBool(e.Args[i], row, colIdx, args)
			if err != nil {
				return types.Null, err
			}
			if cond {
				return evalExpr(e.Args[i+1], row, colIdx, args)
			}
		}
		if n%2 == 1 {
			return evalExpr(e.Args[n-1], row, colIdx, args)
		}
		return types.Null, nil
	case "pg_catalog.pg_get_userbyid", "pg_get_userbyid":
		return types.TextValue("oxen"), nil
	case "format_type", "pg_catalog.format_type":
		if len(e.Args) > 0 {
			v, _ := evalExpr(e.Args[0], row, colIdx, args)
			if v.IsNull {
				return types.TextValue("-"), nil
			}
			return types.TextValue(oidToTypeName(int(v.IntVal))), nil
		}
	case "current_setting", "pg_catalog.current_setting":
		return types.TextValue(""), nil
	case "set_config":
		return types.TextValue(""), nil
	case "array_to_string":
		return types.TextValue(""), nil
	case "string_agg":
		return types.TextValue(""), nil
	case "count":
		return types.IntValue(0), nil
	}

	return types.Null, nil
}

func oidToTypeName(oid int) string {
	switch oid {
	case 16:
		return "boolean"
	case 20:
		return "bigint"
	case 21:
		return "smallint"
	case 23:
		return "integer"
	case 25:
		return "text"
	case 700:
		return "real"
	case 701:
		return "double precision"
	case 1043:
		return "character varying"
	case 1184:
		return "timestamp with time zone"
	default:
		return "text"
	}
}

// ---- Shared helpers ----

func buildColIdx(td *catalog.TableDescriptor) map[string]int {
	idx := make(map[string]int, len(td.Columns)*2)
	for i, col := range td.Columns {
		idx[col.Name] = i
		idx[strings.ToLower(col.Name)] = i
		idx[td.Name+"."+col.Name] = i
	}
	return idx
}

func applySortRows(rows []result.Row, orderBy []OrderByItem, colIdx map[string]int) []result.Row {
	sorted := append([]result.Row{}, rows...)
	sort.SliceStable(sorted, func(i, j int) bool {
		for _, ob := range orderBy {
			idx := -1
			if ob.Col != "" {
				if v, ok := colIdx[ob.Col]; ok {
					idx = v
				} else {
					for k, v2 := range colIdx {
						if strings.EqualFold(k, ob.Col) {
							idx = v2
							break
						}
					}
				}
			}
			var a, b types.Value
			if idx >= 0 && idx < len(sorted[i]) {
				a = sorted[i][idx]
			} else {
				a = types.Null
			}
			if idx >= 0 && idx < len(sorted[j]) {
				b = sorted[j][idx]
			} else {
				b = types.Null
			}
			cmp, _ := types.CompareValues(a, b)
			if cmp == 0 {
				continue
			}
			if ob.Desc {
				return cmp > 0
			}
			return cmp < 0
		}
		return false
	})
	return sorted
}

func coerceToColumn(v types.Value, col catalog.ColumnDef) (types.Value, error) {
	if v.IsNull {
		return types.Null, nil
	}
	if v.Type == col.Type {
		return v, nil
	}
	return types.CoerceValue(v.NativeValue(), col.Type)
}

func valueToString(v types.Value) string {
	if v.IsNull {
		return ""
	}
	switch v.Type {
	case types.TypeInteger, types.TypeBigInt:
		return strconv.FormatInt(v.IntVal, 10)
	case types.TypeFloat, types.TypeDouble:
		return strconv.FormatFloat(v.FloatVal, 'f', -1, 64)
	case types.TypeText, types.TypeVarChar:
		return v.StrVal
	case types.TypeBoolean:
		if v.BoolVal {
			return "true"
		}
		return "false"
	case types.TypeTimestamp:
		return v.TimeVal.UTC().Format("2006-01-02 15:04:05")
	}
	return ""
}

func toFloat(v types.Value) float64 {
	switch v.Type {
	case types.TypeInteger, types.TypeBigInt:
		return float64(v.IntVal)
	case types.TypeFloat, types.TypeDouble:
		return v.FloatVal
	}
	return 0
}

func toInt(v types.Value) int64 {
	switch v.Type {
	case types.TypeInteger, types.TypeBigInt:
		return v.IntVal
	case types.TypeFloat, types.TypeDouble:
		return int64(v.FloatVal)
	}
	return 0
}
