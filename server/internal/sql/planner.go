package sql

import (
	"fmt"
	"strconv"
	"strings"

	pg_query "github.com/pganalyze/pg_query_go/v5"

	"oxendb/server/internal/sql/catalog"
	"oxendb/server/internal/sql/types"
)

// ---- Plan interface and plan types ----

// Plan is a node in the logical query plan tree.
type Plan interface {
	planNode()
}

// SelectPlan represents a SELECT statement.
type SelectPlan struct {
	TableName  string
	Columns    []string // nil = SELECT *
	Filter     Expr
	OrderBy    []OrderByItem
	Limit      int64 // -1 = no limit
	Offset     int64
	GroupBy    []string
	Aggregates []AggFunc
	Joins      []Join
	// For expressions in column list (e.g., SELECT 1, SELECT version())
	ColExprs []ColExpr
}

// ColExpr represents an expression in the SELECT column list with its alias.
type ColExpr struct {
	Expr  Expr
	Alias string
}

// OrderByItem describes one ORDER BY element.
type OrderByItem struct {
	Col  string
	Desc bool
	Expr Expr
}

// AggFunc describes an aggregate function application.
type AggFunc struct {
	Fn      string // "count", "sum", "avg", "min", "max"
	Col     string // column name, or "*" for count(*)
	Alias   string
	Distinct bool
}

// Join describes a JOIN clause.
type Join struct {
	TableName string
	Alias     string
	On        Expr
	Kind      string // "inner", "left", "right", "cross"
}

// InsertPlan represents an INSERT statement.
type InsertPlan struct {
	TableName string
	ColNames  []string
	Rows      [][]Expr
}

// UpdatePlan represents an UPDATE statement.
type UpdatePlan struct {
	TableName   string
	Assignments []Assignment
	Filter      Expr
}

// Assignment is a single col = expr in UPDATE SET.
type Assignment struct {
	Col  string
	Expr Expr
}

// DeletePlan represents a DELETE statement.
type DeletePlan struct {
	TableName string
	Filter    Expr
}

// CreateTablePlan represents a CREATE TABLE statement.
type CreateTablePlan struct {
	Desc     *catalog.TableDescriptor
	IfNotExists bool
}

// DropTablePlan represents a DROP TABLE statement.
type DropTablePlan struct {
	TableName string
	IfExists  bool
}

// CreateIndexPlan represents a CREATE INDEX statement.
type CreateIndexPlan struct {
	Desc *catalog.IndexDescriptor
}

// DropIndexPlan represents a DROP INDEX statement.
type DropIndexPlan struct {
	TableName string
	IndexName string
	IfExists  bool
}

// TxnPlan represents BEGIN/COMMIT/ROLLBACK.
type TxnPlan struct {
	Kind string // "begin", "commit", "rollback"
}

// SetPlan represents a SET variable statement.
type SetPlan struct {
	Name  string
	Value string
}

// ShowPlan represents a SHOW variable statement.
type ShowPlan struct {
	Name string
}

// DeallocatePlan represents DEALLOCATE statement.
type DeallocatePlan struct {
	Name string
}

func (p *SelectPlan) planNode()      {}
func (p *InsertPlan) planNode()      {}
func (p *UpdatePlan) planNode()      {}
func (p *DeletePlan) planNode()      {}
func (p *CreateTablePlan) planNode() {}
func (p *DropTablePlan) planNode()   {}
func (p *CreateIndexPlan) planNode() {}
func (p *DropIndexPlan) planNode()   {}
func (p *TxnPlan) planNode()         {}
func (p *SetPlan) planNode()         {}
func (p *ShowPlan) planNode()        {}
func (p *DeallocatePlan) planNode()  {}

// ---- Expression types ----

// Expr is a scalar expression node.
type Expr interface {
	exprNode()
}

// ColRefExpr references a column, optionally qualified with a table name.
type ColRefExpr struct {
	Table string
	Col   string
}

// LiteralExpr is a literal constant value.
type LiteralExpr struct {
	Val types.Value
}

// ParamExpr references a query parameter ($1, $2, ...).
type ParamExpr struct {
	Idx int // 1-based
}

// BinOpExpr is a binary operation.
type BinOpExpr struct {
	Op    string
	Left  Expr
	Right Expr
}

// UnaryExpr is a unary operation.
type UnaryExpr struct {
	Op string
	E  Expr
}

// LikeExpr is a LIKE or NOT LIKE predicate.
type LikeExpr struct {
	E       Expr
	Pattern Expr
	Negate  bool
}

// InExpr is an IN or NOT IN predicate.
type InExpr struct {
	E      Expr
	List   []Expr
	Negate bool
}

// IsNullExpr checks for NULL.
type IsNullExpr struct {
	E      Expr
	IsNull bool // true = IS NULL, false = IS NOT NULL
}

// FuncExpr is a function call.
type FuncExpr struct {
	Name string
	Args []Expr
}

// CastExpr is an explicit type cast.
type CastExpr struct {
	E  Expr
	To types.DataType
}

// StarExpr represents * in a SELECT list.
type StarExpr struct{}

// BetweenExpr is a BETWEEN predicate.
type BetweenExpr struct {
	E      Expr
	Low    Expr
	High   Expr
	Negate bool
}

func (e *ColRefExpr) exprNode()   {}
func (e *LiteralExpr) exprNode()  {}
func (e *ParamExpr) exprNode()    {}
func (e *BinOpExpr) exprNode()    {}
func (e *UnaryExpr) exprNode()    {}
func (e *LikeExpr) exprNode()     {}
func (e *InExpr) exprNode()       {}
func (e *IsNullExpr) exprNode()   {}
func (e *FuncExpr) exprNode()     {}
func (e *CastExpr) exprNode()     {}
func (e *StarExpr) exprNode()     {}
func (e *BetweenExpr) exprNode()  {}

// ---- Planner ----

// Planner parses SQL strings and builds logical plans.
type Planner struct{}

// Parse parses a SQL string and returns a slice of Plans (one per statement).
func (p *Planner) Parse(sql string) ([]Plan, error) {
	result, err := pg_query.Parse(sql)
	if err != nil {
		return nil, fmt.Errorf("parse error: %w", err)
	}

	plans := make([]Plan, 0, len(result.Stmts))
	for _, rawStmt := range result.Stmts {
		if rawStmt.Stmt == nil {
			continue
		}
		plan, err := p.buildPlan(rawStmt.Stmt)
		if err != nil {
			return nil, err
		}
		if plan != nil {
			plans = append(plans, plan)
		}
	}
	return plans, nil
}

func (p *Planner) buildPlan(node *pg_query.Node) (Plan, error) {
	switch n := node.Node.(type) {
	case *pg_query.Node_SelectStmt:
		return p.buildSelect(n.SelectStmt)
	case *pg_query.Node_InsertStmt:
		return p.buildInsert(n.InsertStmt)
	case *pg_query.Node_UpdateStmt:
		return p.buildUpdate(n.UpdateStmt)
	case *pg_query.Node_DeleteStmt:
		return p.buildDelete(n.DeleteStmt)
	case *pg_query.Node_CreateStmt:
		return p.buildCreateTable(n.CreateStmt)
	case *pg_query.Node_DropStmt:
		return p.buildDrop(n.DropStmt)
	case *pg_query.Node_IndexStmt:
		return p.buildCreateIndex(n.IndexStmt)
	case *pg_query.Node_TransactionStmt:
		return p.buildTransaction(n.TransactionStmt)
	case *pg_query.Node_VariableSetStmt:
		return p.buildSet(n.VariableSetStmt)
	case *pg_query.Node_VariableShowStmt:
		return p.buildShow(n.VariableShowStmt)
	case *pg_query.Node_DeallocateStmt:
		return p.buildDeallocate(n.DeallocateStmt)
	default:
		return nil, fmt.Errorf("unsupported statement type: %T", node.Node)
	}
}

// ---- SELECT ----

func (p *Planner) buildSelect(stmt *pg_query.SelectStmt) (Plan, error) {
	plan := &SelectPlan{
		Limit:  -1,
		Offset: 0,
	}

	// FROM clause
	if len(stmt.FromClause) > 0 {
		tableName, joins, err := p.buildFrom(stmt.FromClause)
		if err != nil {
			return nil, err
		}
		plan.TableName = tableName
		plan.Joins = joins
	}

	// WHERE clause
	if stmt.WhereClause != nil {
		filter, err := p.buildExpr(stmt.WhereClause)
		if err != nil {
			return nil, err
		}
		plan.Filter = filter
	}

	// SELECT list
	if err := p.buildTargetList(stmt.TargetList, plan); err != nil {
		return nil, err
	}

	// ORDER BY
	for _, sortNode := range stmt.SortClause {
		if sc := sortNode.GetSortBy(); sc != nil {
			item := OrderByItem{Desc: sc.SortbyDir == pg_query.SortByDir_SORTBY_DESC}
			if sc.Node != nil {
				expr, err := p.buildExpr(sc.Node)
				if err != nil {
					return nil, err
				}
				if colRef, ok := expr.(*ColRefExpr); ok {
					item.Col = colRef.Col
				}
				item.Expr = expr
			}
			plan.OrderBy = append(plan.OrderBy, item)
		}
	}

	// GROUP BY
	for _, gbNode := range stmt.GroupClause {
		expr, err := p.buildExpr(gbNode)
		if err != nil {
			return nil, err
		}
		if colRef, ok := expr.(*ColRefExpr); ok {
			plan.GroupBy = append(plan.GroupBy, colRef.Col)
		}
	}

	// LIMIT
	if stmt.LimitCount != nil {
		expr, err := p.buildExpr(stmt.LimitCount)
		if err != nil {
			return nil, err
		}
		if lit, ok := expr.(*LiteralExpr); ok {
			plan.Limit = lit.Val.IntVal
		}
	}

	// OFFSET
	if stmt.LimitOffset != nil {
		expr, err := p.buildExpr(stmt.LimitOffset)
		if err != nil {
			return nil, err
		}
		if lit, ok := expr.(*LiteralExpr); ok {
			plan.Offset = lit.Val.IntVal
		}
	}

	return plan, nil
}

func (p *Planner) buildFrom(fromClause []*pg_query.Node) (string, []Join, error) {
	var tableName string
	var joins []Join

	for _, fromNode := range fromClause {
		switch f := fromNode.Node.(type) {
		case *pg_query.Node_RangeVar:
			rv := f.RangeVar
			if tableName == "" {
				tableName = rv.Relname
			} else {
				// Cross join
				joins = append(joins, Join{
					TableName: rv.Relname,
					Kind:      "cross",
				})
			}
		case *pg_query.Node_JoinExpr:
			j := f.JoinExpr
			// Extract right table
			rightTable := ""
			if j.Rarg != nil {
				if rv := j.Rarg.GetRangeVar(); rv != nil {
					rightTable = rv.Relname
				}
			}
			// Extract left table if this is the first
			if tableName == "" && j.Larg != nil {
				if rv := j.Larg.GetRangeVar(); rv != nil {
					tableName = rv.Relname
				}
			}
			kind := "inner"
			switch j.Jointype {
			case pg_query.JoinType_JOIN_LEFT:
				kind = "left"
			case pg_query.JoinType_JOIN_RIGHT:
				kind = "right"
			case pg_query.JoinType_JOIN_FULL:
				kind = "full"
			}
			var onExpr Expr
			if j.Quals != nil {
				var err error
				onExpr, err = p.buildExpr(j.Quals)
				if err != nil {
					return "", nil, err
				}
			}
			joins = append(joins, Join{
				TableName: rightTable,
				Kind:      kind,
				On:        onExpr,
			})
		}
	}
	return tableName, joins, nil
}

func (p *Planner) buildTargetList(targets []*pg_query.Node, plan *SelectPlan) error {
	for _, tNode := range targets {
		rt := tNode.GetResTarget()
		if rt == nil {
			continue
		}
		if rt.Val == nil {
			continue
		}

		// Check for star
		if rt.Val.GetColumnRef() != nil {
			cr := rt.Val.GetColumnRef()
			for _, field := range cr.Fields {
				if field.GetAStar() != nil {
					// SELECT *
					plan.Columns = nil
					plan.ColExprs = nil
					return nil
				}
			}
		}

		// Check for aggregate function
		if rt.Val.GetFuncCall() != nil {
			fc := rt.Val.GetFuncCall()
			fnName := ""
			if len(fc.Funcname) > 0 {
				if sv := fc.Funcname[0].GetString_(); sv != nil {
					fnName = strings.ToLower(sv.Sval)
				}
			}
			switch fnName {
			case "count", "sum", "avg", "min", "max":
				agg := AggFunc{Fn: fnName, Alias: rt.Name}
				if fc.AggStar {
					agg.Col = "*"
				} else if len(fc.Args) > 0 {
					argExpr, err := p.buildExpr(fc.Args[0])
					if err != nil {
						return err
					}
					if colRef, ok := argExpr.(*ColRefExpr); ok {
						agg.Col = colRef.Col
					}
				}
				plan.Aggregates = append(plan.Aggregates, agg)
				continue
			}
		}

		// Regular expression
		expr, err := p.buildExpr(rt.Val)
		if err != nil {
			return err
		}
		alias := rt.Name
		if alias == "" {
			if colRef, ok := expr.(*ColRefExpr); ok {
				alias = colRef.Col
			}
		}
		plan.ColExprs = append(plan.ColExprs, ColExpr{Expr: expr, Alias: alias})

		// Also track column names for simple column references
		if colRef, ok := expr.(*ColRefExpr); ok {
			plan.Columns = append(plan.Columns, colRef.Col)
		} else {
			plan.Columns = append(plan.Columns, alias)
		}
	}
	return nil
}

// ---- INSERT ----

func (p *Planner) buildInsert(stmt *pg_query.InsertStmt) (Plan, error) {
	plan := &InsertPlan{}
	if stmt.Relation != nil {
		plan.TableName = stmt.Relation.Relname
	}

	// Column names
	for _, colNode := range stmt.Cols {
		rt := colNode.GetResTarget()
		if rt != nil {
			plan.ColNames = append(plan.ColNames, rt.Name)
		}
	}

	// VALUES
	if stmt.SelectStmt != nil {
		sel := stmt.SelectStmt.GetSelectStmt()
		if sel != nil && len(sel.ValuesLists) > 0 {
			for _, valueList := range sel.ValuesLists {
				listNode := valueList.GetList()
				if listNode == nil {
					continue
				}
				var rowExprs []Expr
				for _, valNode := range listNode.Items {
					expr, err := p.buildExpr(valNode)
					if err != nil {
						return nil, err
					}
					rowExprs = append(rowExprs, expr)
				}
				plan.Rows = append(plan.Rows, rowExprs)
			}
		}
	}

	return plan, nil
}

// ---- UPDATE ----

func (p *Planner) buildUpdate(stmt *pg_query.UpdateStmt) (Plan, error) {
	plan := &UpdatePlan{}
	if stmt.Relation != nil {
		plan.TableName = stmt.Relation.Relname
	}

	// SET assignments
	for _, targetNode := range stmt.TargetList {
		rt := targetNode.GetResTarget()
		if rt == nil {
			continue
		}
		expr, err := p.buildExpr(rt.Val)
		if err != nil {
			return nil, err
		}
		plan.Assignments = append(plan.Assignments, Assignment{
			Col:  rt.Name,
			Expr: expr,
		})
	}

	// WHERE
	if stmt.WhereClause != nil {
		filter, err := p.buildExpr(stmt.WhereClause)
		if err != nil {
			return nil, err
		}
		plan.Filter = filter
	}

	return plan, nil
}

// ---- DELETE ----

func (p *Planner) buildDelete(stmt *pg_query.DeleteStmt) (Plan, error) {
	plan := &DeletePlan{}
	if stmt.Relation != nil {
		plan.TableName = stmt.Relation.Relname
	}
	if stmt.WhereClause != nil {
		filter, err := p.buildExpr(stmt.WhereClause)
		if err != nil {
			return nil, err
		}
		plan.Filter = filter
	}
	return plan, nil
}

// ---- CREATE TABLE ----

func (p *Planner) buildCreateTable(stmt *pg_query.CreateStmt) (Plan, error) {
	td := &catalog.TableDescriptor{}
	if stmt.Relation != nil {
		td.Name = stmt.Relation.Relname
	}
	ifNotExists := stmt.IfNotExists

	for _, eltNode := range stmt.TableElts {
		colDef := eltNode.GetColumnDef()
		if colDef == nil {
			// Could be a table constraint
			continue
		}
		col, err := p.buildColumnDef(colDef, len(td.Columns))
		if err != nil {
			return nil, err
		}
		td.Columns = append(td.Columns, col)
		if col.IsPrimaryKey {
			td.PKColumns = append(td.PKColumns, col.Name)
		}
	}

	// Handle table-level constraints (e.g., PRIMARY KEY(...))
	for _, eltNode := range stmt.TableElts {
		constraint := eltNode.GetConstraint()
		if constraint == nil {
			continue
		}
		if constraint.Contype == pg_query.ConstrType_CONSTR_PRIMARY {
			td.PKColumns = nil
			for _, keyNode := range constraint.Keys {
				if sv := keyNode.GetString_(); sv != nil {
					colName := sv.Sval
					td.PKColumns = append(td.PKColumns, colName)
					// Mark column as PK
					for i := range td.Columns {
						if td.Columns[i].Name == colName {
							td.Columns[i].IsPrimaryKey = true
						}
					}
				}
			}
		}
	}

	return &CreateTablePlan{Desc: td, IfNotExists: ifNotExists}, nil
}

func (p *Planner) buildColumnDef(colDef *pg_query.ColumnDef, ordinal int) (catalog.ColumnDef, error) {
	col := catalog.ColumnDef{
		Ordinal:  ordinal,
		Name:     colDef.Colname,
		Nullable: true,
	}

	// Type name
	if colDef.TypeName != nil {
		dt, varcharLen, err := p.pgTypeToDT(colDef.TypeName)
		if err != nil {
			return col, err
		}
		col.Type = dt
		col.VarCharLen = varcharLen
	}

	// Constraints
	for _, cNode := range colDef.Constraints {
		constraint := cNode.GetConstraint()
		if constraint == nil {
			continue
		}
		switch constraint.Contype {
		case pg_query.ConstrType_CONSTR_PRIMARY:
			col.IsPrimaryKey = true
			col.Nullable = false
		case pg_query.ConstrType_CONSTR_NOTNULL:
			col.Nullable = false
		case pg_query.ConstrType_CONSTR_UNIQUE:
			col.IsUnique = true
		case pg_query.ConstrType_CONSTR_DEFAULT:
			col.HasDefault = true
			if constraint.RawExpr != nil {
				// Try to parse default value
				if ac := constraint.RawExpr.GetAConst(); ac != nil {
					col.DefaultValue = p.aConstToString(ac)
				} else if fc := constraint.RawExpr.GetFuncCall(); fc != nil {
					// e.g., nextval('seq')
					col.AutoIncrement = true
				}
			}
		case pg_query.ConstrType_CONSTR_GENERATED:
			col.AutoIncrement = true
		}
	}

	// SERIAL type implies auto-increment
	if colDef.TypeName != nil {
		for _, nameNode := range colDef.TypeName.Names {
			if sv := nameNode.GetString_(); sv != nil {
				lower := strings.ToLower(sv.Sval)
				if lower == "serial" || lower == "bigserial" || lower == "smallserial" {
					col.AutoIncrement = true
					col.HasDefault = true
					col.Nullable = false
					if lower == "bigserial" {
						col.Type = types.TypeBigInt
					} else {
						col.Type = types.TypeInteger
					}
				}
			}
		}
	}

	return col, nil
}

func (p *Planner) aConstToString(ac *pg_query.A_Const) string {
	if ac == nil {
		return ""
	}
	switch v := ac.Val.(type) {
	case *pg_query.A_Const_Ival:
		return strconv.FormatInt(int64(v.Ival.Ival), 10)
	case *pg_query.A_Const_Fval:
		return v.Fval.Fval
	case *pg_query.A_Const_Sval:
		return v.Sval.Sval
	case *pg_query.A_Const_Boolval:
		if v.Boolval.Boolval {
			return "true"
		}
		return "false"
	}
	return ""
}

func (p *Planner) pgTypeToDT(tn *pg_query.TypeName) (types.DataType, int, error) {
	var typeName string
	for _, nameNode := range tn.Names {
		if sv := nameNode.GetString_(); sv != nil {
			n := strings.ToLower(sv.Sval)
			if n != "pg_catalog" {
				typeName = n
			}
		}
	}

	varcharLen := 0
	if len(tn.Typmods) > 0 {
		if ac := tn.Typmods[0].GetAConst(); ac != nil {
			if iv, ok := ac.Val.(*pg_query.A_Const_Ival); ok {
				varcharLen = int(iv.Ival.Ival)
			}
		}
	}

	switch typeName {
	case "int", "int4", "integer", "serial":
		return types.TypeInteger, 0, nil
	case "int8", "int2", "bigint", "bigserial":
		return types.TypeBigInt, 0, nil
	case "float4", "real":
		return types.TypeFloat, 0, nil
	case "float8", "double", "double precision", "numeric", "decimal":
		return types.TypeDouble, 0, nil
	case "text", "name":
		return types.TypeText, 0, nil
	case "varchar", "character varying", "character", "char", "bpchar":
		return types.TypeVarChar, varcharLen, nil
	case "bool", "boolean":
		return types.TypeBoolean, 0, nil
	case "timestamp", "timestamptz", "timestamp without time zone", "timestamp with time zone", "date":
		return types.TypeTimestamp, 0, nil
	default:
		// Default to text for unknown types
		return types.TypeText, 0, nil
	}
}

// ---- DROP ----

func (p *Planner) buildDrop(stmt *pg_query.DropStmt) (Plan, error) {
	switch stmt.RemoveType {
	case pg_query.ObjectType_OBJECT_TABLE:
		if len(stmt.Objects) == 0 {
			return nil, fmt.Errorf("DROP TABLE: no table specified")
		}
		// Each object is a List of String nodes (schema.table)
		tableName := ""
		obj := stmt.Objects[0]
		if list := obj.GetList(); list != nil {
			for _, item := range list.Items {
				if sv := item.GetString_(); sv != nil {
					tableName = sv.Sval
				}
			}
		} else if sv := obj.GetString_(); sv != nil {
			tableName = sv.Sval
		}
		return &DropTablePlan{TableName: tableName, IfExists: stmt.MissingOk}, nil

	case pg_query.ObjectType_OBJECT_INDEX:
		if len(stmt.Objects) == 0 {
			return nil, fmt.Errorf("DROP INDEX: no index specified")
		}
		indexName := ""
		obj := stmt.Objects[0]
		if list := obj.GetList(); list != nil {
			for _, item := range list.Items {
				if sv := item.GetString_(); sv != nil {
					indexName = sv.Sval
				}
			}
		} else if sv := obj.GetString_(); sv != nil {
			indexName = sv.Sval
		}
		return &DropIndexPlan{IndexName: indexName, IfExists: stmt.MissingOk}, nil

	default:
		return nil, fmt.Errorf("unsupported DROP type: %v", stmt.RemoveType)
	}
}

// ---- CREATE INDEX ----

func (p *Planner) buildCreateIndex(stmt *pg_query.IndexStmt) (Plan, error) {
	desc := &catalog.IndexDescriptor{
		Name:      stmt.Idxname,
		IsUnique:  stmt.Unique,
		TableName: stmt.Relation.Relname,
	}
	for _, param := range stmt.IndexParams {
		ie := param.GetIndexElem()
		if ie != nil && ie.Name != "" {
			desc.Columns = append(desc.Columns, ie.Name)
		}
	}
	return &CreateIndexPlan{Desc: desc}, nil
}

// ---- TRANSACTION ----

func (p *Planner) buildTransaction(stmt *pg_query.TransactionStmt) (Plan, error) {
	switch stmt.Kind {
	case pg_query.TransactionStmtKind_TRANS_STMT_BEGIN,
		pg_query.TransactionStmtKind_TRANS_STMT_START:
		return &TxnPlan{Kind: "begin"}, nil
	case pg_query.TransactionStmtKind_TRANS_STMT_COMMIT:
		return &TxnPlan{Kind: "commit"}, nil
	case pg_query.TransactionStmtKind_TRANS_STMT_ROLLBACK:
		return &TxnPlan{Kind: "rollback"}, nil
	case pg_query.TransactionStmtKind_TRANS_STMT_SAVEPOINT:
		return &TxnPlan{Kind: "savepoint"}, nil
	case pg_query.TransactionStmtKind_TRANS_STMT_ROLLBACK_TO:
		return &TxnPlan{Kind: "rollback"}, nil
	case pg_query.TransactionStmtKind_TRANS_STMT_RELEASE:
		return &TxnPlan{Kind: "commit"}, nil
	default:
		return &TxnPlan{Kind: "begin"}, nil
	}
}

// ---- SET / SHOW ----

func (p *Planner) buildSet(stmt *pg_query.VariableSetStmt) (Plan, error) {
	val := ""
	if len(stmt.Args) > 0 {
		if ac := stmt.Args[0].GetAConst(); ac != nil {
			val = p.aConstToString(ac)
		} else if sv := stmt.Args[0].GetString_(); sv != nil {
			val = sv.Sval
		}
	}
	return &SetPlan{Name: strings.ToLower(stmt.Name), Value: val}, nil
}

func (p *Planner) buildShow(stmt *pg_query.VariableShowStmt) (Plan, error) {
	return &ShowPlan{Name: strings.ToLower(stmt.Name)}, nil
}

func (p *Planner) buildDeallocate(stmt *pg_query.DeallocateStmt) (Plan, error) {
	return &DeallocatePlan{Name: stmt.Name}, nil
}

// ---- Expression builder ----

func (p *Planner) buildExpr(node *pg_query.Node) (Expr, error) {
	if node == nil {
		return &LiteralExpr{Val: types.Null}, nil
	}

	switch n := node.Node.(type) {
	case *pg_query.Node_AConst:
		return p.buildAConst(n.AConst)

	case *pg_query.Node_ColumnRef:
		return p.buildColumnRef(n.ColumnRef)

	case *pg_query.Node_ParamRef:
		return &ParamExpr{Idx: int(n.ParamRef.Number)}, nil

	case *pg_query.Node_AExpr:
		return p.buildAExpr(n.AExpr)

	case *pg_query.Node_BoolExpr:
		return p.buildBoolExpr(n.BoolExpr)

	case *pg_query.Node_NullTest:
		nt := n.NullTest
		inner, err := p.buildExpr(nt.Arg)
		if err != nil {
			return nil, err
		}
		return &IsNullExpr{E: inner, IsNull: nt.Nulltesttype == pg_query.NullTestType_IS_NULL}, nil

	case *pg_query.Node_FuncCall:
		return p.buildFuncCall(n.FuncCall)

	case *pg_query.Node_TypeCast:
		return p.buildTypeCast(n.TypeCast)

	case *pg_query.Node_AStar:
		return &StarExpr{}, nil

	case *pg_query.Node_List:
		// Shouldn't happen directly, but handle gracefully
		if len(n.List.Items) > 0 {
			return p.buildExpr(n.List.Items[0])
		}
		return &LiteralExpr{Val: types.Null}, nil

	case *pg_query.Node_String_:
		return &LiteralExpr{Val: types.TextValue(n.String_.Sval)}, nil

	case *pg_query.Node_Integer:
		return &LiteralExpr{Val: types.IntValue(int64(n.Integer.Ival))}, nil

	case *pg_query.Node_Float:
		f, err := strconv.ParseFloat(n.Float.Fval, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid float: %s", n.Float.Fval)
		}
		return &LiteralExpr{Val: types.DoubleValue(f)}, nil

	case *pg_query.Node_BooleanTest:
		bt := n.BooleanTest
		inner, err := p.buildExpr(bt.Arg)
		if err != nil {
			return nil, err
		}
		// IS TRUE / IS FALSE / IS NOT TRUE / IS NOT FALSE
		switch bt.Booltesttype {
		case pg_query.BoolTestType_IS_TRUE:
			return &BinOpExpr{Op: "=", Left: inner, Right: &LiteralExpr{Val: types.BoolValue(true)}}, nil
		case pg_query.BoolTestType_IS_FALSE:
			return &BinOpExpr{Op: "=", Left: inner, Right: &LiteralExpr{Val: types.BoolValue(false)}}, nil
		case pg_query.BoolTestType_IS_NOT_TRUE:
			return &BinOpExpr{Op: "!=", Left: inner, Right: &LiteralExpr{Val: types.BoolValue(true)}}, nil
		case pg_query.BoolTestType_IS_NOT_FALSE:
			return &BinOpExpr{Op: "!=", Left: inner, Right: &LiteralExpr{Val: types.BoolValue(false)}}, nil
		}
		return inner, nil

	case *pg_query.Node_SubLink:
		// Subquery — not supported, return null
		return &LiteralExpr{Val: types.Null}, nil

	case *pg_query.Node_CaseExpr:
		return p.buildCaseExpr(n.CaseExpr)

	case *pg_query.Node_CoalesceExpr:
		args := make([]Expr, 0, len(n.CoalesceExpr.Args))
		for _, a := range n.CoalesceExpr.Args {
			e, err := p.buildExpr(a)
			if err != nil {
				return nil, err
			}
			args = append(args, e)
		}
		return &FuncExpr{Name: "coalesce", Args: args}, nil

	default:
		return &LiteralExpr{Val: types.Null}, nil
	}
}

func (p *Planner) buildAConst(ac *pg_query.A_Const) (Expr, error) {
	if ac == nil {
		return &LiteralExpr{Val: types.Null}, nil
	}
	switch v := ac.Val.(type) {
	case *pg_query.A_Const_Ival:
		return &LiteralExpr{Val: types.IntValue(int64(v.Ival.Ival))}, nil
	case *pg_query.A_Const_Fval:
		f, err := strconv.ParseFloat(v.Fval.Fval, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid float literal: %s", v.Fval.Fval)
		}
		return &LiteralExpr{Val: types.DoubleValue(f)}, nil
	case *pg_query.A_Const_Sval:
		return &LiteralExpr{Val: types.TextValue(v.Sval.Sval)}, nil
	case *pg_query.A_Const_Boolval:
		return &LiteralExpr{Val: types.BoolValue(v.Boolval.Boolval)}, nil
	default:
		if ac.GetIsnull() {
			return &LiteralExpr{Val: types.Null}, nil
		}
		return &LiteralExpr{Val: types.Null}, nil
	}
}

func (p *Planner) buildColumnRef(cr *pg_query.ColumnRef) (Expr, error) {
	ref := &ColRefExpr{}
	switch len(cr.Fields) {
	case 0:
		return ref, nil
	case 1:
		if sv := cr.Fields[0].GetString_(); sv != nil {
			ref.Col = sv.Sval
		} else if cr.Fields[0].GetAStar() != nil {
			return &StarExpr{}, nil
		}
	default:
		// table.col
		if sv := cr.Fields[0].GetString_(); sv != nil {
			ref.Table = sv.Sval
		}
		if sv := cr.Fields[1].GetString_(); sv != nil {
			ref.Col = sv.Sval
		} else if cr.Fields[1].GetAStar() != nil {
			return &StarExpr{}, nil
		}
	}
	return ref, nil
}

func (p *Planner) buildAExpr(ae *pg_query.A_Expr) (Expr, error) {
	// Get operator name
	opName := ""
	if len(ae.Name) > 0 {
		if sv := ae.Name[0].GetString_(); sv != nil {
			opName = sv.Sval
		}
	}

	switch ae.Kind {
	case pg_query.A_Expr_Kind_AEXPR_OP,
		pg_query.A_Expr_Kind_AEXPR_OP_ANY,
		pg_query.A_Expr_Kind_AEXPR_OP_ALL:
		left, err := p.buildExpr(ae.Lexpr)
		if err != nil {
			return nil, err
		}
		right, err := p.buildExpr(ae.Rexpr)
		if err != nil {
			return nil, err
		}
		op := opName
		// Normalize operator
		switch op {
		case "<>":
			op = "!="
		}
		return &BinOpExpr{Op: op, Left: left, Right: right}, nil

	case pg_query.A_Expr_Kind_AEXPR_LIKE:
		left, err := p.buildExpr(ae.Lexpr)
		if err != nil {
			return nil, err
		}
		right, err := p.buildExpr(ae.Rexpr)
		if err != nil {
			return nil, err
		}
		negate := opName == "!~~" || strings.Contains(opName, "NOT")
		return &LikeExpr{E: left, Pattern: right, Negate: negate}, nil

	case pg_query.A_Expr_Kind_AEXPR_ILIKE:
		left, err := p.buildExpr(ae.Lexpr)
		if err != nil {
			return nil, err
		}
		right, err := p.buildExpr(ae.Rexpr)
		if err != nil {
			return nil, err
		}
		negate := strings.Contains(opName, "!") || strings.Contains(strings.ToUpper(opName), "NOT")
		return &LikeExpr{E: left, Pattern: right, Negate: negate}, nil

	case pg_query.A_Expr_Kind_AEXPR_IN:
		left, err := p.buildExpr(ae.Lexpr)
		if err != nil {
			return nil, err
		}
		negate := opName == "<>"
		var list []Expr
		if listNode := ae.Rexpr.GetList(); listNode != nil {
			for _, item := range listNode.Items {
				expr, err := p.buildExpr(item)
				if err != nil {
					return nil, err
				}
				list = append(list, expr)
			}
		}
		return &InExpr{E: left, List: list, Negate: negate}, nil

	case pg_query.A_Expr_Kind_AEXPR_BETWEEN,
		pg_query.A_Expr_Kind_AEXPR_NOT_BETWEEN:
		left, err := p.buildExpr(ae.Lexpr)
		if err != nil {
			return nil, err
		}
		var low, high Expr
		if listNode := ae.Rexpr.GetList(); listNode != nil && len(listNode.Items) == 2 {
			low, err = p.buildExpr(listNode.Items[0])
			if err != nil {
				return nil, err
			}
			high, err = p.buildExpr(listNode.Items[1])
			if err != nil {
				return nil, err
			}
		}
		negate := ae.Kind == pg_query.A_Expr_Kind_AEXPR_NOT_BETWEEN
		return &BetweenExpr{E: left, Low: low, High: high, Negate: negate}, nil

	default:
		// Try to handle as a binary op anyway
		if ae.Lexpr != nil && ae.Rexpr != nil {
			left, err := p.buildExpr(ae.Lexpr)
			if err != nil {
				return nil, err
			}
			right, err := p.buildExpr(ae.Rexpr)
			if err != nil {
				return nil, err
			}
			return &BinOpExpr{Op: opName, Left: left, Right: right}, nil
		}
		return &LiteralExpr{Val: types.Null}, nil
	}
}

func (p *Planner) buildBoolExpr(be *pg_query.BoolExpr) (Expr, error) {
	if len(be.Args) == 0 {
		return &LiteralExpr{Val: types.BoolValue(true)}, nil
	}

	switch be.Boolop {
	case pg_query.BoolExprType_NOT_EXPR:
		inner, err := p.buildExpr(be.Args[0])
		if err != nil {
			return nil, err
		}
		return &UnaryExpr{Op: "NOT", E: inner}, nil

	case pg_query.BoolExprType_AND_EXPR:
		left, err := p.buildExpr(be.Args[0])
		if err != nil {
			return nil, err
		}
		for _, argNode := range be.Args[1:] {
			right, err := p.buildExpr(argNode)
			if err != nil {
				return nil, err
			}
			left = &BinOpExpr{Op: "AND", Left: left, Right: right}
		}
		return left, nil

	case pg_query.BoolExprType_OR_EXPR:
		left, err := p.buildExpr(be.Args[0])
		if err != nil {
			return nil, err
		}
		for _, argNode := range be.Args[1:] {
			right, err := p.buildExpr(argNode)
			if err != nil {
				return nil, err
			}
			left = &BinOpExpr{Op: "OR", Left: left, Right: right}
		}
		return left, nil

	default:
		return &LiteralExpr{Val: types.BoolValue(true)}, nil
	}
}

func (p *Planner) buildFuncCall(fc *pg_query.FuncCall) (Expr, error) {
	fnName := ""
	if len(fc.Funcname) > 0 {
		// Collect all name parts
		parts := make([]string, 0, len(fc.Funcname))
		for _, n := range fc.Funcname {
			if sv := n.GetString_(); sv != nil {
				parts = append(parts, strings.ToLower(sv.Sval))
			}
		}
		fnName = strings.Join(parts, ".")
	}

	args := make([]Expr, 0, len(fc.Args))
	if fc.AggStar {
		args = append(args, &StarExpr{})
	}
	for _, argNode := range fc.Args {
		expr, err := p.buildExpr(argNode)
		if err != nil {
			return nil, err
		}
		args = append(args, expr)
	}
	return &FuncExpr{Name: fnName, Args: args}, nil
}

func (p *Planner) buildTypeCast(tc *pg_query.TypeCast) (Expr, error) {
	inner, err := p.buildExpr(tc.Arg)
	if err != nil {
		return nil, err
	}
	if tc.TypeName == nil {
		return inner, nil
	}
	dt, _, err := p.pgTypeToDT(tc.TypeName)
	if err != nil {
		return inner, nil
	}
	return &CastExpr{E: inner, To: dt}, nil
}

func (p *Planner) buildCaseExpr(ce *pg_query.CaseExpr) (Expr, error) {
	// Build CASE as nested IF expressions represented as FuncExpr "case"
	// Simplified: for each WHEN/THEN pair, build a BinOpExpr chain
	// We represent as FuncExpr "case" with alternating condition/result args, last arg = ELSE
	args := make([]Expr, 0, len(ce.Args)*2+1)
	for _, whenNode := range ce.Args {
		cw := whenNode.GetCaseWhen()
		if cw == nil {
			continue
		}
		cond, err := p.buildExpr(cw.Expr)
		if err != nil {
			return nil, err
		}
		result, err := p.buildExpr(cw.Result)
		if err != nil {
			return nil, err
		}
		args = append(args, cond, result)
	}
	if ce.Defresult != nil {
		elseExpr, err := p.buildExpr(ce.Defresult)
		if err != nil {
			return nil, err
		}
		args = append(args, elseExpr)
	} else {
		args = append(args, &LiteralExpr{Val: types.Null})
	}
	return &FuncExpr{Name: "case", Args: args}, nil
}
