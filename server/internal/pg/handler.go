package pg

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgproto3/v2"

	sqlpkg "oxendb/server/internal/sql"
	"oxendb/server/internal/sql/catalog"
	"oxendb/server/internal/sql/result"
	"oxendb/server/internal/sql/types"
)

// OID constants for PostgreSQL data types.
var dtToOID = map[types.DataType]uint32{
	types.TypeBoolean:   16,
	types.TypeBigInt:    20,
	types.TypeInteger:   23,
	types.TypeText:      25,
	types.TypeFloat:     700,
	types.TypeDouble:    701,
	types.TypeVarChar:   1043,
	types.TypeTimestamp: 1184,
	types.TypeNull:      25, // treat as text
}

// handleSimpleQuery handles the 'Q' (simple query) message.
func (c *Conn) handleSimpleQuery(q *pgproto3.Query) {
	sql := strings.TrimSpace(q.String)

	// TxFailed: only allow ROLLBACK
	if c.session.TxState == TxFailed {
		lower := strings.ToLower(sql)
		if !strings.HasPrefix(lower, "rollback") {
			c.sendError("25P02", "ERROR", "current transaction is aborted, commands ignored until end of transaction block")
			c.sendReadyForQuery()
			return
		}
	}

	ctx := context.Background()

	// Intercept system catalog queries for compatibility
	if rs, ok := c.interceptQuery(sql); ok {
		if rs != nil {
			c.sendResultSet(rs)
		}
		c.sendReadyForQuery()
		return
	}

	// Execute the SQL
	results, err := c.engine.Execute(ctx, sql, c.session.ActiveTxn, nil)
	if err != nil {
		c.session.TxState = TxFailed
		c.sendError("42601", "ERROR", err.Error())
		c.sendReadyForQuery()
		return
	}

	for _, rs := range results {
		// Handle transaction control statements
		tag := strings.ToUpper(strings.TrimSpace(rs.Tag))
		switch tag {
		case "BEGIN":
			if c.session.TxState == TxIdle {
				c.session.TxState = TxActive
				c.session.ActiveTxn = c.engine.NewTxn()
			}
			if err := c.be.Send(&pgproto3.CommandComplete{CommandTag: []byte("BEGIN")}); err != nil {
				return
			}
			continue
		case "COMMIT":
			if c.session.TxState == TxActive && c.session.ActiveTxn != nil {
				if err := c.session.ActiveTxn.Commit(); err != nil {
					c.sendError("XX000", "ERROR", "commit failed: "+err.Error())
					c.session.TxState = TxFailed
					continue
				}
			}
			c.session.TxState = TxIdle
			c.session.ActiveTxn = nil
			if err := c.be.Send(&pgproto3.CommandComplete{CommandTag: []byte("COMMIT")}); err != nil {
				return
			}
			continue
		case "ROLLBACK", "SAVEPOINT", "RELEASE":
			if c.session.ActiveTxn != nil {
				c.session.ActiveTxn.Rollback()
			}
			c.session.TxState = TxIdle
			c.session.ActiveTxn = nil
			if err := c.be.Send(&pgproto3.CommandComplete{CommandTag: []byte(tag)}); err != nil {
				return
			}
			continue
		}

		c.sendResultSet(rs)
	}

	c.sendReadyForQuery()
}

// sendResultSet sends a complete result set to the client.
func (c *Conn) sendResultSet(rs *result.ResultSet) {
	if rs == nil {
		return
	}

	if len(rs.Columns) > 0 {
		// Send row description
		rd := buildRowDescription(rs.Columns)
		if err := c.be.Send(rd); err != nil {
			return
		}
		// Send data rows
		for _, row := range rs.Rows {
			dr := buildDataRow(row)
			if err := c.be.Send(dr); err != nil {
				return
			}
		}
	}

	// Command complete
	tag := rs.Tag
	if tag == "" {
		tag = "OK"
	}
	_ = c.be.Send(&pgproto3.CommandComplete{CommandTag: []byte(tag)})
}

// handleParse handles the 'P' (Parse) message in the extended query protocol.
func (c *Conn) handleParse(p *pgproto3.Parse) {
	plans, err := c.engine.ParseOnly(p.Query)
	if err != nil {
		c.sendError("42601", "ERROR", "parse error: "+err.Error())
		return
	}

	stmt := &PreparedStatement{
		Name:  p.Name,
		SQL:   p.Query,
		Plans: make([]interface{}, len(plans)),
	}
	for i, plan := range plans {
		stmt.Plans[i] = plan
	}

	c.session.Prepared[p.Name] = stmt
	_ = c.be.Send(&pgproto3.ParseComplete{})
}

// handleBind handles the 'B' (Bind) message in the extended query protocol.
func (c *Conn) handleBind(b *pgproto3.Bind) {
	stmt, ok := c.session.Prepared[b.PreparedStatement]
	if !ok {
		c.sendError("26000", "ERROR", fmt.Sprintf("prepared statement %q does not exist", b.PreparedStatement))
		return
	}

	// Parse parameter values — all treated as text format (format code 0)
	args := make([]types.Value, len(b.Parameters))
	for i, param := range b.Parameters {
		if param == nil {
			args[i] = types.Null
			continue
		}
		// Parse as text
		s := string(param)
		args[i] = types.TextValue(s)
	}

	portal := &Portal{
		Name: b.DestinationPortal,
		Stmt: stmt,
		Args: args,
	}
	c.session.Portals[b.DestinationPortal] = portal
	_ = c.be.Send(&pgproto3.BindComplete{})
}

// handleDescribe handles the 'D' (Describe) message in the extended query protocol.
func (c *Conn) handleDescribe(d *pgproto3.Describe) {
	switch d.ObjectType {
	case 'S':
		// Describe a prepared statement
		stmt, ok := c.session.Prepared[d.Name]
		if !ok {
			c.sendError("26000", "ERROR", fmt.Sprintf("prepared statement %q does not exist", d.Name))
			return
		}
		// Send ParameterDescription (empty for now — no type inference)
		_ = c.be.Send(&pgproto3.ParameterDescription{ParameterOIDs: nil})
		// Determine result columns from first SELECT plan
		if rd := c.describeStmt(stmt); rd != nil {
			_ = c.be.Send(rd)
		} else {
			_ = c.be.Send(&pgproto3.NoData{})
		}

	case 'P':
		// Describe a portal
		portal, ok := c.session.Portals[d.Name]
		if !ok {
			c.sendError("34000", "ERROR", fmt.Sprintf("portal %q does not exist", d.Name))
			return
		}
		if rd := c.describePortal(portal); rd != nil {
			_ = c.be.Send(rd)
		} else {
			_ = c.be.Send(&pgproto3.NoData{})
		}
	}
}

// handleExecute handles the 'E' (Execute) message in the extended query protocol.
func (c *Conn) handleExecute(ex *pgproto3.Execute) {
	portal, ok := c.session.Portals[ex.Portal]
	if !ok {
		c.sendError("34000", "ERROR", fmt.Sprintf("portal %q does not exist", ex.Portal))
		return
	}

	ctx := context.Background()

	// Execute the plan if not yet done
	if portal.Rows == nil {
		if len(portal.Stmt.Plans) == 0 {
			_ = c.be.Send(&pgproto3.CommandComplete{CommandTag: []byte("OK")})
			return
		}

		// Execute each plan
		var finalRS *result.ResultSet
		for _, planIface := range portal.Stmt.Plans {
			plan, ok := planIface.(sqlpkg.Plan)
			if !ok {
				continue
			}
			// Handle TXN plans
			if tp, ok := planIface.(*sqlpkg.TxnPlan); ok {
				switch strings.ToLower(tp.Kind) {
				case "begin":
					if c.session.TxState == TxIdle {
						c.session.TxState = TxActive
						c.session.ActiveTxn = c.engine.NewTxn()
					}
				case "commit":
					if c.session.ActiveTxn != nil {
						_ = c.session.ActiveTxn.Commit()
					}
					c.session.TxState = TxIdle
					c.session.ActiveTxn = nil
				case "rollback":
					if c.session.ActiveTxn != nil {
						c.session.ActiveTxn.Rollback()
					}
					c.session.TxState = TxIdle
					c.session.ActiveTxn = nil
				}
				finalRS = &result.ResultSet{Tag: strings.ToUpper(tp.Kind)}
				continue
			}

			rs, err := c.engine.ExecPlanDirect(ctx, plan, c.session.ActiveTxn, portal.Args)
			if err != nil {
				c.session.TxState = TxFailed
				c.sendError("XX000", "ERROR", err.Error())
				return
			}
			finalRS = rs
		}
		portal.Rows = finalRS
		portal.Pos = 0
	}

	if portal.Rows == nil {
		_ = c.be.Send(&pgproto3.CommandComplete{CommandTag: []byte("OK")})
		return
	}

	rs := portal.Rows
	maxRows := int(ex.MaxRows)

	if len(rs.Columns) > 0 {
		// Stream rows
		total := len(rs.Rows)
		start := portal.Pos
		end := total
		if maxRows > 0 && start+maxRows < total {
			end = start + maxRows
		}

		for i := start; i < end; i++ {
			dr := buildDataRow(rs.Rows[i])
			if err := c.be.Send(dr); err != nil {
				return
			}
		}
		portal.Pos = end

		if end < total {
			// More rows available
			_ = c.be.Send(&pgproto3.PortalSuspended{})
			return
		}
	}

	// All rows sent
	tag := rs.Tag
	if tag == "" {
		tag = "SELECT 0"
	}
	_ = c.be.Send(&pgproto3.CommandComplete{CommandTag: []byte(tag)})
	delete(c.session.Portals, ex.Portal)
}

// handleSync handles the 'S' (Sync) message — ends an extended query cycle.
func (c *Conn) handleSync() {
	// If we have an implicit auto-commit txn (not started by BEGIN), commit it
	// For explicit transactions (TxActive), don't auto-commit
	c.sendReadyForQuery()
}

// handleTerminate handles the 'X' (Terminate) message.
func (c *Conn) handleTerminate() {
	_ = c.net.Close()
}

// ---- System catalog intercept ----

// interceptQuery intercepts common system catalog queries for tool compatibility.
// Returns (ResultSet, true) if the query was handled, or (nil, false) otherwise.
func (c *Conn) interceptQuery(sql string) (*result.ResultSet, bool) {
	trimmed := strings.TrimSpace(sql)
	lower := strings.ToLower(trimmed)
	// Remove trailing semicolons
	lower = strings.TrimRight(lower, "; \t\n\r")
	trimmed = strings.TrimRight(trimmed, "; \t\n\r")

	// DEALLOCATE
	if strings.HasPrefix(lower, "deallocate ") {
		name := strings.TrimSpace(lower[len("deallocate "):])
		if name == "all" {
			c.session.Prepared = make(map[string]*PreparedStatement)
		} else {
			delete(c.session.Prepared, name)
		}
		return &result.ResultSet{Tag: "DEALLOCATE"}, true
	}

	// SET commands — accept silently
	if strings.HasPrefix(lower, "set ") {
		return &result.ResultSet{Tag: "SET"}, true
	}

	// DISCARD ALL
	if lower == "discard all" {
		c.session.Prepared = make(map[string]*PreparedStatement)
		c.session.Portals = make(map[string]*Portal)
		return &result.ResultSet{Tag: "DISCARD"}, true
	}

	// Exact matches for common probe queries
	switch lower {
	case "select 1":
		return oneColResult("?column?", types.TypeInteger, types.IntValue(1)), true
	case `select 1 as "1"`, `select 1 as "?column?"`:
		return oneColResult("?column?", types.TypeInteger, types.IntValue(1)), true
	}

	// System function intercepts
	if lower == "select version()" {
		return oneTextResult("version", "OxenDB 0.1.0 (PostgreSQL 14.0 compatible)"), true
	}
	if lower == "select current_database()" {
		return oneTextResult("current_database", c.session.Database), true
	}
	if lower == "select current_schema()" {
		return oneTextResult("current_schema", "public"), true
	}
	if lower == "select current_schema(),version()" || lower == "select current_schema(), version()" {
		rs := &result.ResultSet{
			Columns: []result.ColMeta{
				{Name: "current_schema", DataType: types.TypeText},
				{Name: "version", DataType: types.TypeText},
			},
			Rows: []result.Row{
				{types.TextValue("public"), types.TextValue("OxenDB 0.1.0 (PostgreSQL 14.0 compatible)")},
			},
			Tag: "SELECT 1",
		}
		return rs, true
	}

	// pg_catalog.pg_namespace — return a single row for "public"
	if strings.Contains(lower, "pg_namespace") && !strings.Contains(lower, "information_schema") {
		rs := &result.ResultSet{
			Columns: []result.ColMeta{
				{Name: "oid", DataType: types.TypeInteger},
				{Name: "nspname", DataType: types.TypeText},
				{Name: "nspowner", DataType: types.TypeInteger},
			},
			Rows: []result.Row{
				{types.IntValue(2200), types.TextValue("public"), types.IntValue(10)},
			},
			Tag: "SELECT 1",
		}
		return rs, true
	}

	// pg_catalog.pg_type — return empty result with expected columns
	if strings.Contains(lower, "pg_type") && !strings.Contains(lower, "information_schema") {
		rs := &result.ResultSet{
			Columns: []result.ColMeta{
				{Name: "oid", DataType: types.TypeInteger},
				{Name: "typname", DataType: types.TypeText},
				{Name: "typnamespace", DataType: types.TypeInteger},
				{Name: "typowner", DataType: types.TypeInteger},
				{Name: "typlen", DataType: types.TypeInteger},
				{Name: "typtype", DataType: types.TypeText},
				{Name: "typcategory", DataType: types.TypeText},
			},
			Rows: []result.Row{},
			Tag:  "SELECT 0",
		}
		return rs, true
	}

	// pg_catalog.pg_class — return real tables
	if strings.Contains(lower, "pg_class") && !strings.Contains(lower, "information_schema") {
		return c.handlePgClass(), true
	}

	// pg_catalog.pg_attribute
	if strings.Contains(lower, "pg_attribute") && !strings.Contains(lower, "information_schema") {
		return c.handlePgAttribute(), true
	}

	// pg_catalog.pg_index
	if strings.Contains(lower, "pg_index") && !strings.Contains(lower, "information_schema") {
		rs := &result.ResultSet{
			Columns: []result.ColMeta{
				{Name: "indexrelid", DataType: types.TypeInteger},
				{Name: "indrelid", DataType: types.TypeInteger},
				{Name: "indnatts", DataType: types.TypeInteger},
				{Name: "indisunique", DataType: types.TypeBoolean},
				{Name: "indisprimary", DataType: types.TypeBoolean},
			},
			Rows: []result.Row{},
			Tag:  "SELECT 0",
		}
		return rs, true
	}

	// pg_catalog.pg_constraint
	if strings.Contains(lower, "pg_constraint") && !strings.Contains(lower, "information_schema") {
		rs := &result.ResultSet{
			Columns: []result.ColMeta{
				{Name: "oid", DataType: types.TypeInteger},
				{Name: "conname", DataType: types.TypeText},
				{Name: "contype", DataType: types.TypeText},
				{Name: "conrelid", DataType: types.TypeInteger},
			},
			Rows: []result.Row{},
			Tag:  "SELECT 0",
		}
		return rs, true
	}

	// pg_catalog.pg_roles / pg_user
	if strings.Contains(lower, "pg_roles") || strings.Contains(lower, "pg_user") {
		rs := &result.ResultSet{
			Columns: []result.ColMeta{
				{Name: "rolname", DataType: types.TypeText},
				{Name: "rolsuper", DataType: types.TypeBoolean},
				{Name: "rolinherit", DataType: types.TypeBoolean},
				{Name: "rolcreaterole", DataType: types.TypeBoolean},
				{Name: "rolcreatedb", DataType: types.TypeBoolean},
				{Name: "rolcanlogin", DataType: types.TypeBoolean},
			},
			Rows: []result.Row{
				{
					types.TextValue("oxen"),
					types.BoolValue(true),
					types.BoolValue(true),
					types.BoolValue(true),
					types.BoolValue(true),
					types.BoolValue(true),
				},
			},
			Tag: "SELECT 1",
		}
		return rs, true
	}

	// information_schema.tables
	if strings.Contains(lower, "information_schema.tables") {
		return c.handleInfoSchemaTables(), true
	}

	// information_schema.columns
	if strings.Contains(lower, "information_schema.columns") {
		return c.handleInfoSchemaColumns(), true
	}

	// information_schema.table_constraints
	if strings.Contains(lower, "information_schema") {
		rs := &result.ResultSet{
			Columns: []result.ColMeta{{Name: "result", DataType: types.TypeText}},
			Rows:    []result.Row{},
			Tag:     "SELECT 0",
		}
		return rs, true
	}

	return nil, false
}

// handlePgClass returns a result set simulating pg_catalog.pg_class for real tables.
func (c *Conn) handlePgClass() *result.ResultSet {
	rs := &result.ResultSet{
		Columns: []result.ColMeta{
			{Name: "oid", DataType: types.TypeInteger},
			{Name: "relname", DataType: types.TypeText},
			{Name: "relnamespace", DataType: types.TypeInteger},
			{Name: "relkind", DataType: types.TypeText},
			{Name: "relowner", DataType: types.TypeInteger},
			{Name: "relam", DataType: types.TypeInteger},
			{Name: "reltablespace", DataType: types.TypeInteger},
			{Name: "relhasrules", DataType: types.TypeBoolean},
			{Name: "relhastriggers", DataType: types.TypeBoolean},
			{Name: "relrowsecurity", DataType: types.TypeBoolean},
			{Name: "relforcerowsecurity", DataType: types.TypeBoolean},
			{Name: "relispartition", DataType: types.TypeBoolean},
			{Name: "relpersistence", DataType: types.TypeText},
			{Name: "relacl", DataType: types.TypeText},
		},
		Tag: "SELECT 0",
	}

	tables, err := c.engine.Catalog().ListTables()
	if err != nil {
		return rs
	}
	for _, td := range tables {
		rs.Rows = append(rs.Rows, result.Row{
			types.IntValue(int64(td.TableID)),
			types.TextValue(td.Name),
			types.IntValue(2200), // public namespace OID
			types.TextValue("r"), // r = ordinary table
			types.IntValue(10),   // owner OID
			types.IntValue(2),    // heap AM
			types.IntValue(0),    // default tablespace
			types.BoolValue(false),
			types.BoolValue(false),
			types.BoolValue(false),
			types.BoolValue(false),
			types.BoolValue(false),
			types.TextValue("p"), // permanent
			types.Null,           // relacl
		})
	}
	rs.Tag = fmt.Sprintf("SELECT %d", len(rs.Rows))
	return rs
}

// handlePgAttribute returns a result set simulating pg_catalog.pg_attribute.
func (c *Conn) handlePgAttribute() *result.ResultSet {
	rs := &result.ResultSet{
		Columns: []result.ColMeta{
			{Name: "attrelid", DataType: types.TypeInteger},
			{Name: "attname", DataType: types.TypeText},
			{Name: "atttypid", DataType: types.TypeInteger},
			{Name: "attstattarget", DataType: types.TypeInteger},
			{Name: "attlen", DataType: types.TypeInteger},
			{Name: "attnum", DataType: types.TypeInteger},
			{Name: "attndims", DataType: types.TypeInteger},
			{Name: "attnotnull", DataType: types.TypeBoolean},
			{Name: "atthasdef", DataType: types.TypeBoolean},
			{Name: "atthasmissing", DataType: types.TypeBoolean},
			{Name: "attidentity", DataType: types.TypeText},
			{Name: "attgenerated", DataType: types.TypeText},
			{Name: "attisdropped", DataType: types.TypeBoolean},
			{Name: "attislocal", DataType: types.TypeBoolean},
			{Name: "attinhcount", DataType: types.TypeInteger},
			{Name: "atttypmod", DataType: types.TypeInteger},
			{Name: "attacl", DataType: types.TypeText},
		},
		Tag: "SELECT 0",
	}

	tables, err := c.engine.Catalog().ListTables()
	if err != nil {
		return rs
	}
	for _, td := range tables {
		for _, col := range td.Columns {
			oid := uint32(25) // text OID default
			if o, ok := dtToOID[col.Type]; ok {
				oid = o
			}
			rs.Rows = append(rs.Rows, result.Row{
				types.IntValue(int64(td.TableID)),
				types.TextValue(col.Name),
				types.IntValue(int64(oid)),
				types.IntValue(-1),
				types.IntValue(-1),
				types.IntValue(int64(col.Ordinal + 1)),
				types.IntValue(0),
				types.BoolValue(!col.Nullable),
				types.BoolValue(col.HasDefault),
				types.BoolValue(false),
				types.TextValue(""),
				types.TextValue(""),
				types.BoolValue(false),
				types.BoolValue(true),
				types.IntValue(0),
				types.IntValue(-1),
				types.Null,
			})
		}
	}
	rs.Tag = fmt.Sprintf("SELECT %d", len(rs.Rows))
	return rs
}

// handleInfoSchemaTables returns information_schema.tables data for real tables.
func (c *Conn) handleInfoSchemaTables() *result.ResultSet {
	rs := &result.ResultSet{
		Columns: []result.ColMeta{
			{Name: "table_catalog", DataType: types.TypeText},
			{Name: "table_schema", DataType: types.TypeText},
			{Name: "table_name", DataType: types.TypeText},
			{Name: "table_type", DataType: types.TypeText},
		},
		Tag: "SELECT 0",
	}

	tables, err := c.engine.Catalog().ListTables()
	if err != nil {
		return rs
	}
	for _, td := range tables {
		rs.Rows = append(rs.Rows, result.Row{
			types.TextValue(c.session.Database),
			types.TextValue("public"),
			types.TextValue(td.Name),
			types.TextValue("BASE TABLE"),
		})
	}
	rs.Tag = fmt.Sprintf("SELECT %d", len(rs.Rows))
	return rs
}

// handleInfoSchemaColumns returns information_schema.columns data for real tables.
func (c *Conn) handleInfoSchemaColumns() *result.ResultSet {
	rs := &result.ResultSet{
		Columns: []result.ColMeta{
			{Name: "table_catalog", DataType: types.TypeText},
			{Name: "table_schema", DataType: types.TypeText},
			{Name: "table_name", DataType: types.TypeText},
			{Name: "column_name", DataType: types.TypeText},
			{Name: "ordinal_position", DataType: types.TypeInteger},
			{Name: "column_default", DataType: types.TypeText},
			{Name: "is_nullable", DataType: types.TypeText},
			{Name: "data_type", DataType: types.TypeText},
			{Name: "character_maximum_length", DataType: types.TypeInteger},
		},
		Tag: "SELECT 0",
	}

	tables, err := c.engine.Catalog().ListTables()
	if err != nil {
		return rs
	}
	for _, td := range tables {
		for _, col := range td.Columns {
			nullable := "YES"
			if !col.Nullable {
				nullable = "NO"
			}
			defaultVal := types.Null
			if col.HasDefault && col.DefaultValue != "" {
				defaultVal = types.TextValue(col.DefaultValue)
			}
			charMaxLen := types.Null
			if col.VarCharLen > 0 {
				charMaxLen = types.IntValue(int64(col.VarCharLen))
			}
			rs.Rows = append(rs.Rows, result.Row{
				types.TextValue(c.session.Database),
				types.TextValue("public"),
				types.TextValue(td.Name),
				types.TextValue(col.Name),
				types.IntValue(int64(col.Ordinal + 1)),
				defaultVal,
				types.TextValue(nullable),
				types.TextValue(strings.ToLower(col.Type.String())),
				charMaxLen,
			})
		}
	}
	rs.Tag = fmt.Sprintf("SELECT %d", len(rs.Rows))
	return rs
}

// describeStmt returns a RowDescription for the first SELECT plan in a prepared statement.
func (c *Conn) describeStmt(stmt *PreparedStatement) *pgproto3.RowDescription {
	for _, planIface := range stmt.Plans {
		if sp, ok := planIface.(*sqlpkg.SelectPlan); ok {
			// Try to get table metadata for column type info
			if sp.TableName != "" {
				td, err := c.engine.Catalog().GetTable(sp.TableName)
				if err == nil {
					cols := tableToColMeta(td)
					return buildRowDescription(cols)
				}
			}
			// Expression select — create generic columns
			var cols []result.ColMeta
			for _, ce := range sp.ColExprs {
				alias := ce.Alias
				if alias == "" {
					alias = "?column?"
				}
				cols = append(cols, result.ColMeta{Name: alias, DataType: types.TypeText, Nullable: true})
			}
			if len(cols) > 0 {
				return buildRowDescription(cols)
			}
		}
	}
	return nil
}

// describePortal returns a RowDescription for a portal.
func (c *Conn) describePortal(portal *Portal) *pgproto3.RowDescription {
	if portal.Rows != nil && len(portal.Rows.Columns) > 0 {
		return buildRowDescription(portal.Rows.Columns)
	}
	return c.describeStmt(portal.Stmt)
}

func tableToColMeta(td *catalog.TableDescriptor) []result.ColMeta {
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
	return cols
}

// ---- Wire encoding ----

// buildRowDescription converts column metadata to a pgproto3 RowDescription.
func buildRowDescription(cols []result.ColMeta) *pgproto3.RowDescription {
	fields := make([]pgproto3.FieldDescription, len(cols))
	for i, col := range cols {
		oid, ok := dtToOID[col.DataType]
		if !ok {
			oid = 25 // text
		}
		fields[i] = pgproto3.FieldDescription{
			Name:                 []byte(col.Name),
			TableOID:             col.TableID,
			TableAttributeNumber: uint16(col.ColIdx + 1),
			DataTypeOID:          oid,
			DataTypeSize:         -1, // variable length
			TypeModifier:         -1,
			Format:               0, // text format
		}
	}
	return &pgproto3.RowDescription{Fields: fields}
}

// buildDataRow converts a result row to a pgproto3 DataRow.
// All values are text-encoded (format 0).
func buildDataRow(row result.Row) *pgproto3.DataRow {
	values := make([][]byte, len(row))
	for i, v := range row {
		values[i] = valueToText(v)
	}
	return &pgproto3.DataRow{Values: values}
}

// valueToText converts a Value to its text representation.
// Returns nil for NULL (which pgproto3 encodes as -1 length).
func valueToText(v types.Value) []byte {
	if v.IsNull {
		return nil
	}
	switch v.Type {
	case types.TypeNull:
		return nil
	case types.TypeInteger, types.TypeBigInt:
		return []byte(strconv.FormatInt(v.IntVal, 10))
	case types.TypeFloat:
		return []byte(strconv.FormatFloat(v.FloatVal, 'f', -1, 32))
	case types.TypeDouble:
		return []byte(strconv.FormatFloat(v.FloatVal, 'f', -1, 64))
	case types.TypeText, types.TypeVarChar:
		return []byte(v.StrVal)
	case types.TypeBoolean:
		if v.BoolVal {
			return []byte("t")
		}
		return []byte("f")
	case types.TypeTimestamp:
		return []byte(v.TimeVal.UTC().Format("2006-01-02 15:04:05.999999999+00"))
	default:
		return []byte(fmt.Sprintf("%v", v.NativeValue()))
	}
}

// ---- Quick result constructors ----

func oneColResult(name string, dt types.DataType, val types.Value) *result.ResultSet {
	return &result.ResultSet{
		Columns: []result.ColMeta{{Name: name, DataType: dt}},
		Rows:    []result.Row{{val}},
		Tag:     "SELECT 1",
	}
}

func oneTextResult(name, text string) *result.ResultSet {
	return oneColResult(name, types.TypeText, types.TextValue(text))
}

// timeVal is a helper used for timestamps.
var _ = time.Now
