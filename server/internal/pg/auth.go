package pg

import (
	"fmt"

	"github.com/jackc/pgproto3/v2"
)

// runStartup performs the PostgreSQL startup handshake:
//  1. Receive startup message (handles SSL downgrade)
//  2. Authenticate according to cfg.PG.AuthMode
//  3. Send server parameters and BackendKeyData
//  4. Send initial ReadyForQuery
func (c *Conn) runStartup() error {
	// Receive the initial startup message
	startupMsg, err := c.be.ReceiveStartupMessage()
	if err != nil {
		return fmt.Errorf("startup: receive: %w", err)
	}

	// Handle SSL negotiation — client may ask for SSL first
	for {
		switch msg := startupMsg.(type) {
		case *pgproto3.SSLRequest:
			// Send 'N' to decline SSL, then receive the real startup message
			if _, err := c.net.Write([]byte{'N'}); err != nil {
				return fmt.Errorf("startup: ssl decline: %w", err)
			}
			startupMsg, err = c.be.ReceiveStartupMessage()
			if err != nil {
				return fmt.Errorf("startup: receive after ssl: %w", err)
			}
			continue

		case *pgproto3.GSSEncRequest:
			// Decline GSS encryption too
			if _, err := c.net.Write([]byte{'N'}); err != nil {
				return fmt.Errorf("startup: gss decline: %w", err)
			}
			startupMsg, err = c.be.ReceiveStartupMessage()
			if err != nil {
				return fmt.Errorf("startup: receive after gss: %w", err)
			}
			continue

		case *pgproto3.StartupMessage:
			// Extract parameters
			if user, ok := msg.Parameters["user"]; ok {
				c.session.User = user
			}
			if db, ok := msg.Parameters["database"]; ok {
				c.session.Database = db
			} else {
				c.session.Database = c.session.User
			}
			if app, ok := msg.Parameters["application_name"]; ok {
				c.session.AppName = app
			}
			goto doAuth

		default:
			return fmt.Errorf("startup: unexpected message type %T", startupMsg)
		}
	}

doAuth:
	// Authenticate
	authMode := c.cfg.PG.AuthMode
	if authMode == "" {
		authMode = "trust"
	}

	switch authMode {
	case "trust":
		if err := c.be.Send(&pgproto3.AuthenticationOk{}); err != nil {
			return err
		}

	case "cleartext":
		// Tell backend we'll use cleartext auth so it routes PasswordMessage correctly
		if err := c.be.SetAuthType(pgproto3.AuthTypeCleartextPassword); err != nil {
			return err
		}
		if err := c.be.Send(&pgproto3.AuthenticationCleartextPassword{}); err != nil {
			return err
		}
		// Receive password
		pwMsg, err := c.be.Receive()
		if err != nil {
			return fmt.Errorf("auth: receive password: %w", err)
		}
		pw, ok := pwMsg.(*pgproto3.PasswordMessage)
		if !ok {
			return fmt.Errorf("auth: expected PasswordMessage, got %T", pwMsg)
		}
		// Validate against configured users
		if expected, exists := c.cfg.PG.Users[c.session.User]; exists {
			if pw.Password != expected {
				_ = c.be.Send(&pgproto3.ErrorResponse{
					Severity: "FATAL",
					Code:     "28P01",
					Message:  "password authentication failed for user \"" + c.session.User + "\"",
				})
				return fmt.Errorf("auth: bad password for user %q", c.session.User)
			}
		}
		if err := c.be.Send(&pgproto3.AuthenticationOk{}); err != nil {
			return err
		}

	default:
		// Default to trust
		if err := c.be.Send(&pgproto3.AuthenticationOk{}); err != nil {
			return err
		}
	}

	// Send server parameter status messages that clients expect
	params := [][2]string{
		{"server_version", "14.0"},
		{"client_encoding", "UTF8"},
		{"server_encoding", "UTF8"},
		{"DateStyle", "ISO, MDY"},
		{"TimeZone", "UTC"},
		{"integer_datetimes", "on"},
		{"standard_conforming_strings", "on"},
		{"is_superuser", "on"},
		{"session_authorization", c.session.User},
		{"IntervalStyle", "postgres"},
	}
	for _, p := range params {
		if err := c.be.Send(&pgproto3.ParameterStatus{Name: p[0], Value: p[1]}); err != nil {
			return err
		}
	}

	// BackendKeyData
	if err := c.be.Send(&pgproto3.BackendKeyData{
		ProcessID: uint32(c.session.PID),
		SecretKey: uint32(c.session.SecretKey),
	}); err != nil {
		return err
	}

	// Initial ReadyForQuery
	return c.be.Send(&pgproto3.ReadyForQuery{TxStatus: c.session.TxStatusByte()})
}
