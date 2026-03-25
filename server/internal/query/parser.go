package query

import (
	"fmt"
	"strconv"
)

// Parser is a recursive-descent parser for OxenQL.
type Parser struct {
	tokens []Token
	pos    int
}

// NewParser creates a Parser from a slice of tokens (as produced by Lexer.All).
func NewParser(tokens []Token) *Parser {
	return &Parser{tokens: tokens}
}

// Parse parses the token stream and returns a Statement.
func (p *Parser) Parse() (Statement, error) {
	tok := p.peek()
	switch tok.Type {
	case TokenGET:
		return p.parseGet()
	case TokenPUT:
		return p.parsePut()
	case TokenDELETE:
		return p.parseDelete()
	case TokenSCAN:
		return p.parseScan()
	case TokenBATCH:
		return p.parseBatch()
	case TokenEOF:
		return nil, fmt.Errorf("parser: empty query")
	default:
		return nil, fmt.Errorf("parser: unexpected token %q at position %d", tok.Literal, p.pos)
	}
}

// ParseQuery is the top-level entry point: lex + parse in one call.
func ParseQuery(input string) (Statement, error) {
	lexer := NewLexer(input)
	tokens, err := lexer.All()
	if err != nil {
		return nil, err
	}
	return NewParser(tokens).Parse()
}

// ---- statement parsers ----

func (p *Parser) parseGet() (*GetStmt, error) {
	p.consume() // GET
	key, err := p.expectString("GET requires a key")
	if err != nil {
		return nil, err
	}
	return &GetStmt{Key: []byte(key)}, nil
}

func (p *Parser) parsePut() (*PutStmt, error) {
	p.consume() // PUT
	key, err := p.expectString("PUT requires a key")
	if err != nil {
		return nil, err
	}
	value, err := p.expectString("PUT requires a value")
	if err != nil {
		return nil, err
	}
	return &PutStmt{Key: []byte(key), Value: []byte(value)}, nil
}

func (p *Parser) parseDelete() (*DeleteStmt, error) {
	p.consume() // DELETE
	key, err := p.expectString("DELETE requires a key")
	if err != nil {
		return nil, err
	}
	return &DeleteStmt{Key: []byte(key)}, nil
}

func (p *Parser) parseScan() (*ScanStmt, error) {
	p.consume() // SCAN
	stmt := &ScanStmt{}

	for {
		tok := p.peek()
		switch tok.Type {
		case TokenFROM:
			p.consume()
			k, err := p.expectString("SCAN FROM requires a key")
			if err != nil {
				return nil, err
			}
			stmt.From = []byte(k)
		case TokenTO:
			p.consume()
			k, err := p.expectString("SCAN TO requires a key")
			if err != nil {
				return nil, err
			}
			stmt.To = []byte(k)
		case TokenLIMIT:
			p.consume()
			n, err := p.expectNumber("SCAN LIMIT requires a number")
			if err != nil {
				return nil, err
			}
			stmt.Limit = n
		default:
			return stmt, nil
		}
	}
}

func (p *Parser) parseBatch() (*BatchStmt, error) {
	p.consume() // BATCH

	if err := p.expect(TokenLBrace, "BATCH requires '{'"); err != nil {
		return nil, err
	}

	stmt := &BatchStmt{}
	for p.peek().Type != TokenRBrace && p.peek().Type != TokenEOF {
		tok := p.peek()
		switch tok.Type {
		case TokenPUT:
			p.consume()
			key, err := p.expectString("BATCH PUT requires a key")
			if err != nil {
				return nil, err
			}
			value, err := p.expectString("BATCH PUT requires a value")
			if err != nil {
				return nil, err
			}
			stmt.Ops = append(stmt.Ops, BatchOp{Op: "PUT", Key: []byte(key), Value: []byte(value)})
		case TokenDELETE:
			p.consume()
			key, err := p.expectString("BATCH DELETE requires a key")
			if err != nil {
				return nil, err
			}
			stmt.Ops = append(stmt.Ops, BatchOp{Op: "DELETE", Key: []byte(key)})
		default:
			return nil, fmt.Errorf("parser: unexpected token %q inside BATCH block", tok.Literal)
		}
	}

	if err := p.expect(TokenRBrace, "BATCH requires closing '}'"); err != nil {
		return nil, err
	}

	if len(stmt.Ops) == 0 {
		return nil, fmt.Errorf("parser: BATCH block is empty")
	}
	return stmt, nil
}

// ---- helpers ----

func (p *Parser) peek() Token {
	if p.pos >= len(p.tokens) {
		return Token{Type: TokenEOF}
	}
	return p.tokens[p.pos]
}

func (p *Parser) consume() Token {
	tok := p.peek()
	if p.pos < len(p.tokens) {
		p.pos++
	}
	return tok
}

func (p *Parser) expect(tt TokenType, msg string) error {
	tok := p.peek()
	if tok.Type != tt {
		return fmt.Errorf("parser: %s (got %q)", msg, tok.Literal)
	}
	p.consume()
	return nil
}

func (p *Parser) expectString(msg string) (string, error) {
	tok := p.peek()
	if tok.Type != TokenString {
		return "", fmt.Errorf("parser: %s (got %q)", msg, tok.Literal)
	}
	p.consume()
	return tok.Literal, nil
}

func (p *Parser) expectNumber(msg string) (int, error) {
	tok := p.peek()
	if tok.Type != TokenNumber {
		return 0, fmt.Errorf("parser: %s (got %q)", msg, tok.Literal)
	}
	p.consume()
	n, err := strconv.Atoi(tok.Literal)
	if err != nil {
		return 0, fmt.Errorf("parser: %s: %w", msg, err)
	}
	return n, nil
}
