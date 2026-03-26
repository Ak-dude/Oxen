package query

import (
	"fmt"
	"strings"
)

// TokenType enumerates the kinds of tokens the lexer produces.
type TokenType int

const (
	// Keywords
	TokenGET TokenType = iota + 1
	TokenPUT
	TokenDELETE
	TokenSCAN
	TokenBATCH
	TokenFROM
	TokenTO
	TokenLIMIT

	// Literals / punctuation
	TokenString  // quoted string or bare word
	TokenNumber  // integer literal
	TokenLBrace  // {
	TokenRBrace  // }
	TokenEOF
)

// Token is one lexical unit.
type Token struct {
	Type    TokenType
	Literal string // raw text (without quotes for strings)
}

var keywords = map[string]TokenType{
	"GET":    TokenGET,
	"PUT":    TokenPUT,
	"DELETE": TokenDELETE,
	"SCAN":   TokenSCAN,
	"BATCH":  TokenBATCH,
	"FROM":   TokenFROM,
	"TO":     TokenTO,
	"LIMIT":  TokenLIMIT,
}

// Lexer tokenises an OxenQL input string.
type Lexer struct {
	input []rune
	pos   int
}

// NewLexer creates a Lexer for the given input.
func NewLexer(input string) *Lexer {
	return &Lexer{input: []rune(strings.TrimSpace(input))}
}

// All returns all tokens from the input, stopping at EOF.
func (l *Lexer) All() ([]Token, error) {
	var tokens []Token
	for {
		tok, err := l.Next()
		if err != nil {
			return nil, err
		}
		tokens = append(tokens, tok)
		if tok.Type == TokenEOF {
			break
		}
	}
	return tokens, nil
}

// Next returns the next token.
func (l *Lexer) Next() (Token, error) {
	l.skipWhitespace()

	if l.pos >= len(l.input) {
		return Token{Type: TokenEOF}, nil
	}

	ch := l.input[l.pos]

	switch {
	case ch == '{':
		l.pos++
		return Token{Type: TokenLBrace, Literal: "{"}, nil
	case ch == '}':
		l.pos++
		return Token{Type: TokenRBrace, Literal: "}"}, nil
	case ch == '"' || ch == '\'':
		return l.readQuoted(ch)
	case isDigit(ch) || (ch == '-' && l.pos+1 < len(l.input) && isDigit(l.input[l.pos+1])):
		return l.readNumber()
	case isAlpha(ch) || ch == '_' || ch == '/' || ch == '.' || ch == '-':
		return l.readWord()
	default:
		return Token{}, fmt.Errorf("lexer: unexpected character %q at position %d", string(ch), l.pos)
	}
}

func (l *Lexer) skipWhitespace() {
	for l.pos < len(l.input) && isSpace(l.input[l.pos]) {
		l.pos++
	}
}

func (l *Lexer) readQuoted(delim rune) (Token, error) {
	l.pos++ // skip opening quote
	var sb strings.Builder
	for l.pos < len(l.input) {
		ch := l.input[l.pos]
		if ch == delim {
			l.pos++
			return Token{Type: TokenString, Literal: sb.String()}, nil
		}
		if ch == '\\' && l.pos+1 < len(l.input) {
			l.pos++
			escaped := l.input[l.pos]
			switch escaped {
			case 'n':
				sb.WriteRune('\n')
			case 't':
				sb.WriteRune('\t')
			case '\\':
				sb.WriteRune('\\')
			default:
				sb.WriteRune(escaped)
			}
			l.pos++
			continue
		}
		sb.WriteRune(ch)
		l.pos++
	}
	return Token{}, fmt.Errorf("lexer: unterminated string literal")
}

func (l *Lexer) readNumber() (Token, error) {
	start := l.pos
	if l.input[l.pos] == '-' {
		l.pos++
	}
	for l.pos < len(l.input) && isDigit(l.input[l.pos]) {
		l.pos++
	}
	return Token{Type: TokenNumber, Literal: string(l.input[start:l.pos])}, nil
}

func (l *Lexer) readWord() (Token, error) {
	start := l.pos
	for l.pos < len(l.input) && isWordChar(l.input[l.pos]) {
		l.pos++
	}
	word := string(l.input[start:l.pos])
	upper := strings.ToUpper(word)
	if tt, ok := keywords[upper]; ok {
		return Token{Type: tt, Literal: upper}, nil
	}
	return Token{Type: TokenString, Literal: word}, nil
}

func isSpace(ch rune) bool  { return ch == ' ' || ch == '\t' || ch == '\n' || ch == '\r' }
func isDigit(ch rune) bool  { return ch >= '0' && ch <= '9' }
func isAlpha(ch rune) bool  { return (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') }
func isWordChar(ch rune) bool {
	return isAlpha(ch) || isDigit(ch) || ch == '_' || ch == '-' || ch == '.' || ch == '/' || ch == ':'
}
