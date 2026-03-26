// Package api implements the HTTP REST API for OxenDB.
package api

import (
	"encoding/json"
	"net/http"
)

// envelope wraps every JSON response with a status field.
type envelope struct {
	Status  string      `json:"status"`
	Message string      `json:"message,omitempty"`
	Data    interface{} `json:"data,omitempty"`
}

// errorEnvelope is the JSON body for error responses.
type errorEnvelope struct {
	Status  string `json:"status"`
	Code    string `json:"code"`
	Message string `json:"message"`
}

// JSON writes a success response with HTTP 200.
func JSON(w http.ResponseWriter, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(envelope{Status: "ok", Data: data})
}

// JSONCreated writes a success response with HTTP 201.
func JSONCreated(w http.ResponseWriter, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(envelope{Status: "ok", Data: data})
}

// JSONNoContent writes HTTP 204 (no body).
func JSONNoContent(w http.ResponseWriter) {
	w.WriteHeader(http.StatusNoContent)
}

// JSONMessage writes a success response that carries only a text message.
func JSONMessage(w http.ResponseWriter, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(envelope{Status: "ok", Message: msg})
}

// Error writes a JSON error response.
func Error(w http.ResponseWriter, status int, code, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(errorEnvelope{
		Status:  "error",
		Code:    code,
		Message: message,
	})
}

// ErrBadRequest writes HTTP 400.
func ErrBadRequest(w http.ResponseWriter, msg string) {
	Error(w, http.StatusBadRequest, "bad_request", msg)
}

// ErrNotFound writes HTTP 404.
func ErrNotFound(w http.ResponseWriter, msg string) {
	Error(w, http.StatusNotFound, "not_found", msg)
}

// ErrUnauthorized writes HTTP 401.
func ErrUnauthorized(w http.ResponseWriter) {
	Error(w, http.StatusUnauthorized, "unauthorized", "valid bearer token required")
}

// ErrInternal writes HTTP 500.
func ErrInternal(w http.ResponseWriter, msg string) {
	Error(w, http.StatusInternalServerError, "internal_error", msg)
}

// ErrMethodNotAllowed writes HTTP 405.
func ErrMethodNotAllowed(w http.ResponseWriter) {
	Error(w, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
}
