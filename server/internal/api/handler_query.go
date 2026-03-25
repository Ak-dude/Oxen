package api

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"

	"oxendb/server/internal/bridge"
	"oxendb/server/internal/metrics"
	"oxendb/server/internal/query"
)

// queryRequest is the JSON body for POST /v1/query.
type queryRequest struct {
	Query string `json:"query"`
}

// queryResponse is the JSON body for a successful query response.
type queryResponse struct {
	Message string        `json:"message,omitempty"`
	Key     string        `json:"key,omitempty"`
	Value   string        `json:"value,omitempty"` // base64
	Pairs   []kvPairResp  `json:"pairs,omitempty"`
}

type kvPairResp struct {
	Key   string `json:"key"`
	Value string `json:"value"` // base64
}

// QueryHandler handles POST /v1/query.
type QueryHandler struct {
	db *bridge.DB
}

// NewQueryHandler constructs a QueryHandler.
func NewQueryHandler(db *bridge.DB) *QueryHandler {
	return &QueryHandler{db: db}
}

// Handle handles POST /v1/query — parses and executes an OxenQL statement.
func (h *QueryHandler) Handle(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(io.LimitReader(r.Body, 1*1024*1024)) // 1 MiB max query size
	if err != nil {
		ErrBadRequest(w, "could not read request body")
		return
	}
	defer r.Body.Close()

	var req queryRequest
	if err := json.Unmarshal(body, &req); err != nil {
		ErrBadRequest(w, "invalid JSON: "+err.Error())
		return
	}
	if req.Query == "" {
		ErrBadRequest(w, "query field is required")
		return
	}

	stmt, err := query.ParseQuery(req.Query)
	if err != nil {
		ErrBadRequest(w, "parse error: "+err.Error())
		return
	}

	result, err := query.Execute(stmt, h.db)
	if err != nil {
		var nfe *query.NotFoundError
		if errors.As(err, &nfe) {
			metrics.RecordOp("query", "not_found")
			ErrNotFound(w, err.Error())
			return
		}
		metrics.RecordOp("query", "error")
		ErrInternal(w, err.Error())
		return
	}

	metrics.RecordOp("query", "ok")

	resp := queryResponse{Message: result.Message}
	if result.Key != nil {
		resp.Key = string(result.Key)
	}
	if result.Value != nil {
		resp.Value = base64.StdEncoding.EncodeToString(result.Value)
	}
	for _, p := range result.Pairs {
		resp.Pairs = append(resp.Pairs, kvPairResp{
			Key:   string(p[0]),
			Value: base64.StdEncoding.EncodeToString(p[1]),
		})
	}

	JSON(w, resp)
}
