package elastic

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
)

// Error encapsulates error details as returned from Elasticsearch.
type Error struct {
	Status  int           `json:"status"`
	Details *ErrorDetails `json:"error,omitempty"`
}

// Error returns a string representation of the error.
func (e *Error) Error() string {
	if e.Details != nil && e.Details.Reason != "" {
		return fmt.Sprintf("elastic: Error %d (%s): %s [type=%s]", e.Status, http.StatusText(e.Status), e.Details.Reason, e.Details.Type)
	}
	return fmt.Sprintf("elastic: Error %d (%s)", e.Status, http.StatusText(e.Status))
}

// ErrorDetails encapsulate error details from Elasticsearch.
// It is used in e.g. elastic.Error and elastic.BulkResponseItem.
type ErrorDetails struct {
	Type         string                   `json:"type"`
	Reason       string                   `json:"reason"`
	ResourceType string                   `json:"resource.type,omitempty"`
	ResourceId   string                   `json:"resource.id,omitempty"`
	Index        string                   `json:"index,omitempty"`
	Phase        string                   `json:"phase,omitempty"`
	Grouped      bool                     `json:"grouped,omitempty"`
	CausedBy     map[string]interface{}   `json:"caused_by,omitempty"`
	RootCause    []*ErrorDetails          `json:"root_cause,omitempty"`
	Suppressed   []*ErrorDetails          `json:"suppressed,omitempty"`
	FailedShards []map[string]interface{} `json:"failed_shards,omitempty"`
	Header       map[string]interface{}   `json:"header,omitempty"`

	// ScriptException adds the information in the following block.

	ScriptStack []string             `json:"script_stack,omitempty"` // from ScriptException
	Script      string               `json:"script,omitempty"`       // from ScriptException
	Lang        string               `json:"lang,omitempty"`         // from ScriptException
	Position    *ScriptErrorPosition `json:"position,omitempty"`     // from ScriptException (7.7+)
}

// ScriptErrorPosition specifies the position of the error
// in a script. It is used in ErrorDetails for scripting errors.
type ScriptErrorPosition struct {
	Offset int `json:"offset"`
	Start  int `json:"start"`
	End    int `json:"end"`
}

// IsContextErr returns true if the error is from a context that was canceled or deadline exceeded
func IsContextErr(err error) bool {
	if err == context.Canceled || err == context.DeadlineExceeded {
		return true
	}
	// This happens e.g. on redirect errors, see https://golang.org/src/net/http/client_test.go#L329
	if ue, ok := err.(*url.Error); ok {
		if ue.Temporary() {
			return true
		}
		// Use of an AWS Signing Transport can result in a wrapped url.Error
		return IsContextErr(ue.Err)
	}
	return false
}

// IsConnErr returns true if the error indicates that Elastic could not
// find an Elasticsearch host to connect to.
//func IsConnErr(err error) bool {
//	return err == ErrNoClient || errors.Cause(err) == ErrNoClient
//}

// IsNotFound returns true if the given error indicates that Elasticsearch
// returned HTTP status 404. The err parameter can be of type *elastic.Error,
// elastic.Error, *http.Response or int (indicating the HTTP status code).
func IsNotFound(err interface{}) bool {
	return IsStatusCode(err, http.StatusNotFound)
}

// IsTimeout returns true if the given error indicates that Elasticsearch
// returned HTTP status 408. The err parameter can be of type *elastic.Error,
// elastic.Error, *http.Response or int (indicating the HTTP status code).
func IsTimeout(err interface{}) bool {
	return IsStatusCode(err, http.StatusRequestTimeout)
}

// IsConflict returns true if the given error indicates that the Elasticsearch
// operation resulted in a version conflict. This can occur in operations like
// `update` or `index` with `op_type=create`. The err parameter can be of
// type *elastic.Error, elastic.Error, *http.Response or int (indicating the
// HTTP status code).
func IsConflict(err interface{}) bool {
	return IsStatusCode(err, http.StatusConflict)
}

// IsUnauthorized returns true if the given error indicates that
// Elasticsearch returned HTTP status 401. This happens e.g. when the
// cluster is configured to require HTTP Basic Auth.
// The err parameter can be of type *elastic.Error, elastic.Error,
// *http.Response or int (indicating the HTTP status code).
func IsUnauthorized(err interface{}) bool {
	return IsStatusCode(err, http.StatusUnauthorized)
}

// IsForbidden returns true if the given error indicates that Elasticsearch
// returned HTTP status 403. This happens e.g. due to a missing license.
// The err parameter can be of type *elastic.Error, elastic.Error,
// *http.Response or int (indicating the HTTP status code).
func IsForbidden(err interface{}) bool {
	return IsStatusCode(err, http.StatusForbidden)
}

// IsStatusCode returns true if the given error indicates that the Elasticsearch
// operation returned the specified HTTP status code. The err parameter can be of
// type *http.Response, *Error, Error, or int (indicating the HTTP status code).
func IsStatusCode(err interface{}, code int) bool {
	switch e := err.(type) {
	case *http.Response:
		return e.StatusCode == code
	case *Error:
		return e.Status == code
	case Error:
		return e.Status == code
	case int:
		return e == code
	}
	return false
}

// -- General errors --

// ShardsInfo represents information from a shard.
type ShardsInfo struct {
	Total      int                              `json:"total"`
	Successful int                              `json:"successful"`
	Failed     int                              `json:"failed"`
	Failures   []*ShardOperationFailedException `json:"failures,omitempty"`
	Skipped    int                              `json:"skipped,omitempty"`
}

type ShardOperationFailedException struct {
	Shard  int                    `json:"shard,omitempty"`
	Index  string                 `json:"index,omitempty"`
	Status string                 `json:"status,omitempty"`
	Reason map[string]interface{} `json:"reason,omitempty"`
}

type BroadcastResponse struct {
	Shards     *ShardsInfo                      `json:"_shards,omitempty"`
	Total      int                              `json:"total"`
	Successful int                              `json:"successful"`
	Failed     int                              `json:"failed"`
	Failures   []*ShardOperationFailedException `json:"failures,omitempty"`
}

// FailedNodeException returns an error on the node level.
type FailedNodeException struct {
	*ErrorDetails
	NodeId string `json:"node_id"`
}
