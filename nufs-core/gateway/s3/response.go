// Package s3 implements an S3-compatible HTTP gateway for the DFS distributed storage system.
package s3

import (
	"encoding/xml"
	"fmt"
	"net/http"
	"time"
)

// ========== S3 XML Response Types ==========

// ErrorResponse represents an S3 error response.
type ErrorResponse struct {
	XMLName   xml.Name `xml:"Error"`
	Code      string   `xml:"Code"`
	Message   string   `xml:"Message"`
	Resource  string   `xml:"Resource,omitempty"`
	RequestID string   `xml:"RequestId,omitempty"`
}

// ListAllMyBucketsResult represents the ListBuckets response.
type ListAllMyBucketsResult struct {
	XMLName xml.Name      `xml:"ListAllMyBucketsResult"`
	Owner   Owner         `xml:"Owner"`
	Buckets []BucketEntry `xml:"Buckets>Bucket"`
}

// Owner represents the S3 bucket/object owner.
type Owner struct {
	ID          string `xml:"ID"`
	DisplayName string `xml:"DisplayName"`
}

// BucketEntry represents a bucket in the ListBuckets response.
type BucketEntry struct {
	Name         string `xml:"Name"`
	CreationDate string `xml:"CreationDate"`
}

// ListBucketResult represents the ListObjects/ListObjectsV2 response.
type ListBucketResult struct {
	XMLName        xml.Name       `xml:"ListBucketResult"`
	Name           string         `xml:"Name"`
	Prefix         string         `xml:"Prefix"`
	Marker         string         `xml:"Marker,omitempty"`
	NextMarker     string         `xml:"NextMarker,omitempty"`
	MaxKeys        int            `xml:"MaxKeys"`
	IsTruncated    bool           `xml:"IsTruncated"`
	Contents       []ObjectEntry  `xml:"Contents,omitempty"`
	CommonPrefixes []CommonPrefix `xml:"CommonPrefixes,omitempty"`
	// V2 fields
	ContinuationToken     string `xml:"ContinuationToken,omitempty"`
	NextContinuationToken string `xml:"NextContinuationToken,omitempty"`
	KeyCount              int    `xml:"KeyCount,omitempty"`
}

// ObjectEntry represents an object in the ListObjects response.
type ObjectEntry struct {
	Key          string `xml:"Key"`
	LastModified string `xml:"LastModified"`
	ETag         string `xml:"ETag"`
	Size         int64  `xml:"Size"`
	StorageClass string `xml:"StorageClass"`
	Owner        *Owner `xml:"Owner,omitempty"`
}

// CommonPrefix represents a common prefix (virtual directory) in ListObjects.
type CommonPrefix struct {
	Prefix string `xml:"Prefix"`
}

// CopyObjectResult represents the CopyObject response.
type CopyObjectResult struct {
	XMLName      xml.Name `xml:"CopyObjectResult"`
	LastModified string   `xml:"LastModified"`
	ETag         string   `xml:"ETag"`
}

// InitiateMultipartUploadResult represents the InitiateMultipartUpload response.
type InitiateMultipartUploadResult struct {
	XMLName  xml.Name `xml:"InitiateMultipartUploadResult"`
	Bucket   string   `xml:"Bucket"`
	Key      string   `xml:"Key"`
	UploadID string   `xml:"UploadId"`
}

// CompleteMultipartUploadResult represents the CompleteMultipartUpload response.
type CompleteMultipartUploadResult struct {
	XMLName  xml.Name `xml:"CompleteMultipartUploadResult"`
	Location string   `xml:"Location"`
	Bucket   string   `xml:"Bucket"`
	Key      string   `xml:"Key"`
	ETag     string   `xml:"ETag"`
}

// CompleteMultipartUpload is the request body for CompleteMultipartUpload.
type CompleteMultipartUpload struct {
	XMLName xml.Name       `xml:"CompleteMultipartUpload"`
	Parts   []CompletePart `xml:"Part"`
}

// CompletePart represents a part in the CompleteMultipartUpload request.
type CompletePart struct {
	PartNumber int    `xml:"PartNumber"`
	ETag       string `xml:"ETag"`
}

// ListPartsResult represents the ListParts response.
type ListPartsResult struct {
	XMLName     xml.Name    `xml:"ListPartsResult"`
	Bucket      string      `xml:"Bucket"`
	Key         string      `xml:"Key"`
	UploadID    string      `xml:"UploadId"`
	MaxParts    int         `xml:"MaxParts"`
	IsTruncated bool        `xml:"IsTruncated"`
	Parts       []PartEntry `xml:"Part,omitempty"`
}

// PartEntry represents a part in the ListParts response.
type PartEntry struct {
	PartNumber   int    `xml:"PartNumber"`
	LastModified string `xml:"LastModified"`
	ETag         string `xml:"ETag"`
	Size         int64  `xml:"Size"`
}

// ========== S3 Error Codes ==========

const (
	ErrCodeNoSuchBucket        = "NoSuchBucket"
	ErrCodeBucketAlreadyExists = "BucketAlreadyExists"
	ErrCodeBucketNotEmpty      = "BucketNotEmpty"
	ErrCodeNoSuchKey           = "NoSuchKey"
	ErrCodeInvalidArgument     = "InvalidArgument"
	ErrCodeInvalidRequest      = "InvalidRequest"
	ErrCodeInternalError       = "InternalError"
	ErrCodeAccessDenied        = "AccessDenied"
	ErrCodeNoSuchUpload        = "NoSuchUpload"
	ErrCodeInvalidPart         = "InvalidPart"
	ErrCodeMethodNotAllowed    = "MethodNotAllowed"
	ErrCodeNotImplemented      = "NotImplemented"
	ErrCodeEntityTooLarge      = "EntityTooLarge"
	ErrCodeServiceUnavailable  = "ServiceUnavailable"
	ErrCodeSlowDown            = "SlowDown"
)

// ========== Response Helpers ==========

const (
	// S3TimeFormat is the ISO 8601 format used for S3 timestamps.
	S3TimeFormat = "2006-01-02T15:04:05.000Z"
	// DefaultMaxKeys is the default maximum number of keys returned by ListObjects.
	DefaultMaxKeys = 1000
)

// WriteXMLError writes an S3-style XML error response.
func WriteXMLError(w http.ResponseWriter, statusCode int, code, message, resource, requestID string) {
	w.Header().Set("Content-Type", "application/xml")
	w.Header().Set("x-amz-request-id", requestID)
	w.WriteHeader(statusCode)

	resp := ErrorResponse{
		Code:      code,
		Message:   message,
		Resource:  resource,
		RequestID: requestID,
	}
	data, _ := xml.Marshal(resp)
	w.Write([]byte(xml.Header))
	w.Write(data)
}

// WriteXML writes an S3-style XML success response.
func WriteXML(w http.ResponseWriter, statusCode int, v interface{}) {
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(statusCode)
	data, err := xml.Marshal(v)
	if err != nil {
		WriteXMLError(w, http.StatusInternalServerError, ErrCodeInternalError,
			"Failed to marshal response", "", "")
		return
	}
	w.Write([]byte(xml.Header))
	w.Write(data)
}

// FormatS3Time formats a time.Time to S3 timestamp format.
func FormatS3Time(t time.Time) string {
	return t.UTC().Format(S3TimeFormat)
}

// FormatETag formats a checksum as an S3 ETag (quoted hex).
func FormatETag(checksum uint32) string {
	return fmt.Sprintf("\"%08x\"", checksum)
}

// StatusForS3Error maps S3 error codes to HTTP status codes.
func StatusForS3Error(code string) int {
	switch code {
	case ErrCodeNoSuchBucket, ErrCodeNoSuchKey, ErrCodeNoSuchUpload:
		return http.StatusNotFound
	case ErrCodeBucketAlreadyExists:
		return http.StatusConflict
	case ErrCodeBucketNotEmpty:
		return http.StatusConflict
	case ErrCodeAccessDenied:
		return http.StatusForbidden
	case ErrCodeInvalidArgument, ErrCodeInvalidRequest, ErrCodeInvalidPart:
		return http.StatusBadRequest
	case ErrCodeMethodNotAllowed:
		return http.StatusMethodNotAllowed
	case ErrCodeNotImplemented:
		return http.StatusNotImplemented
	default:
		return http.StatusInternalServerError
	}
}
