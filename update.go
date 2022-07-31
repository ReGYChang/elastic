package elasticsearch

// UpdateResponse is the result of updating a document in Elasticsearch.
type UpdateResponse struct {
	Index         string      `json:"_index,omitempty"`
	Type          string      `json:"_type,omitempty"`
	Id            string      `json:"_id,omitempty"`
	Version       int64       `json:"_version,omitempty"`
	Result        string      `json:"result,omitempty"`
	Shards        *ShardsInfo `json:"_shards,omitempty"`
	SeqNo         int64       `json:"_seq_no,omitempty"`
	PrimaryTerm   int64       `json:"_primary_term,omitempty"`
	Status        int         `json:"status,omitempty"`
	ForcedRefresh bool        `json:"forced_refresh,omitempty"`
	//GetResult     *GetResult  `json:"get,omitempty"`
}
