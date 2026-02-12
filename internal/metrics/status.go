package metrics

type Status struct {
	FsmMetrics     *FsmMetrics     `json:"fsm"`
	NodeMetrics    *NodeMetrics    `json:"node"`
	StorageMetrics *StorageMetrics `json:"storage"`
}
