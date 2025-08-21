package tasks

// ข้อมูลของแต่ละ item ที่อยู่ใน order
type OrderItem struct {
	ProductID int64 `json:"product_id"`
	Quantity  int64 `json:"quantity"`
}

// Payload ที่ใช้ส่งเข้า queue
type DeductStockPayload struct {
	TenantID    int64       `json:"tenant_id"`
	WarehouseID int64       `json:"warehouse_id"`
	Items       []OrderItem `json:"items"`
}

// Request body ที่ client จะส่งเข้ามาที่ API
type OrderRequest struct {
	WarehouseID int64       `json:"warehouse_id"`
	Items       []OrderItem `json:"items"`
}
