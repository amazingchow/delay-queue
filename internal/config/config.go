package config

type DelayQueue struct {
	GRPCEndpoint string `json:"grpc_endpoint"` // grpc服务地址
	Backend
}

type Backend struct {
	SentinelEndpoints   []string `json:"sentinel_endpoints"`
	SentinelPassword    string   `json:"sentinel_password"`
	RedisPoolMaxIdle    int      `json:"redis_pool_max_idle"`   // 连接池最大空闲连接数
	RedisPoolMaxActive  int      `json:"redis_pool_max_active"` // 连接池最大激活连接数
	RedisConnectTimeout int      `json:"redis_connect_timeout"` // 连接超时, 单位毫秒
	RedisReadTimeout    int      `json:"redis_read_timeout"`    // 读取超时, 单位毫秒
	RedisWriteTimeout   int      `json:"redis_write_timeout"`   // 写入超时, 单位毫秒
}
