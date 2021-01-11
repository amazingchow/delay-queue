package delayqueue

const (
	DefaultBucketNameFormatter           = "delay_queue_bucket_%03d"
	DefaultReadyQueueNameFormatter       = "ready_queue_for_%s"
	DefaultBucketCnt                     = 8
	DefaultTopicSetName                  = "delay_queue_topic_set"
	DefaultBlockPopFromReadyQueueTimeout = 10
)
