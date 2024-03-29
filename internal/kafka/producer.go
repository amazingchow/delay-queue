package kafka

import (
	"os"

	"github.com/Shopify/sarama"
	"github.com/rs/zerolog/log"

	conf "github.com/amazingchow/delay-queue/internal/config"
)

type Producer struct {
	producer sarama.AsyncProducer
}

func newKafkaConfig(cfg *conf.KafkaService) *sarama.Config {
	conf := sarama.NewConfig()
	getKafkaAccessEnv(conf)
	return conf
}

func getKafkaAccessEnv(cfg *sarama.Config) {
	usr := os.Getenv("KAFKA_USERNAME")
	pwd := os.Getenv("KAFKA_PASSWORD")
	if usr == "" || pwd == "" {
		log.Warn().Msg("access kafka without SASL setting")
		return
	}
	cfg.Net.SASL.Enable = true
	cfg.Net.SASL.Mechanism = sarama.SASLTypePlaintext
	cfg.Net.SASL.User = usr
	cfg.Net.SASL.Password = pwd
	cfg.Net.SASL.Version = sarama.SASLHandshakeV1
}

func NewProducer(cfg *conf.KafkaService) *Producer {
	producer, err := sarama.NewAsyncProducer(cfg.KafkaBrokers, newKafkaConfig(cfg))
	if err != nil {
		log.Fatal().Err(err).Msg("failed to create kafka producer")
	}
	return &Producer{
		producer: producer,
	}
}

func (p *Producer) Publish(topic string, bytes []byte) error {
	return p.publish(topic, "", -1, bytes)
}

func (p *Producer) PublishByKey(topic, key string, bytes []byte) error {
	return p.publish(topic, key, -1, bytes)
}

func (p *Producer) PublishByPartition(topic string, partition int32, bytes []byte) error {
	return p.publish(topic, "", partition, bytes)
}

func (p *Producer) publish(topic, key string, partition int32, bytes []byte) error {
	if p.producer == nil {
		log.Warn().Msg("producer has been closed")
		return nil
	}

	message := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(bytes),
	}
	if partition >= 0 {
		message.Partition = partition
	}
	if len(key) > 0 {
		message.Key = sarama.StringEncoder(key)
	}

	select {
	case p.producer.Input() <- message:
		return nil
	case err := <-p.producer.Errors():
		return err
	}
}

func (p *Producer) Close() {
	if p.producer != nil {
		if err := p.producer.Close(); err != nil {
			log.Error().Err(err).Msg("failed to close kafka producer")
		}
		p.producer = nil
	}
}
