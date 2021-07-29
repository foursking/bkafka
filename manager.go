package bkafka

import (
	"fmt"
	errors "github.com/foursking/berrors"
	"github.com/Shopify/sarama"
	"sort"
	"strings"
)

//TopicOffset kafka 每个分区的offset信息
type TopicOffset struct {
	PartitionIndex int32 `json:"index"`
	Offset         int64 `json:"offset"`
}

func kafkaConf() *sarama.Config {
	conf := sarama.NewConfig()
	conf.Version = Version //kafka版本号
	return conf
}


func NewClient(addrs ...string) (sarama.Client, error) {
	client, err := sarama.NewClient(addrs, kafkaConf())
	if err != nil {
		err := errors.New(ErrKafkaClusterCreate,
			fmt.Sprintf("error in create new client addrs %s, %s ", strings.Join(addrs, ","), err.Error()))
		//log.Error(err)
		return nil, err
	}
	return client, nil
}

//GetOffset 获取指定topic的offset
func GetOffset(topic string, partitions []int32) ([]TopicOffset, error) {
	client, err := NewClient()
	if err != nil {
		return nil, err
	}
	defer client.Close()

	to := make([]TopicOffset, 0, len(partitions))
	for _, v := range partitions {
		offset, err := client.GetOffset(topic, v, sarama.OffsetNewest)
		if err != nil {
		//	log.Error(err)
			continue
		}
		to = append(to, TopicOffset{
			PartitionIndex: v,
			Offset:         offset,
		})
	}
	sort.Slice(to, func(i, j int) bool {
		return to[i].PartitionIndex < to[j].PartitionIndex
	})
	return to, nil
}

