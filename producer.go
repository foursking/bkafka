package bkafka

import (
	"fmt"
	"strings"
	errors "github.com/foursking/berrors"
	"github.com/Shopify/sarama"
	"time"
)

//SyncProducer kafka 同步消息发送
type SyncProducer struct {
	Address []string
	Version sarama.KafkaVersion
	Timeout int
	stop    chan bool
}

//AsyncProducer kafka异步消息发送
type AsyncProducer struct {
	Address []string
	Version sarama.KafkaVersion
	Timeout int
	stop    chan bool
}

//生产消息结构体
type ProducerMsg struct {
	Topic string
	Key string
	Content string
}

//SyncProducer.Stop 停止kafka发送数据
func (v *SyncProducer) Stop() {
	v.stop <- true
}

//AsyncProducer.Stop 停止kafka发送数据
func (v *AsyncProducer) Stop() {
	v.stop <- true
}

//SyncProducer.Start 同步消息模式 发送消息
func (v *SyncProducer) Start(topic string, data <-chan string, f func(topic string, partition int32, offset int64)) error {
	v.stop = make(chan bool)
	conf := kafkaConf()
	if v.Version != (sarama.KafkaVersion{}) {
		conf.Version = v.Version //kafka版本号
	} else {
		conf.Version = Version //kafka版本号
	}
	conf.Producer.Return.Successes = true
	conf.Producer.Timeout = time.Duration(v.Timeout) * time.Second
	producer, err := sarama.NewSyncProducer(v.Address, conf)

	if err != nil {
		err := errors.New(ErrKafkaSyncProducer, err.Error())
		fmt.Println(err)
		return err
	}
	defer func() {
		if err := producer.Close(); err != nil {
			fmt.Println(err)
		//	log.Error(err)
		}
	}()
	for {
		select {
		case <-v.stop:
			fmt.Println("kafka SyncProducer is stop")
			return nil
		case value := <-data:
			if value != "" {
				var msg *sarama.ProducerMessage
				if split := strings.Split(value, "#!!#"); len(split) > 1 {
					msg = &sarama.ProducerMessage{
					Topic: topic,
					Key: sarama.StringEncoder(split[0]),
					Value: sarama.StringEncoder(split[1]),
					}
				}else{
					msg = &sarama.ProducerMessage{
					Topic: topic,
					Value: sarama.StringEncoder(value),
					}
				}
				part, offset, err := producer.SendMessage(msg)
				if f != nil {
					f(topic, part, offset)
				}
				if err != nil {
				s := fmt.Sprintf("send message(%s) err=%s ", value, err)
				fmt.Println(s)
				} else {
					s := fmt.Sprintf("[%s] "+value+" send success，partition=%d, offset=%d ", topic, part, offset)
					fmt.Println(s)
				}
			}
		}
	}
}

//AsyncProducer.Start 异步消息
func (v *AsyncProducer) Start(data <-chan ProducerMsg, f func(data *sarama.ProducerError)) error {
	v.stop = make(chan bool)
	config := kafkaConf()
	config.Producer.Timeout = time.Duration(v.Timeout) * time.Second
	//等待服务器所有副本都保存成功后的响应
	config.Producer.RequiredAcks = sarama.WaitForAll
	//随机向partition发送消息
	config.Producer.Partitioner = sarama.NewRandomPartitioner
	//是否等待成功和失败后的响应,只有上面的RequireAcks设置不是NoReponse这里才有用. (异步发送不受影响)
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true
	config.Producer.Compression = sarama.CompressionLZ4

	//设置使用的kafka版本,如果低于V0_10_0_0版本,消息中的timestrap没有作用.需要消费和生产同时配置
	//注意，版本设置不对的话，kafka会返回很奇怪的错误，并且无法成功发送消息

	/*
	if v.Version != (sarama.KafkaVersion{}) {
		config.Version = v.Version
	} else {
		config.Version = Version
	}
	*/

	//使用配置,新建一个异步生产者
	producer, err := sarama.NewAsyncProducer(v.Address, config)

	if err != nil {
		//log.Errorf("sarama.NewAsyncProducer err, message=%s ", err)
		return err
	}
	defer producer.AsyncClose()
	////循环判断哪个通道发送过来数据.
	go func(p sarama.AsyncProducer) {
		for {
			select {
				//如果成功暂时不做处理吧 还没时间弄
			case <-p.Successes():
				//fmt.Println("offset: ", suc.Offset, "timestamp: ", suc.Timestamp.String(), "partitions: ", suc.Partition)
				/*
				if f != nil {
					f(suc.Topic, suc.Partition, suc.Offset)
				}
				*/

				//异常返回 p.Errors() *sarama.ProducerError
			case fail := <-p.Errors():
				if f != nil {
					f(fail)
				}
				//log.Error("err: ", fail.Err)
			}
		}
	}(producer)

	for {
		select {
		case <-v.stop:
			//fmt.Println("kafka AsyncProducer is stop")
			return nil
		case msg := <-data:
			if msg.Content != "" {
				//fmt.Println("kafka AsyncProducer is work")
					msg := &sarama.ProducerMessage{
						Topic: msg.Topic,
						Key: sarama.StringEncoder(msg.Key),
						Value: sarama.StringEncoder(msg.Content),
					}
					producer.Input() <- msg
			}
		}
	}
}
