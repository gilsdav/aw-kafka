
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Confluent.Kafka;

namespace test.Kafka
{
    public class MyProducer
    {

        private Producer<Null, string> producer;
        private Producer<Null, string> producerHistory;

        public MyProducer()
        {
            var config = new ProducerConfig { BootstrapServers = "localhost:9092", MessageTimeoutMs = 10000 };
            this.producer = new Producer<Null, string>(config);
            this.producerHistory = new Producer<Null, string>(config);
        }

        public async Task<bool> send(string text)
        {
            Console.WriteLine("test " + text);
            try
            {
                var dr = await this.producer.ProduceAsync("click-topic", new Message<Null, string> { Value=text });
                Console.WriteLine($"Delivered '{dr.Value}' to '{dr.TopicPartitionOffset}'");
                return true;
            }
            catch (KafkaException e)
            {
                Console.WriteLine($"Delivery failed: {e.Error.Reason}");
                return false;
            }
        }

        public async Task<bool> askHistory(string clientID)
        {
            Console.WriteLine("askHistory for client " + clientID);
            try
            {
                var dr = await this.producerHistory.ProduceAsync("ask-history-topic", new Message<Null, string> { Value=clientID });
                Console.WriteLine($"Delivered ask-histor '{dr.Value}' to '{dr.TopicPartitionOffset}'");
                return true;
            }
            catch (KafkaException e)
            {
                Console.WriteLine($"Delivery ask-histor failed: {e.Error.Reason}");
                return false;
            }
        }

    }
}
