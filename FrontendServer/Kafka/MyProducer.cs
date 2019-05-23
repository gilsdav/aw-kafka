
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
            // TODO: create an instance of Producer as "this.producer"
            // TODO: create an instance of Producer as "this.producerHistory"
        }

        public async Task<bool> send(string text)
        {
            Console.WriteLine("test " + text);
            try
            {
                // TODO: Send text over "this.producer" on topic
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
                // TODO: Send clientID over "this.producerHistory" on topic
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
