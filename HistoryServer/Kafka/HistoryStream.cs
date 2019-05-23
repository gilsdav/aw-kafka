using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Microsoft.Extensions.Hosting;
using Newtonsoft.Json;

namespace test.Kafka
{
    public class HistoryStream : BackgroundService
    {
        // add-history-topic:1:1,ask-history-topic:1:1,send-history-topic:1:1
        private Producer<Null, string> sendHistoryProducer; // the string is JSON of history
        private Consumer<Ignore, string> askHistoryConsumer; // the string is the SignalR clientId
        private Consumer<Ignore, string> clickConsumer; // the string is the SignalR clientId
        private bool runing;
        private List<string> historyMock;

        public HistoryStream()
        {
            this.historyMock = new List<string>();
            #region HistoryConsumer
            var consumerConfig = new ConsumerConfig
            { 
                GroupId = "test-consumer-group",
                BootstrapServers = "localhost:9092",
                AutoOffsetReset = AutoOffsetResetType.Earliest
            };
            this.askHistoryConsumer = new Consumer<Ignore, string>(consumerConfig);
            #endregion
            #region HistoryProducer
            var producerConfig = new ProducerConfig { BootstrapServers = "localhost:9092", MessageTimeoutMs = 10000 };
            this.sendHistoryProducer = new Producer<Null, string>(producerConfig);
            #endregion
            #region ClickConsumer
            var consumerClickConfig = new ConsumerConfig
            { 
                // Reveive only by once if same groupe id : probably for clustering
                // If you change this groupeId, you will receive Kafka history : usefull when add new modules
                GroupId = "test-consumer-group-2", 
                BootstrapServers = "localhost:9092",
                AutoOffsetReset = AutoOffsetResetType.Earliest,
                EnableAutoCommit = false
            };
            this.clickConsumer = new Consumer<Ignore, string>(consumerClickConfig);
            #endregion
        }

        protected override Task ExecuteAsync(CancellationToken stoppingToken)
        {
            Console.WriteLine("execute listening history");
            return Task.WhenAll(
                Task.Run(() => this.listenAskHistory()),
                Task.Run(() => this.listenClickHistory())
            );
        }

        public override Task StopAsync(CancellationToken cancellationToken)
        {
            Console.WriteLine("stop listening history");
            this.stop();
            return base.StopAsync(cancellationToken);
        }

        private async Task listenAskHistory()
        {
            Console.WriteLine("Subscribe ask-history-topic");
            this.askHistoryConsumer.Subscribe("ask-history-topic");
            this.runing = true;
            this.askHistoryConsumer.OnError += (_, e) => this.runing = !e.IsFatal;
            while (this.runing)
            {
                try
                {
                    Console.WriteLine("Start Consume ask-history-topic");
                    var cr = this.askHistoryConsumer.Consume();
                    Console.WriteLine($"Consumed message '{cr.Value}' at: '{cr.TopicPartitionOffset}'.");
                    var clientId = cr.Value;
                    var toSend = new List<string>(this.historyMock);
                    toSend.Insert(0, clientId);
                    await this.send(toSend.ToArray());
                }
                catch (ConsumeException e)
                {
                    Console.WriteLine($"Error occured: {e.Error.Reason}");
                }
            }
            // Ensure the consumer leaves the group cleanly and final offsets are committed.
            this.stop();
        }

        private async Task listenClickHistory()
        {
            Console.WriteLine("Subscribe click for history");
            this.clickConsumer.Subscribe("click-topic");
            this.runing = true;
            this.clickConsumer.OnError += (_, e) => this.runing = !e.IsFatal;
            while (this.runing)
            {
                try
                {
                    Console.WriteLine("Start Consume click for history");
                    var cr = this.clickConsumer.Consume();
                    Console.WriteLine($"Consumed click history message '{cr.Value}' at: '{cr.TopicPartitionOffset}'.");
                    var clickValue = cr.Value;
                    // TODO: save it to DB
                    this.historyMock.Add(clickValue);
                    Console.WriteLine($"Save new history {JsonConvert.SerializeObject(this.historyMock)}");

                    this.clickConsumer.Commit();
                }
                catch (ConsumeException e)
                {
                    Console.WriteLine($"Error occured: {e.Error.Reason}");
                }
            }
            // Ensure the consumer leaves the group cleanly and final offsets are committed.
            this.stop();
        }

        private async Task<bool> send(string[] history)
        {
            Console.WriteLine("send history " + history.ToString());
            try
            {
                var jsonHistory = JsonConvert.SerializeObject(history);
                var dr = await this.sendHistoryProducer.ProduceAsync("send-history-topic", new Message<Null, string> { Value=jsonHistory });
                Console.WriteLine($"Delivered '{dr.Value.ToString()}' to '{dr.TopicPartitionOffset}'");
                return true;
            }
            catch (KafkaException e)
            {
                Console.WriteLine($"Delivery failed: {e.Error.Reason}");
                return false;
            }
        }

        private void stop()
        {
            this.runing = false;
            this.askHistoryConsumer.Close();
            this.clickConsumer.Close();
            this.sendHistoryProducer.Flush(TimeSpan.FromMilliseconds(1000));
        }
    }
}