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
            // TODO: create an instance of Consumer as "this.askHistoryConsumer"
            #endregion
            #region HistoryProducer
            // TODO: create an instance of Producer as "this.sendHistoryProducer"
            #endregion
            #region ClickConsumer
            // TODO: create an instance of Consumer as "this.clickConsumer"
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
            // TODO: Subscribe "ask-history-topic" using "this.askHistoryConsumer"
            this.runing = true;
            this.askHistoryConsumer.OnError += (_, e) => this.runing = !e.IsFatal;
            while (this.runing)
            {
                try
                {
                    // TODO: consume the askHistoryConsumer and store result in "cr" and replace "bob" by the result
                    var clientId = "bob";
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
            // TODO: Subscribe "click-topic" using "this.clickConsumer"
            this.runing = true;
            this.clickConsumer.OnError += (_, e) => this.runing = !e.IsFatal;
            while (this.runing)
            {
                try
                {
                    // TODO: consume the clickConsumer and store result in "cr" and replace "bob" by the result
                    var clickValue = "bob";
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
                // TODO: Send history over "this.sendHistoryProducer" on topic 
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
            // TODO: close all consumers
            // TODO: flush producer 
        }
    }
}