using System;
using System.Reactive.Subjects;
using System.Threading.Tasks;
using Microsoft.AspNetCore.SignalR;
using test.Kafka;

namespace test.Hubs
{
    public interface IClickClient
    {
        Task SendClick(string clickedElement);
    }

    public class ClickHub: Hub<IClickClient>
    {
        private MyProducer myProducer;
        public Subject<string> askHistorySubject;

        public ClickHub(MyProducer _myProducer)
        {
            this.myProducer = _myProducer;
            this.askHistorySubject = new Subject<string>();
        }

        public override async Task OnConnectedAsync()
        {
            await base.OnConnectedAsync();
            await this.AskHistory();
        }

        public async Task AskHistory()
        {
            var id = this.Context.ConnectionId;
            Console.WriteLine($"SignalR AskHistory id: {id}");
            this.askHistorySubject.OnNext(id); // Test Ractive .NET
            // TODO: send kafka info
            await this.myProducer.askHistory(id);
        }
    }
}