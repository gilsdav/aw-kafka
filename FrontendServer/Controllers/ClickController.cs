using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;

namespace test.Kafka
{
    [Route("api/[controller]")]
    [ApiController]
    public class ClickController : ControllerBase {

        private MyProducer myProducer;

        public ClickController(MyProducer _myProducer) {
            this.myProducer = _myProducer;
        }

        [HttpPost]
        public  async Task<ActionResult<string>> Post([FromBody] string elementId)
        {
            var sended = await this.myProducer.send(elementId);
            if (sended) {
                return elementId;
            } else {
                return new StatusCodeResult(500);
            }
        }

    }
}
