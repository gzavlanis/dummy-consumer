require("dotenv").config();
const amqp = require("amqplib/callback_api");

amqp.connect(process.env.RMQ_URI, (err, connection) => {
  if (err) throw err;
  console.log(`Connected to ${process.env.RMQ_URI}`);

  connection.createChannel((err1, channel) => {
    if (err1) throw err1;

    let exchange = process.env.RMQ_EXCHANGE;
    let key = process.env.RMQ_KEY;
    let eventId = process.env.EVENT;
    let date = new Date().toLocaleString();

    channel.assertExchange(exchange, "topic", { durable: true });

    channel.assertQueue(
      `zavlanis-consumer-queue ${date}`,
      { exclusive: false, autoDelete: true },
      (err2, q) => {
        if (err2) throw err2;
        console.log(`Waiting for messages. Press CTRL+C to exit.`);
        channel.bindQueue(q.queue, exchange, key);
        channel.consume(
          q.queue,
          (msg) => {
            let response = [];
            let message = JSON.parse(msg.content.toString());
            if (message.body.fixture.eventId == eventId) {
              console.log(message.body.fixture.eventId);
              let markets = message.body.markets;
              if (markets && markets.length > 0) {
                for (let i = 0; i < markets.length; i++) {
                  response.push({ name: markets[i].name });
                  response[i].odds = [];
                  for (let j = 0; j < markets[i].odds.length; j++) {
                    response[i].odds.push({
                      name: markets[i].odds[j].name,
                      value: markets[i].odds[j].value,
                      odd: markets[i].odds[j].odd,
                    });
                  }
                }
                
                for (let market of response) {
                  console.log(JSON.parse(JSON.stringify(market)));
                }
              }
            }
          },
          { noAck: true }
        );
      }
    );
  });
});
