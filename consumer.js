require("dotenv").config();
const amqp = require("amqplib/callback_api");

amqp.connect(process.env.RMQ_URI, (err, connection) => {
    if (err) throw err;
    console.log(`Connected to ${process.env.RMQ_URI}`);

    connection.createChannel((err1, channel) => {
        if (err1) throw err1;

        let exchange = process.env.RMQ_EXCHANGE;
        let key = process.env.RMQ_KEY;
        let sport = process.env.SPORT;
        let date = new Date().toLocaleString();

        channel.assertExchange(exchange, "topic", { durable: true });

        let passedMarkets = [];
        let passedOdds = [];

        channel.assertQueue(
            `zavlanis-consumer-queue ${date}`,
            { exclusive: false, autoDelete: true },
            (err2, q) => {
                if (err2) throw err2;
                console.log(`Waiting for odds with name = null for ${sport}. Press CTRL+C to exit.`);
                channel.bindQueue(q.queue, exchange, key);
                channel.consume(
                    q.queue,
                    (msg) => {
                        let message = JSON.parse(msg.content.toString());
                        if (message.body.fixture.sport.name == sport) {
                            let markets = message.body.markets;
                            if (markets && markets.length > 0) {
                                for (let i = 0; i < markets.length; i++) {
                                    let index = passedMarkets.findIndex(o => o == markets[i].properties.uni_name);
                                    let odds = markets[i].odds;
                                    if (odds) {
                                        let oddName = odds.find(o => o.name == null);
                                        if (oddName) {
                                            let oddIndex = passedOdds.findIndex(o => o == oddName.properties.t);
                                            if (index == -1) {
                                                passedMarkets.push(markets[i].properties.uni_name);
                                                console.log(passedMarkets[passedMarkets.length - 1]);
                                            }
                                            if (oddIndex == -1) {
                                                passedOdds.push(oddName.properties.t);
                                                let odd = odds.find(o => o.properties.t == passedOdds[passedOdds.length - 1]);
                                                console.log(odd.properties);
                                            }
                                        }
                                    }
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
