var crypto = require("crypto");
var test = require("tape");
var Kafka = require(".");
var eventListener = require("./e2e/listener");

var kafkaBrokerList = process.env.KAFKA_HOST || "localhost:9092";

function getProducer(t) {
  return new Promise((res) => {
    let producer = new Kafka.Producer(
      {
        "client.id": "kafka-mocha",
        "metadata.broker.list": kafkaBrokerList,
        "fetch.wait.max.ms": 1,
        debug: "all",
        dr_cb: true,
      },
      {
        "produce.offset.report": true,
      }
    );
    producer.connect({}, function (err, d) {
      console.log("connecting p");
      t.ifError(err, "no error after connecting");
      t.equal(typeof d, "object", "metadata should be returned");
      res(producer);
    });
  });
}

function getConsumer(t) {
  return new Promise((res) => {
    const grp = "kafka-mocha-grp-" + crypto.randomBytes(20).toString("hex");
    let consumer = new Kafka.KafkaConsumer(
      {
        "metadata.broker.list": kafkaBrokerList,
        "group.id": grp,
        "fetch.wait.max.ms": 1000,
        "session.timeout.ms": 10000,
        "enable.auto.commit": false,
        "enable.partition.eof": true,
        debug: "all",
        // paused: true,
      },
      {
        "auto.offset.reset": "largest",
      }
    );

    consumer.connect({}, function (err, d) {
      console.log("connecting c");
      t.ifError(err, "no error after connecting");
      t.equal(typeof d, "object", "metadata should be returned");
      res(consumer);
    });
  });
}

function disconnectProducer(t, producer) {
  return new Promise((res) => {
    producer.disconnect(function (err) {
      t.ifError(err, "no error after disconnecting");
      res();
    });
  });
}

function disconnectConsumer(t, consumer) {
  return new Promise((res) => {
    consumer.disconnect(function (err) {
      console.log('dicsonnected;');
      t.ifError(err, "no error after disconnecting");
      res();
    });
  });
}

function assert_headers_match(t, expectedHeaders, messageHeaders) {
  t.equal(
    expectedHeaders.length,
    messageHeaders.length,
    "Headers length does not match expected length"
  );
  for (var i = 0; i < expectedHeaders.length; i++) {
    var expectedKey = Object.keys(expectedHeaders[i])[0];
    var messageKey = Object.keys(messageHeaders[i]);
    t.equal(messageKey.length, 1, "Expected only one Header key");
    t.equal(
      expectedKey,
      messageKey[0],
      "Expected key does not match message key"
    );
    var expectedValue = Buffer.isBuffer(expectedHeaders[i][expectedKey])
      ? expectedHeaders[i][expectedKey].toString()
      : expectedHeaders[i][expectedKey];
    var actualValue = messageHeaders[i][expectedKey].toString();
    t.equal(
      expectedValue.toString(),
      actualValue.toString(),
      "invalid message header"
    );
  }
}

function run_headers_test(t, headers, consumer, producer, topic) {
  return new Promise((res) => {
    var key = "key";

    crypto.randomBytes(4096, function (ex, buffer) {
      producer.setPollInterval(10);

      consumer.on("data", function (message) {
        console.log("tf", message);
        t.equal(
          buffer.toString(),
          message.value.toString(),
          "invalid message value"
        );
        t.equal(key.toString(), message.key.toString(), "invalid message key");
        t.equal(topic, message.topic, "invalid message topic");
        t.ok(message.offset >= 0, "invalid message offset");
        assert_headers_match(t, headers, message.headers);
        res();
      });

      consumer.subscribe([topic]);
      consumer.consume();

      setTimeout(function () {
        var timestamp = new Date().getTime();
        console.log("producing now", topic, key);
        producer.produce(topic, null, buffer, key, timestamp, "", headers);
      }, 2000);
    });
  });
}

test.only("should be able to produce and consume messages with one header value as float: consumeLoop", async function (t) {
  var topic = "test";
  const consumer = await getConsumer(t);
  eventListener(consumer);
  const producer = await getProducer(t);
  eventListener(producer);

  var headers = [{ key: "value" }];
  await run_headers_test(t, headers, consumer, producer, topic);
  await disconnectConsumer(t, consumer);
  await disconnectProducer(t, producer);
  t.end();
});
