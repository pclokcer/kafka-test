import { Kafka } from 'kafkajs'

const main = async () => {

  const kafka = new Kafka({
    clientId: 'my-app',
    brokers: ['localhost:9092', 'localhost:9092'],
  })

  const producer = kafka.producer()

  await producer.connect()
  setInterval(async () => {
    console.log('producer ->')
    await producer.send({
      topic: 'test-topic',
      messages: [
        { value: 'Hello KafkaJS user!' },
      ],
    })
  }, 2000)

  const consumer = kafka.consumer({ groupId: 'test-group' })

  await consumer.connect()
  await consumer.subscribe({ topic: 'test-topic', fromBeginning: true })

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      console.log('consumer -> ', {
        value: message.value.toString(),
      })
    },
  })
}

main()