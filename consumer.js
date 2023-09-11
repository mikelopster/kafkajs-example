const { Kafka } = require('kafkajs')
const axios = require('axios')
const LINE_API_URL = 'https://api.line.me/v2/bot/message/push'
const LINE_ACCESS_TOKEN = 'c4EllYGi1e8OiWUmJezse+ZCqs1dinkawgj+Mn/3zth6W+gfMjU2Tp5zLNFPzPAO64w3F/uycYW6EpwSq0Q2gmIY2QgSSr3SrnJeLa3b8Rc4r2mX09z4Vm/tqm689sy1J7g6/KCtBWFZKyvm70J1agdB04t89/1O/w1cDnyilFU='

const kafka = new Kafka({
  clientId: 'express-app',
  brokers: ['localhost:9092', 'localhost:9092'] // Adjust this if you are running inside a Docker container.
})

const consumer = kafka.consumer({ groupId: 'test-group' })

const run = async () => {
  // Consuming
  await consumer.connect()
  await consumer.subscribe({ topic: 'test-topic', fromBeginning: true })

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      console.log('consumer message', JSON.parse(message.value.toString()))
      const messageData = JSON.parse(message.value.toString())
      const headers = {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${LINE_ACCESS_TOKEN}`
      }

      const body = {
        'to': messageData.userId,
        'messages': [
          {
            'type': 'text',
            'text': messageData.message
          }
        ]
      }

      try {
        const response = await axios.post(LINE_API_URL, body, { headers })
        console.log(response.data)
      } catch (error) {
        console.log('error', error.response.data)
      }
    },
  })
}

const runNew = async () => {
  // Consuming
  await consumer.connect()
  await consumer.subscribe({ topic: 'test-topic', fromBeginning: true })

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      console.log('new consumer message', JSON.parse(message.value.toString()))
    },
  })
}

run().catch(console.error)
runNew().catch(console.error)
