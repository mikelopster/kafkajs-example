const { Kafka } = require('kafkajs')
const { Order } = require('./schema')
const axios = require('axios')

require('dotenv').config()

const LINE_API_URL = 'https://api.line.me/v2/bot/message/push'
const LINE_ACCESS_TOKEN = process.env.LINE_UID


const kafka = new Kafka({
  clientId: 'express-app',
  brokers: ['localhost:9092', 'localhost:9092'] // Adjust this if you are running inside a Docker container.
})

const consumer = kafka.consumer({ groupId: 'message-group' })

const run = async () => {
  // Consuming
  await consumer.connect()
  await consumer.subscribe({ topic: 'message-topic', fromBeginning: true })

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      console.log('=== consumer message', JSON.parse(message.value.toString()))
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
            'text': `Buy product: ${messageData.productName} successful!`
          }
        ]
      }

      try {
        const response = await axios.post(LINE_API_URL, body, { headers })
        console.log('=== LINE log', response.data)

        // send message complete = update order
        await Order.update({
          status: 'success'
        }, {
          where: {
            id: messageData.orderId
          }
        })
      } catch (error) {
        console.log('error', error.response.data)
      }
    },
  })
}

run().catch(console.error)
