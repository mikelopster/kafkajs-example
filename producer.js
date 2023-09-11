const express = require('express')
const { Kafka } = require('kafkajs')

const app = express()
const port = 3000

const kafka = new Kafka({
  clientId: 'express-app',
  brokers: ['localhost:9092', 'localhost:9092']
})

const producer = kafka.producer()

app.use(express.json())

app.post('/send', async (req, res) => {
  const { message, userId } = req.body

  const testData = {
    userId,
    message
  }

  await producer.connect()
  await producer.send({
    topic: 'test-topic',
    messages: [{
      value: JSON.stringify(testData)
    }],
  })
  await producer.disconnect()

  res.send('Message sent successfully')
})

app.listen(port, () => {
  console.log(`Express app listening at http://localhost:${port}`)
})
