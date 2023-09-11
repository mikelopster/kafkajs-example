const express = require('express')
const { Kafka } = require('kafkajs')
const { Order, Product, sequelize } = require('./schema')

const app = express()
const port = 8000

const kafka = new Kafka({
  clientId: 'express-app',
  brokers: ['localhost:9092', 'localhost:9092']
})

const producer = kafka.producer()

app.use(express.json())

app.post('/api/create-product', async (req, res) => {
  const productData = req.body
  try {
    const product = await Product.create(productData)
    res.json(product)
  } catch (error) {
    res.json({
      message: 'something wront',
      error
    })
  }
})

app.post('/api/placeorder', async (req, res) => {
  try {
    const { productId, userId } = req.body

    const product = await Product.findOne({ where: {
      id: productId
    }})

    if (product.amount <= 0) {
      res.json({
        message: 'product out of stock'
      })
      return false
    }

    // reduce amount
    product.amount -= 1
    await product.save()

    // create order with status pending
    const order = await Order.create({
      productId: product.id,
      userLineUid: userId,
      status: 'pending'
    })


    const orderData = {
      productName: product.name,
      userId,
      orderId: order.id
    }

    await producer.connect()
    await producer.send({
      topic: 'message-topic',
      messages: [{
        value: JSON.stringify(orderData)
      }],
    })
    await producer.disconnect()

    res.json({
      message: `buy product ${product.name} successful. waiting message for confirm.`
    })
  } catch (error) {
    res.json({
      message: 'something wront',
      error
    })
  }
})

app.listen(port, async () => {
  await sequelize.sync()
  console.log(`Express app listening at http://localhost:${port}`)
})
