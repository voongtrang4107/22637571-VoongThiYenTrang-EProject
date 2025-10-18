const express = require("express");
const mongoose = require("mongoose");
const Order = require("./models/order");
const amqp = require("amqplib");
const config = require("./config");

class App {
    constructor() {
        this.app = express();
        this.connectDB();
        this.setupOrderConsumer();
    }

    // ------------------ MongoDB ------------------
    async connectDB() {
        try {
            await mongoose.connect(config.mongoURI, {
                useNewUrlParser: true,
                useUnifiedTopology: true,
            });
            console.log("MongoDB connected");
        } catch (err) {
            console.error("MongoDB connection error:", err.message);
        }
    }

    async disconnectDB() {
        await mongoose.disconnect();
        console.log("MongoDB disconnected");
    }

    // ------------------ RabbitMQ ------------------
    async setupOrderConsumer() {
        console.log("Connecting to RabbitMQ...");

        const amqpUrl = process.env.RABBITMQ_URL || "amqp://admin:admin123@rabbitmq:5672";
        const maxRetries = 10; // Thử lại tối đa 10 lần
        const delay = 5000; // Chờ 5s mỗi lần

        for (let i = 1; i <= maxRetries; i++) {
            try {
                console.log(`Attempt ${i}: connecting to ${amqpUrl}...`);
                const connection = await amqp.connect(amqpUrl);
                console.log("Connected to RabbitMQ (Order Service)");

                const channel = await connection.createChannel();
                await channel.assertQueue("orders");
                console.log("Queue 'orders' ready to consume");

                channel.consume("orders", async(data) => {
                    console.log("Consuming ORDER service");
                    const { products, username, orderId } = JSON.parse(data.content);

                    const newOrder = new Order({
                        products,
                        user: username,
                        totalPrice: products.reduce((acc, product) => acc + product.price, 0),
                    });

                    await newOrder.save();
                    channel.ack(data);
                    console.log("Order saved to DB and ACK sent to ORDER queue");

                    // Gửi thông tin sang product service
                    const { user, products: savedProducts, totalPrice } = newOrder.toJSON();
                    channel.sendToQueue(
                        "products",
                        Buffer.from(JSON.stringify({ orderId, user, products: savedProducts, totalPrice }))
                    );
                    console.log("Sent fulfilled order to PRODUCTS queue");
                });

                // ✅ Nếu kết nối thành công thì thoát vòng lặp
                break;
            } catch (err) {
                console.error(`Attempt ${i} failed: ${err.message}`);
                if (i === maxRetries) {
                    console.error("RabbitMQ connection failed after max retries.");
                    process.exit(1); // Thoát container nếu vẫn lỗi
                } else {
                    console.log(`Retrying in ${delay / 1000}s...`);
                    await new Promise((resolve) => setTimeout(resolve, delay));
                }
            }
        }
    }

    // ------------------ Server ------------------
    start() {
        this.server = this.app.listen(config.port, () =>
            console.log(`Server started on port ${config.port}`)
        );
    }

    async stop() {
        await mongoose.disconnect();
        this.server.close();
        console.log("Server stopped");
    }
}

module.exports = App;