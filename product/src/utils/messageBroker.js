const amqp = require("amqplib");

class MessageBroker {
    constructor() {
        this.channel = null;
    }

    async connect() {
        console.log("Connecting to RabbitMQ...");

        // Đợi RabbitMQ khởi động (20s nếu chạy trong Docker)
        setTimeout(async() => {
            try {
                // 👉 Lấy URL từ biến môi trường .env (RABBITMQ_URL)
                const amqpUrl = process.env.RABBITMQ_URL || "amqp://admin:admin123@rabbitmq:5672";
                console.log(`Connecting to ${amqpUrl}`);

                // Kết nối RabbitMQ
                const connection = await amqp.connect(amqpUrl);

                this.channel = await connection.createChannel();
                await this.channel.assertQueue("products");

                console.log("RabbitMQ connected successfully (Product Service)");
            } catch (err) {
                console.error("Failed to connect to RabbitMQ:", err.message);
            }
        }, 20000);
    }

    async publishMessage(queue, message) {
        if (!this.channel) {
            console.error("No RabbitMQ channel available.");
            return;
        }

        try {
            await this.channel.sendToQueue(
                queue,
                Buffer.from(JSON.stringify(message))
            );
            console.log(`Message sent to queue '${queue}'`);
        } catch (err) {
            console.error("Error while publishing message:", err.message);
        }
    }

    async consumeMessage(queue, callback) {
        if (!this.channel) {
            console.error("No RabbitMQ channel available.");
            return;
        }

        try {
            await this.channel.consume(queue, (message) => {
                const content = message.content.toString();
                const parsedContent = JSON.parse(content);
                callback(parsedContent);
                this.channel.ack(message);
                console.log(`Consumed message from '${queue}'`);
            });
        } catch (err) {
            console.error("Error while consuming message:", err.message);
        }
    }
}

module.exports = new MessageBroker();