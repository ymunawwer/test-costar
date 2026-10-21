"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
require("@azure/functions-extensions-servicebus");
const functions_1 = require("@azure/functions");
const service_bus_1 = require("@azure/service-bus");
const queueName = "orders";
// HTTP producer: send a message to Service Bus.
functions_1.app.http("sendOrder", {
    methods: ["POST"],
    authLevel: "function",
    route: "orders",
    handler: async (request, context) => {
        const connectionString = process.env.ServiceBusConnection;
        if (!connectionString) {
            return {
                status: 500,
                jsonBody: { error: "Service Bus is not configured." }
            };
        }
        let order;
        try {
            order = (await request.json());
            if (!order ||
                typeof order.orderId !== "string" ||
                typeof order.product !== "string" ||
                !Number.isInteger(order.quantity) ||
                order.quantity <= 0) {
                return {
                    status: 400,
                    jsonBody: { error: "Invalid order payload." }
                };
            }
        }
        catch {
            return {
                status: 400,
                jsonBody: { error: "Request body must be valid JSON." }
            };
        }
        const client = new service_bus_1.ServiceBusClient(connectionString);
        const sender = client.createSender(queueName);
        try {
            await sender.sendMessages({
                body: order,
                contentType: "application/json",
                messageId: order.orderId,
                subject: "OrderCreated"
            });
            context.log(`Sent order ${order.orderId}`);
            return {
                status: 202,
                jsonBody: {
                    message: "Order queued successfully",
                    orderId: order.orderId
                }
            };
        }
        catch (error) {
            context.error("Failed to send order", error);
            return {
                status: 500,
                jsonBody: { error: "Failed to send order." }
            };
        }
        finally {
            await sender.close();
            await client.close();
        }
    }
});
// Consumer: receive and explicitly settle Service Bus messages.
functions_1.app.serviceBusQueue("processOrder", {
    connection: "ServiceBusConnection",
    queueName,
    sdkBinding: true,
    cardinality: "many",
    autoCompleteMessages: false,
    handler: async (sbContext, context) => {
        for (const message of sbContext.messages) {
            try {
                // In extension v0.4+, message.body is a Buffer.
                const rawBody = Buffer.isBuffer(message.body)
                    ? message.body.toString("utf8")
                    : String(message.body);
                const order = JSON.parse(rawBody);
                if (!order.orderId ||
                    !order.product ||
                    !Number.isInteger(order.quantity) ||
                    order.quantity <= 0) {
                    await sbContext.actions.deadletter(message);
                    context.warn("Dead-lettered an invalid order");
                    continue;
                }
                // Replace this with actual business logic.
                context.log("Processing order:", {
                    orderId: order.orderId,
                    product: order.product,
                    quantity: order.quantity,
                    messageId: message.messageId,
                    deliveryCount: message.deliveryCount
                });
                // Complete only after successful processing.
                await sbContext.actions.complete(message);
                context.log(`Completed order ${order.orderId}`);
            }
            catch (error) {
                context.error("Order processing failed", error);
                // Make the message available for redelivery.
                // Production systems should use bounded retries and
                // dead-letter persistently failing messages.
                await sbContext.actions.abandon(message);
            }
        }
    }
});
