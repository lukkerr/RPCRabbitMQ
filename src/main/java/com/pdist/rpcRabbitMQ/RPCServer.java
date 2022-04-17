package com.pdist.rpcRabbitMQ;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class RPCServer {

    private static String RPC_QUEUE  = "row_rpc";

    public static void main(String[] args) throws IOException, TimeoutException {

        ConnectionFactory factory = new ConnectionFactory();
        factory.setUsername("guest");
        factory.setPassword("guest");

        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();
        channel.queueDeclare(RPC_QUEUE, false, false, false, null);
        channel.queuePurge(RPC_QUEUE);
        channel.basicQos(1);

        System.out.println("Servidor ligado, aguardando requisicoes RPC's .... \n");

        Object monitor = new Object();
        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            AMQP.BasicProperties replyProps = new AMQP.BasicProperties
                    .Builder()
                    .correlationId(delivery.getProperties().getCorrelationId())
                    .build();

            try {
                String deliveryMessage = new String(delivery.getBody(), "UTF-8");

                channel.basicPublish("",
                        delivery.getProperties().getReplyTo(),
                        replyProps,
                        String.format("Seja Bem-Vindo %s", deliveryMessage).getBytes());
                channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);

                String formatText = String.format("Foi recebido uma requisicao com ID '%s' e possuindo conteudo '%s'", delivery.getProperties().getCorrelationId(), deliveryMessage);
                System.out.println(formatText);
            } catch (Exception err) {
                System.out.println(err);
            }
        };

        channel.basicConsume(RPC_QUEUE, false, deliverCallback, consumerTag -> {});

        for (;;) {
            synchronized (monitor) {
                try {
                    monitor.wait();
                } catch (InterruptedException err) {
                    err.printStackTrace();
                }
            }
        }
    }
}
