package com.example;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.messaging.Sink;
import org.springframework.integration.annotation.MessageEndpoint;
import org.springframework.integration.annotation.ServiceActivator;

@EnableBinding(Sink.class)
@SpringBootApplication
public class ConsumerApplication {

	@MessageEndpoint
	public static class MessageConsumer {

		@ServiceActivator(inputChannel = Sink.INPUT)
		public void acceptNewMessage(String msg) {
			System.out.println("Hi! " + msg);
		}

	}

	public static void main(String[] args) {
		SpringApplication.run(ConsumerApplication.class, args);
	}
}
