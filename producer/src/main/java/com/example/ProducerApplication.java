package com.example;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.messaging.Source;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

@EnableBinding(Source.class)
@SpringBootApplication
public class ProducerApplication {

	@RestController
	public static class ProducingRestController {

		private final MessageChannel output;

		@RequestMapping(method = RequestMethod.POST, value = "/write/{name}")
		public void write(@PathVariable String name) {
			Message<String> nameMessage = MessageBuilder.withPayload(name).build();
			this.output.send(nameMessage);
		}

		@Autowired
		public ProducingRestController(@Qualifier(Source.OUTPUT) MessageChannel o) {
			this.output = o;
		}
	}

	public static void main(String[] args) {
		SpringApplication.run(ProducerApplication.class, args);
	}
}

