package com.example;

import org.springframework.batch.core.*;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.integration.launch.JobLaunchRequest;
import org.springframework.batch.integration.launch.JobLaunchingGateway;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.batch.JobExecutionEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.Resource;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.dsl.file.Files;
import org.springframework.integration.file.FileHeaders;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.io.File;

@SpringBootApplication
public class BatchAndIntegrationApplication {


	public static class Reservation {

		private int id;
		private String reservationName;

		public int getId() {
			return id;
		}

		public void setId(int id) {
			this.id = id;
		}

		public String getReservationName() {
			return reservationName;
		}

		public void setReservationName(String reservationName) {
			this.reservationName = reservationName;
		}
	}


	@Configuration
	@EnableBatchProcessing
	public static class BatchConfiguration {

		@Component
		public static class BatchExecutionListener
				implements ApplicationListener<JobExecutionEvent> {

			private final JdbcTemplate jdbcTemplate;

			@Autowired
			public BatchExecutionListener(JdbcTemplate jdbcTemplate) {
				this.jdbcTemplate = jdbcTemplate;
			}

			@Override
			public void onApplicationEvent(JobExecutionEvent executionEvent) {
				this.jdbcTemplate.query("select * from RESERVATION", rs -> {
					System.out.println(rs.getInt("ID") + ", " + rs.getString("RESERVATION_NAME"));
				});
				System.out.println("done!");
			}
		}


		@Bean
		DefaultLineMapper<Reservation> lineMapper() {

			DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer();
			tokenizer.setNames("id,reservationName".split(","));
			tokenizer.setDelimiter(",");

			BeanWrapperFieldSetMapper<Reservation> mapper = new BeanWrapperFieldSetMapper<>();
			mapper.setTargetType(Reservation.class);

			DefaultLineMapper<Reservation> defaultLineMapper = new DefaultLineMapper<>();
			defaultLineMapper.setFieldSetMapper(mapper);
			defaultLineMapper.setLineTokenizer(tokenizer);
			defaultLineMapper.afterPropertiesSet();

			return defaultLineMapper;
		}

		@Bean
		@StepScope
		FlatFileItemReader<Reservation> fileReader(@Value("${input:data.csv}") Resource file,
		                                           DefaultLineMapper<Reservation> lm) {
			FlatFileItemReader<Reservation> itemReader = new FlatFileItemReader<>();
			itemReader.setResource(file);
			itemReader.setLineMapper(lm);
			return itemReader;
		}

		@Bean
		ItemWriter<Reservation> jdbcWriter(DataSource dataSource) {
			JdbcBatchItemWriter<Reservation> batchItemWriter = new JdbcBatchItemWriter<>();
			batchItemWriter.setItemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>());
			batchItemWriter.setDataSource(dataSource);
			batchItemWriter.setSql("insert into RESERVATION ( ID, RESERVATION_NAME) values ( :id, :reservationName)");
			return batchItemWriter;
		}

		@Bean
		JdbcTemplate jdbcTemplate(DataSource dataSource) {
			return new JdbcTemplate(dataSource);
		}

		@Bean
		Job job(JobBuilderFactory factory, Step step) {
			return factory.get("etl")
					.start(step)
					.build();
		}

		@Bean
		Step step(StepBuilderFactory factory, ItemReader<Reservation> fileReader,
		          ItemWriter<Reservation> jdbcWriter) {
			return factory
					.get("file-to-jdbc-step")
					.<Reservation, Reservation>chunk(5)
					.reader(fileReader)
					.writer(jdbcWriter)
					.build();
		}
	}

	@Configuration
	public static class IntegrationConfiguration {


		@Bean
		IntegrationFlow integrationFlow(
				@Value("${files:${HOME}/Desktop/in}") File file,
				JobLauncher jobLauncher, Job job) {

			return IntegrationFlows.from(Files.inboundAdapter(file).autoCreateDirectory(true),
					spca -> spca.poller(spec -> spec.fixedRate(1000)))

					.handle(File.class, (in, headers) -> {

						JobParameters parameters = new JobParametersBuilder()
								.addParameter("file", new JobParameter(in.getAbsolutePath()))
								.toJobParameters();

						return MessageBuilder.withPayload(new JobLaunchRequest(job, parameters))
								.setHeader(FileHeaders.ORIGINAL_FILE, in.getAbsolutePath())
								.copyHeadersIfAbsent(headers)
								.build();
					})
					.handle(new JobLaunchingGateway(jobLauncher))
					.handle(JobExecution.class, (jobExecution, map) -> {
						System.out.println(jobExecution.toString());
						return null;
					})
					/*
					.routeToRecipients(spec -> spec
						.recipient(invalidFiles(),
								ms -> !JobExecution.class.cast(ms.getPayload()).getExitStatus().equals(ExitStatus.COMPLETED))
						.recipient(finishedJobs(),
								ms -> JobExecution.class.cast(ms.getPayload()).getExitStatus().equals(ExitStatus.COMPLETED))
					)*/
					.get();
		}
/*

		@Bean
		MessageChannel finishedJobs() {
			return MessageChannels.direct().get();
		}

		@Bean
		MessageChannel invalidFiles() {
			return MessageChannels.direct().get();
		}
*/

	}

	public static void main(String[] args) {
		SpringApplication.run(BatchAndIntegrationApplication.class, args);
	}
}















