package demo;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
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
import org.springframework.core.io.Resource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;

@SpringBootApplication
@EnableBatchProcessing
public class BatchApplication {

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


	public static void main(String[] args) {
		SpringApplication.run(BatchApplication.class, args);
	}


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


