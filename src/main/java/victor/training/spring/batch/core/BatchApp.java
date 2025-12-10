package victor.training.spring.batch.core;

import jakarta.persistence.EntityManagerFactory;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemStreamReader;
import org.springframework.batch.item.database.JpaItemWriter;
import org.springframework.batch.item.support.SynchronizedItemStreamReader;
import org.springframework.batch.item.xml.StaxEventItemReader;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.core.io.FileSystemResource;
import org.springframework.oxm.jaxb.Jaxb2Marshaller;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;
import victor.training.spring.batch.core.domain.City;
import victor.training.spring.batch.core.domain.Person;
import victor.training.spring.batch.core.extra.CaptureStartTimeListener;
import victor.training.spring.batch.core.extra.CountTotalItemsListener;
import victor.training.spring.batch.core.extra.LogProgressListener;
import victor.training.spring.batch.core.extra.LogSqlForFirstChunkListener;
import victor.training.spring.batch.util.PerformanceUtil;

import java.io.File;
import java.io.IOException;

@Slf4j
@SpringBootApplication
@EnableBatchProcessing
@RequiredArgsConstructor
public class BatchApp {
  private final JobRepository jobRepository;
  private final PlatformTransactionManager transactionManager;

  @Bean
  public Job importJob() {
    return new JobBuilder("importJob",jobRepository)
        .listener(new CaptureStartTimeListener())
        .incrementer(new RunIdIncrementer())

        // pretend steps (examples of pre-processing):
//        .start(unzipXml())
//        .next(verifyFileChecksum())
//        .next(disableDBIndexes()) // re-enabled after end, hoping you don't have concurrent users/jobs/mid-way SELECTs

//        .start(importPersonData())

        .start(importCityData())
        .next(importPersonData()) // TODO 2-pass import
        .build();
  }

  @Bean
  public Step importPersonData() {
    return new StepBuilder("importPersonData", jobRepository)
        .<PersonXml, Person>chunk(3,transactionManager)
        // ⭐️ SpBatch COMMITs after each chunk
        // => ⚠️ in your code, DON'T loose the transaction:
        // - ❌ @Transactional(propagation=
        // - ❌ insert from different threads

        // ⭐️ If a chunk fails (which?!), SpBatch retries row by row -- 1 row/commit

        // ⭐️ An error/item can be
        // a) ignored
        // b) permitted on max <5% of items
        // c) fail the job

        // Once job is failed -- strategies:
        // ⭐️ Leave in DB what was inserted until failure
        //    => can resume job from last successful chunk (to save previous time and not repeat work)
        // ⭐️ DELETE all you've previously commited from an after Listener: DELETE WHERE JOB_START_TIME=../JOB_EXECUTION_ID=..
        //    => job can be cleanly re-run from start
        //    ⚠️ Impossible if you've UPDATEd any row
        //      => do first INSERT, then DELETE when job successful

        .reader(xmlReader(null)) // [E]xtract: 1 IN object from < .xls .csv .jsonl
            .processor(personProcessor()) // [T]ransform: validate, convert 1 IN to 1 OUT @Entity (your custom code)
        .writer(jpaWriter(null)) // [L]oad: N OUT objects to > jdbc, mongo, file.. = write(List<Out>)

        .listener(new LogSqlForFirstChunkListener())
        .listener(countTotalNumberOfRecordsListener())
        .listener(progressTrackingChunkListener())

//        .taskExecutor(batchExecutor()) // TODO insert chunks on multiple threads
        .build();
  }

  @Bean
  @StepScope
  public ItemStreamReader<PersonXml> xmlReader(
      @Value("#{jobParameters['FILE_PATH']}") File inputFile
      // there's a Map in the Spring Batch context as a bean named "jobParameters"
  ) {
    log.info("Reading data from file: {}", inputFile);
    if (!inputFile.exists()) throw new IllegalArgumentException("Not Found: " + inputFile);

    StaxEventItemReader<PersonXml> reader = new StaxEventItemReader<>();
    reader.setResource(new FileSystemResource(inputFile));
    reader.setStrict(true);
    reader.setFragmentRootElementName("person"); // iterate over <person>..</person> elements
    Jaxb2Marshaller unmarshaller = new Jaxb2Marshaller();
    unmarshaller.setClassesToBeBound(PersonXml.class);
    reader.setUnmarshaller(unmarshaller);

    // parallelization of a chunk insert step requires synchronizing the reader
    // this is useful only when parallelizing the chunks
    SynchronizedItemStreamReader<PersonXml> syncReader = new SynchronizedItemStreamReader<>();
    syncReader.setDelegate(reader);
    return syncReader;
  }

  @Bean
  @StepScope
  public PersonProcessor personProcessor() {
    return new PersonProcessor();
  }

  @Bean
  public <T> JpaItemWriter<T> jpaWriter(EntityManagerFactory entityManagerFactory) {
    JpaItemWriter<T> writer = new JpaItemWriter<>();
    writer.setEntityManagerFactory(entityManagerFactory);
    return writer;
  }

  @Bean
  @StepScope
  public LogProgressListener progressTrackingChunkListener() {
    return new LogProgressListener();
  }

  @Bean
  @StepScope
  public CountTotalItemsListener countTotalNumberOfRecordsListener() {
    return new CountTotalItemsListener();
  }

  @Bean
  public ThreadPoolTaskExecutor batchExecutor() {
    ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
    executor.setCorePoolSize(8);
    executor.setMaxPoolSize(8);
    executor.setQueueCapacity(500);
    executor.setThreadNamePrefix("batch-");
    return executor;
  }

  @Bean
  public Step importCityData() { // 1 thread
    return new StepBuilder("importCityData",jobRepository)
        .<PersonXml, City>chunk(1000, transactionManager)
        .reader(xmlReader(null))
        .processor(cityMerger())
        .writer(jpaWriter(null))
        .listener(cityMerger())
        .build();
  }

  @Bean
  public CityProcessor cityMerger() {
    return new CityProcessor();
  }

  public static void main(String[] args) throws IOException {
    XmlFileGenerator.generateFile(10_000);
    int dt = PerformanceUtil.measureCall(() -> SpringApplication.run(BatchApp.class, args).close());
    System.out.println("Batch took " + dt + " ms");
  }
}