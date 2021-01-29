package example.micronaut.flowableclient.listeners

import example.micronaut.flowableclient.messages.SensorMeasurement
import example.micronaut.flowableclient.messages.SubscribeSensorMeasurementEvent
import example.micronaut.flowableclient.services.SensorMeasurementService
import groovy.transform.CompileStatic
import io.micronaut.context.annotation.Context
import io.micronaut.context.annotation.Property
import io.micronaut.context.event.ApplicationEventListener
import io.micronaut.context.event.StartupEvent
import io.micronaut.http.client.DefaultHttpClientConfiguration
import io.micronaut.http.client.exceptions.HttpClientException
import io.micronaut.runtime.event.annotation.EventListener
import io.micronaut.scheduling.TaskExecutors
import io.micronaut.scheduling.TaskScheduler
import io.reactivex.Scheduler
import io.reactivex.schedulers.Schedulers
import org.reactivestreams.Subscriber
import org.reactivestreams.Subscription
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import javax.inject.Named
import javax.inject.Singleton
import java.time.Duration
import java.time.Instant
import java.util.concurrent.ExecutorService

@CompileStatic
@Singleton
@Context
class SubscribeSensorMeasurementEventListener implements ApplicationEventListener<SubscribeSensorMeasurementEvent> {
    static final Logger logger = LoggerFactory.getLogger(SubscribeSensorMeasurementEventListener.class)

    @Property(name='sensor.measurement.retry.wait', value='5')
    Integer sensorMeasurementRetryWait

    @Property(name='sensor.measurement.log.rate', value='1')
    Integer sensorMeasurementLogRate

    Integer sensorMeasurementsProcessed = new Integer(0)
    Instant nextLogInstant

    Scheduler scheduler
    SensorMeasurementService sensorMeasurementService
    TaskScheduler taskScheduler

    SubscribeSensorMeasurementEventListener(
            @Named(TaskExecutors.IO) ExecutorService executorService,
            SensorMeasurementService sensorMeasurementService,
            TaskScheduler taskScheduler) {
        this.scheduler = Schedulers.from(executorService)
        this.sensorMeasurementService = sensorMeasurementService
        this.taskScheduler =  taskScheduler
    }

    @EventListener
    void startSubscribeSensorMeasurement(StartupEvent startupEvent) {
        logger.info("Initializing SensorMeasurements subscription, DefaultHttpClientConfiguration.DEFAULT_READ_IDLE_TIMEOUT_MINUTES = ${DefaultHttpClientConfiguration.DEFAULT_READ_IDLE_TIMEOUT_MINUTES}")
        sensorMeasurementService.publishSubscribeSensorMeasurementEvent()
    }

    @Override
    void onApplicationEvent(SubscribeSensorMeasurementEvent event) {
        logger.info("Subscribing to SensorMeasurements")
        try {

            sensorMeasurementService.sensorMeasurements
                    .observeOn(scheduler)
                    .subscribe(sensorMeasurementSubscriber)

        } catch (Exception exception) {
            logger.error("Caught exception: ${exception.class} : ${exception.message}")
            exception.printStackTrace()
        }
    }

    Subscriber<SensorMeasurement> sensorMeasurementSubscriber = new Subscriber<SensorMeasurement>() {
        Subscription subscription

        @Override
        void onSubscribe(Subscription subscription) {
            this.subscription = subscription
            // this.subscription.request(Long.MAX_VALUE)
            this.subscription.request(1)
            logger.info("SensorMeasurements subscription complete")
        }

        @Override
        void onNext(SensorMeasurement sensorMeasurement) {
            sensorMeasurementsProcessed++
            Instant now = Instant.now()
            if (!nextLogInstant || now.plusMillis(1) >= nextLogInstant) {
                logger.info("sensorMeasurementsProcessed: ${sensorMeasurementsProcessed} :: sensorMeasurement: ${sensorMeasurement}")
                nextLogInstant = now.plusSeconds(sensorMeasurementLogRate)
            }
            subscription.request(1)
        }

        @Override
        void onError(Throwable throwable) {
            if (throwable instanceof HttpClientException && throwable.message.contains('Connect error:Connection refused')) {
                logger.warn("Subscription error: ${throwable.class} : ${throwable.message}")
                logger.warn('Retrying connection, is client configuration correct and sensor measurement producer running?')
                taskScheduler.schedule(Duration.ofSeconds(sensorMeasurementRetryWait), new Runnable() {
                    @Override
                    void run() {
                        sensorMeasurementService.publishSubscribeSensorMeasurementEvent()
                    }
                })
            } else {
                logger.error("Subscription error: ${throwable.class} : ${throwable.message}")
                throwable.printStackTrace()
            }
        }

        @Override
        void onComplete() {
            logger.warn('Done, but should not be done')
            logger.warn('Retrying connection')
            sensorMeasurementService.publishSubscribeSensorMeasurementEvent()
        }
    }

}
