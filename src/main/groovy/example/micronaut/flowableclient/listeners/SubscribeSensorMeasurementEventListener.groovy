package example.micronaut.flowableclient.listeners

import example.micronaut.flowableclient.messages.SensorMeasurement
import example.micronaut.flowableclient.messages.SubscribeSensorMeasurementEvent
import example.micronaut.flowableclient.services.SensorMeasurementService
import groovy.transform.CompileStatic
import io.micronaut.context.annotation.Context
import io.micronaut.context.annotation.Property
import io.micronaut.context.event.ApplicationEventListener
import io.micronaut.http.client.exceptions.HttpClientException
import io.micronaut.scheduling.TaskScheduler
import io.reactivex.Flowable
import io.reactivex.schedulers.Schedulers
import org.reactivestreams.Subscriber
import org.reactivestreams.Subscription
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import javax.annotation.PostConstruct
import javax.inject.Singleton
import java.time.Duration
import java.time.Instant

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
    Instant lastLogged

    SensorMeasurementService sensorMeasurementService
    TaskScheduler taskScheduler

    SubscribeSensorMeasurementEventListener(
            SensorMeasurementService sensorMeasurementService,
            TaskScheduler taskScheduler) {
        this.sensorMeasurementService = sensorMeasurementService
        this.taskScheduler =  taskScheduler
    }

    @PostConstruct
    void startSubscribeSensorMeasurement() {
        logger.info("Initializing SensorMeasurements subscription")
        onApplicationEvent(new SubscribeSensorMeasurementEvent())
    }

    @Override
    void onApplicationEvent(SubscribeSensorMeasurementEvent event) {
        logger.info("Subscribing to SensorMeasurements")
        try {
            sensorMeasurementService.sensorMeasurements
                    .subscribeOn(Schedulers.io())
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
            this.subscription.request(1)
            logger.info("SensorMeasurements subscription complete")
        }

        @Override
        void onNext(SensorMeasurement sensorMeasurement) {
            sensorMeasurementsProcessed++
            if (!lastLogged || Instant.now() >= lastLogged.plusSeconds(sensorMeasurementLogRate)) {
                logger.info("sensorMeasurementsProcessed: ${sensorMeasurementsProcessed} :: sensorMeasurement: ${sensorMeasurement}")
                lastLogged = Instant.now()
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