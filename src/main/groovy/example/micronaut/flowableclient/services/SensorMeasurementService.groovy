package example.micronaut.flowableclient.services

import example.micronaut.flowableclient.clients.SensorMeasurementClient
import example.micronaut.flowableclient.messages.SensorMeasurement
import example.micronaut.flowableclient.messages.SubscribeSensorMeasurementEvent
import groovy.transform.CompileStatic
import io.micronaut.context.event.ApplicationEventPublisher
import io.reactivex.BackpressureStrategy
import io.reactivex.Flowable
import io.reactivex.FlowableEmitter
import io.reactivex.FlowableOnSubscribe
import io.reactivex.annotations.NonNull
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import javax.inject.Singleton

@CompileStatic
@Singleton
class SensorMeasurementService {
    static final Logger logger = LoggerFactory.getLogger(SensorMeasurementService.class)

    ApplicationEventPublisher applicationEventPublisher
    SensorMeasurementClient sensorMeasurementClient

    SensorMeasurementService(
            ApplicationEventPublisher applicationEventPublisher,
            SensorMeasurementClient sensorMeasurementClient) {
        this.applicationEventPublisher = applicationEventPublisher
        this.sensorMeasurementClient = sensorMeasurementClient
    }

    Flowable<SensorMeasurement> getSensorMeasurements() {
        sensorMeasurementClient.sensorMeasurements
    }

    void publishSubscribeSensorMeasurementEvent() {
        applicationEventPublisher.publishEvent(new SubscribeSensorMeasurementEvent())
    }
}
