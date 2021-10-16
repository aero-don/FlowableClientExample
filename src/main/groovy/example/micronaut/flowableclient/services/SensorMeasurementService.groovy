package example.micronaut.flowableclient.services

import example.micronaut.flowableclient.clients.SensorMeasurementClient
import example.micronaut.flowableclient.messages.SensorMeasurement
import example.micronaut.flowableclient.messages.SubscribeSensorMeasurementEvent
import groovy.transform.CompileStatic
import io.micronaut.context.event.ApplicationEventPublisher

import org.slf4j.Logger
import org.slf4j.LoggerFactory

import jakarta.inject.Singleton
import reactor.core.publisher.Flux

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

    Flux<SensorMeasurement> getSensorMeasurements() {
        sensorMeasurementClient.sensorMeasurements
    }

    void publishSubscribeSensorMeasurementEvent() {
        applicationEventPublisher.publishEvent(new SubscribeSensorMeasurementEvent())
    }
}
