package example.micronaut.flowableclient.clients

import example.micronaut.flowableclient.messages.SensorMeasurement
import groovy.transform.CompileStatic
import io.micronaut.http.MediaType
import io.micronaut.http.annotation.Get
import io.micronaut.http.client.annotation.Client
import reactor.core.publisher.Flux

@CompileStatic
@Client('sensors')
interface SensorMeasurementClient {
    @Get(value = "/sensors/measurements", processes = MediaType.APPLICATION_JSON_STREAM)
    Flux<SensorMeasurement> getSensorMeasurements()
}