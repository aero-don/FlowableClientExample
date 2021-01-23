package example.micronaut.flowableclient.messages

import groovy.transform.Canonical
import groovy.transform.CompileStatic
import io.micronaut.core.annotation.Introspected

@CompileStatic
@Canonical(includeNames=true)
@Introspected
class SensorMeasurement {
    String id
    String type // will be enum later
    Double value
    Long timestamp

    SensorMeasurement(String id, String type, Double value, Long timestamp) {
        this.id = id
        this.type = type
        this.value = value
        this.timestamp = timestamp
    }
}
