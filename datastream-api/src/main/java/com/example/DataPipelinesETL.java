package com.example;

import com.example.common.datatypes.TaxiRide;
import com.example.common.sources.TaxiRideGenerator;
import com.example.common.utils.GeoUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;

import java.io.Serializable;
import java.time.Instant;

public class DataPipelinesETL {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> control = env
                .fromElements("DROP", "IGNORE", "HELLO", "World")
                .keyBy(String::toLowerCase);

        DataStream<String> streamOfWords = env
                .fromElements("Apache", "DROP", "Flink", "IGNORE", "HELLO", "drop", "ignore")
                .keyBy(String::toLowerCase);

        control
                .connect(streamOfWords)
                .flatMap(new ControlFunction())
                .print();

        DataStream<TaxiRide> rides = env.addSource(new TaxiRideGenerator());

        DataStream<EnrichedRide> enrichedNYCRides = rides
                .map(new Enrichment());

        enrichedNYCRides.print();

        env.execute();
    }


    public static class EnrichedRide extends TaxiRide {
        public int startCell;
        public int endCell;

        public EnrichedRide() {}

        public EnrichedRide(TaxiRide ride) {
            this.rideId = ride.rideId;
            this.isStart = ride.isStart;
            this.eventTime = ride.eventTime;
            this.startLon = ride.startLon;
            this.startLat = ride.startLat;
            this.endLon = ride.endLon;
            this.endLat = ride.endLat;
            this.passengerCnt = ride.passengerCnt;
            this.taxiId = ride.taxiId;
            this.driverId = ride.driverId;
            startCell = GeoUtils.mapToGridCell(ride.startLon, ride.startLat);
            this.endCell = GeoUtils.mapToGridCell(ride.endLon, ride.endLat);
        }

        public String toString() {
            return super.toString() + "," +
                    this.startCell + "," +
                    this.endCell;
        }
    }

    public static class Enrichment implements MapFunction<TaxiRide, EnrichedRide> {

        @Override
        public EnrichedRide map(TaxiRide taxiRide) throws Exception {
            return new EnrichedRide(taxiRide);
        }
    }

}
