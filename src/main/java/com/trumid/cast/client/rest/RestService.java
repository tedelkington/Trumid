package com.trumid.cast.client.rest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Response;

import static javax.ws.rs.core.MediaType.APPLICATION_JSON;
import static javax.ws.rs.core.MediaType.APPLICATION_XML;

public final class RestService {
    private static final Logger log = LoggerFactory.getLogger(RestService.class);

    @GET
    @Path("{machine}")
    @Produces({APPLICATION_JSON, APPLICATION_XML})
    public Response getMachineMetric(@PathParam("machine") String machine) {
        log.info("Fetching metrics for machine {}", machine);

//        KafkaStreams kafkaStreamsks = GlobalAppState.getInstance().getKafkaStreams();
//        HostInfo thisInstance = GlobalAppState.getInstance().getHostPortInfo();
//
//        Metrics metrics = null;
//
//        StreamsMetadata metadataForMachine = ks.metadataForKey(storeName, machine, new StringSerializer());
//
//        if (metadataForMachine.host().equals(thisInstance.host()) && metadataForMachine.port() == thisInstance.port()) {
//            LOGGER.log(Level.INFO, "Querying local store for machine {}", machine);
//            metrics = getLocalMetrics(machine);
//        } else {
//            //LOGGER.log(Level.INFO, "Querying remote store for machine {0}", machine);
//            String url = "http://" + metadataForMachine.host() + ":" + metadataForMachine.port() + "/metrics/remote/" + machine;
//            metrics = Utils.getRemoteStoreState(url, 2, TimeUnit.SECONDS);
//            LOGGER.log(Level.INFO, "Metric from remote store at {0} == {1}", new Object[]{url, metrics});
//        }
//
//        return Response.ok(metrics).build();
        return null;
    }
}
