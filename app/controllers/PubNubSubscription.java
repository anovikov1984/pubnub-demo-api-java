package controllers;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.pubnub.api.PNConfiguration;
import com.pubnub.api.PubNub;
import com.pubnub.api.PubNubException;
import com.pubnub.api.callbacks.SubscribeCallback;
import com.pubnub.api.enums.PNOperationType;
import com.pubnub.api.enums.PNStatusCategory;
import com.pubnub.api.models.consumer.PNPublishResult;
import com.pubnub.api.models.consumer.PNStatus;
import com.pubnub.api.models.consumer.pubsub.PNMessageResult;
import com.pubnub.api.models.consumer.pubsub.PNPresenceEventResult;
import play.libs.Json;
import play.mvc.Controller;
import play.mvc.Result;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;

@Singleton
public class PubNubSubscription extends Controller {

    private PubNub pubnub;
    private static final String CHANNEL = "pubnub_demo_api_java_channel";
    private static final String EVENTS_CHANNEL = "pubnub_demo_api_python_events";

    @Inject
    public PubNubSubscription() {
        PNConfiguration pnconf = new PNConfiguration();
        pnconf.setPublishKey("pub-c-739aa0fc-3ed5-472b-af26-aca1b333ec52");
        pnconf.setSubscribeKey("sub-c-33f55052-190b-11e6-bfbc-02ee2ddab7fe");
        pnconf.setUuid("pubnub-demo-api-java-backend");
        this.pubnub = new PubNub(pnconf);

        StatusListener listener = new StatusListener();
        pubnub.addListener(listener);
    }

    public Result publish() {
        try {
            PNPublishResult result = pubnub.publish().channel(CHANNEL).message("hey").sync();
            return ok("success: " + result.getTimetoken().toString());
        } catch (PubNubException e) {
            return internalServerError("something went wrong(: " + e.getErrormsg());
        }
    }

    /**
     * Listen for the first message on all subscribed channels
     */
    public Result listen() {
        SubscribeListener listener = new SubscribeListener();
        pubnub.addListener(listener);

        try {
            PNMessageResult result = listener.waitForMessage();
            return ok("received message: " + result.getMessage().toString());
        } catch (InterruptedException e) {
            return internalServerError(e.getMessage());
        } finally {
            pubnub.removeListener(listener);
        }
    }

    public Result index() {
        try {
            ObjectNode result = Json.newObject();
            List<String> channels = pubnub.getSubscribedChannels();
            ArrayNode channelsArray = result.putArray("subscribed_channels");
            channels.forEach(channelsArray::add);
            return ok(result);
        } catch (Exception e) {
            return internalServerError();
        }
    }

    /**
     * Add channels to the subscription
     */
    public Result AddChannel() {
        String[] inputChannels = request().queryString().get("channel");
        ObjectNode result = Json.newObject();

        if (inputChannels.length == 0) {
            result.put("message", "Channel missing");
            return badRequest(result);
        }

        pubnub.subscribe().channels(Arrays.asList(inputChannels)).execute();

        List<String> channels = pubnub.getSubscribedChannels();
        ArrayNode channelsArray = result.putArray("subscribed_channels");
        channels.forEach(channelsArray::add);

        return ok(result);
    }

    /**
     * Remove channels from the subscription
     */
    public Result RemoveChannel() {
        String[] inputChannels = request().queryString().get("channel");
        ObjectNode result = Json.newObject();

        if (inputChannels.length == 0) {
            result.put("message", "Channel missing");
            return badRequest(result);
        }

        pubnub.unsubscribe().channels(Arrays.asList(inputChannels)).execute();

        List<String> channels = pubnub.getSubscribedChannels();
        ArrayNode channelsArray = result.putArray("subscribed_channels");
        channels.forEach(channelsArray::add);

        return ok(result);
    }

    private class StatusListener extends SubscribeCallback {

        @Override
        public void status(PubNub pubnub, PNStatus status) {
            String event = "UNDEFINED";

            if (status.getOperation() == PNOperationType.PNSubscribeOperation
                    && status.getCategory() == PNStatusCategory.PNConnectedCategory) {
                event = "subscribed";
            } else if (status.getOperation() == PNOperationType.PNUnsubscribeOperation
                    && status.getCategory() == PNStatusCategory.PNAcknowledgmentCategory) {
                event = "unsubscribed";
            }

            try {
                pubnub.publish().channel(PubNubSubscription.EVENTS_CHANNEL).message(event).sync();
            } catch (PubNubException e) {
                e.printStackTrace();
            }
        }

        @Override
        public void message(PubNub pubnub, PNMessageResult message) {

        }

        @Override
        public void presence(PubNub pubnub, PNPresenceEventResult presence) {

        }
    }

    private class SubscribeListener extends SubscribeCallback {
        private ArrayBlockingQueue<PNMessageResult> messagesQueue = new ArrayBlockingQueue<>(128);
        private ArrayBlockingQueue<PNPresenceEventResult> presenceQueue = new ArrayBlockingQueue<>(128);
        private final Boolean connected = Boolean.FALSE;
        private final Boolean unsubscribed = Boolean.FALSE;

        @Override
        public void status(PubNub pubnub, PNStatus status) {
            if (isSubscribedEvent(status)) {
                synchronized (connected) {
                    connected.notifyAll();
                }
            } else if (isUnsubscribedEvent(status)){
                synchronized (unsubscribed) {
                    unsubscribed.notifyAll();
                }
            }
        }

        @Override
        public synchronized void message(PubNub pubnub, PNMessageResult message) {
            try {
                System.out.println("msg: " + message.getMessage());
                messagesQueue.put(message);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        @Override
        public synchronized void presence(PubNub pubnub, PNPresenceEventResult presence) {
            try {
                presenceQueue.put(presence);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        private void waitForConnect() throws InterruptedException {
            synchronized (connected) {
                connected.wait();
            }
        }

        private void waitForUnsubscribe() throws InterruptedException {
            synchronized (connected) {
                connected.wait();
            }
        }

        private PNMessageResult waitForMessage() throws InterruptedException {
            return messagesQueue.take();
        }

        private PNPresenceEventResult waitForPresence() throws InterruptedException {
            return presenceQueue.take();
        }

        private boolean isSubscribedEvent(PNStatus status) {
            return status.getCategory().equals(PNStatusCategory.PNConnectedCategory);
        }

        private boolean isUnsubscribedEvent(PNStatus status) {
            return status.getCategory().equals(PNStatusCategory.PNAcknowledgmentCategory) &&
                    status.getOperation().equals(PNOperationType.PNUnsubscribeOperation);
        }
    }
}