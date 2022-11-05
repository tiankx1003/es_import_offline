package io.github.tiankx1003;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.InternalSettingsPreparer;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeValidationException;
import org.elasticsearch.transport.Netty4Plugin;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;

/**
 * @author <a href="https://github.com/tiankx1003">tiankx</a>
 * @version 1.0
 * @since 2022-11-05 21:48
 */
public class EmbeddedNode implements Closeable {
    static {
        System.setProperty("es.log4j.shutdownEnable", "false");
        System.setProperty("es.set.netty.runtime.available.processors", "false");
    }

    private final Node node;

    public static class PluginNode extends Node {

        /**
         * Constructs a node with the given settings.
         *
         * @param settings Base settings to configure the node with
         */
        public PluginNode(Settings settings) {
            super(InternalSettingsPreparer.prepareEnvironment(settings, null), Collections.singletonList(Netty4Plugin.class), false);
        }

        /**
         * If the node name was derived from the node id this is called with the
         * node name as soon as it is available so that we can register the
         * node name with the logger. If the node name defined in elasticsearch.yml
         * this is never called.
         *
         * @param nodeName node.name
         */
        @Override
        protected void registerDerivedNodeNameWithLogger(String nodeName) {

        }
    }

    public EmbeddedNode(Settings settings) {
        node = new PluginNode(settings);
    }

    /**
     * Closes this stream and releases any system resources associated
     * with it. If the stream is already closed then invoking this
     * method has no effect.
     *
     * <p> As noted in {@link AutoCloseable#close()}, cases where the
     * close may fail require careful attention. It is strongly advised
     * to relinquish the underlying resources and to internally
     * <em>mark</em> the {@code Closeable} as closed, prior to throwing
     * the {@code IOException}.
     *
     * @throws IOException if an I/O error occurs
     */
    @Override
    public void close() throws IOException {

    }

    public Node start() throws NodeValidationException {
        return node.start();
    }
}
