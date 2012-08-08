package it.gcap.elasticsearch.river.reddit;

import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.river.River;

/**
 *
 */
public class RedditRiverModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(River.class).to(RedditRiver.class).asEagerSingleton();
    }
}
