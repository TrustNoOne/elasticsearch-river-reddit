
package it.gcap.elasticsearch.plugin.river.reddit;

import it.gcap.elasticsearch.river.reddit.RedditRiverModule;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.plugins.AbstractPlugin;
import org.elasticsearch.river.RiversModule;

/**
 *
 */
public class RedditRiverPlugin extends AbstractPlugin {

    @Inject
    public RedditRiverPlugin() {
    }

    @Override
    public String name() {
        return "reddit";
    }

    @Override
    public String description() {
        return "Reddit River Plugin";
    }

    public void onModule(RiversModule module) {
        module.registerRiver("reddit", RedditRiverModule.class);
    }
}
