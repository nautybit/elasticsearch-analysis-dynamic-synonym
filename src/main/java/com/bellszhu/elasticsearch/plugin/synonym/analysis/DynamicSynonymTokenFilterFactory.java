package com.bellszhu.elasticsearch.plugin.synonym.analysis;


import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.synonym.SynonymMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.*;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.List;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

/**
 * @author bellszhu
 */
public class DynamicSynonymTokenFilterFactory extends
        AbstractTokenFilterFactory {

    private static final Logger logger = LogManager.getLogger("dynamic-synonym");

    /**
     * Static id generator
     */
    private static final AtomicInteger id = new AtomicInteger(1);
    private static final ScheduledExecutorService pool = Executors.newScheduledThreadPool(1, r -> {
        Thread thread = new Thread(r);
        thread.setName("monitor-synonym-Thread-" + id.getAndAdd(1));
        return thread;
    });
    private volatile ScheduledFuture<?> scheduledFuture;

    private final String location;
    private final boolean expand;
    private final boolean lenient;
    private final String format;
    private final int interval;
    private final boolean ignoreOffset;
    protected SynonymMap synonymMap;
    protected Map<AbsSynonymFilter, SynonymFile> dynamicSynonymFilters = new WeakHashMap<>();
    protected final Environment environment;
    protected final AnalysisMode analysisMode;
    protected String initReaderStr;

    private static ConcurrentMap<String,DynamicSynonymTokenFilterFactory> instanceMap = new ConcurrentHashMap<>();

    public synchronized static DynamicSynonymTokenFilterFactory getInstance(
            IndexSettings indexSettings,
            Environment env,
            String name,
            Settings settings
    ) throws IOException {

        String location = settings.get("synonyms_path");
        if (location == null) {
            throw new IllegalArgumentException(
                    "dynamic synonym requires `synonyms_path` to be configured");
        }

        DynamicSynonymTokenFilterFactory instance = instanceMap.get(location);
        if( instance != null ){
            return instance;
        }
        instance = new DynamicSynonymTokenFilterFactory(
                indexSettings,
                env,
                name,
                settings
        );
        instanceMap.put(location,instance);
        return instance;
    }

    public DynamicSynonymTokenFilterFactory(
            IndexSettings indexSettings,
            Environment env,
            String name,
            Settings settings
    ) throws IOException {
        super(indexSettings, name, settings);

        this.location = settings.get("synonyms_path");
        if (this.location == null) {
            throw new IllegalArgumentException(
                    "dynamic synonym requires `synonyms_path` to be configured");
        }
        if (settings.get("ignore_case") != null) {
        }

        this.interval = settings.getAsInt("interval", 60);
        this.expand = settings.getAsBoolean("expand", true);
        this.lenient = settings.getAsBoolean("lenient", false);
        this.format = settings.get("format", "");
        this.ignoreOffset = settings.getAsBoolean("ignore_offset", true);
        boolean updateable = settings.getAsBoolean("updateable", false);
        this.analysisMode = updateable ? AnalysisMode.SEARCH_TIME : AnalysisMode.ALL;
        this.environment = env;

        SynonymFile synonymFile = new RemoteSynonymFile(environment, null, expand, lenient,  format, location);
        try {
            logger.info("init synonym from {}.", location);
            Reader initReader = synonymFile.getReader();
            initReaderStr = IOUtils.toString(initReader);
        } catch (Exception e) {
            logger.error("init synonym {} error!", location, e);
            throw new IllegalArgumentException("could not init synonym", e);
        }
//
//        Exception e = new Exception("create instance of "+location);
//        e.printStackTrace();

    }

    @Override
    public AnalysisMode getAnalysisMode() {
        return this.analysisMode;
    }


    @Override
    public TokenStream create(TokenStream tokenStream) {
        throw new IllegalStateException(
                "Call getChainAwareTokenFilterFactory to specialize this factory for an analysis chain first");
    }

    public TokenFilterFactory getChainAwareTokenFilterFactory(
            TokenizerFactory tokenizer,
            List<CharFilterFactory> charFilters,
            List<TokenFilterFactory> previousTokenFilters,
            Function<String, TokenFilterFactory> allFilters
    ) {
        final Analyzer analyzer = buildSynonymAnalyzer(tokenizer, charFilters, previousTokenFilters, allFilters);
        SynonymFile synonymFile = getSynonymFile(analyzer);
        synonymMap = buildSynonyms(synonymFile);
        final String name = name();
        return new TokenFilterFactory() {
            @Override
            public String name() {
                return name;
            }

            @Override
            public TokenStream create(TokenStream tokenStream) {
                // fst is null means no synonyms
                if (synonymMap.fst == null) {
                    return tokenStream;
                }
//                DynamicSynonymFilter dynamicSynonymFilter = new DynamicSynonymFilter(tokenStream, synonymMap, false);
                DynamicSynonymFilter dynamicSynonymFilter = new DynamicSynonymFilter(tokenStream, synonymMap, false,ignoreOffset);
                dynamicSynonymFilters.put(dynamicSynonymFilter, synonymFile);

                return dynamicSynonymFilter;
            }

            @Override
            public TokenFilterFactory getSynonymFilter() {
                // In order to allow chained synonym filters, we return IDENTITY here to
                // ensure that synonyms don't get applied to the synonym map itself,
                // which doesn't support stacked input tokens
                return IDENTITY_FILTER;
            }

            @Override
            public AnalysisMode getAnalysisMode() {
                return analysisMode;
            }
        };
    }

    Analyzer buildSynonymAnalyzer(
            TokenizerFactory tokenizer,
            List<CharFilterFactory> charFilters,
            List<TokenFilterFactory> tokenFilters,
            Function<String, TokenFilterFactory> allFilters
    ) {
        return new CustomAnalyzer(
                tokenizer,
                charFilters.toArray(new CharFilterFactory[0]),
                tokenFilters.stream().map(TokenFilterFactory::getSynonymFilter).toArray(TokenFilterFactory[]::new)
        );
    }

    SynonymMap buildSynonyms(SynonymFile synonymFile) {
        try {
            Reader initReader = new StringReader(initReaderStr);
            return synonymFile.reloadSynonymMap(initReader);
        } catch (Exception e) {
            logger.error("failed to build synonyms", e);
            throw new IllegalArgumentException("failed to build synonyms", e);
        }
    }

    SynonymFile getSynonymFile(Analyzer analyzer) {
//        Exception ex = new Exception("rewrite instance of "+location);
//        ex.printStackTrace();
        logger.debug("rewrite instance of "+location);
        try {
            SynonymFile synonymFile;
            if (location.startsWith("http://") || location.startsWith("https://")) {
                synonymFile = new RemoteSynonymFile(
                        environment, analyzer, expand, lenient,  format, location);
            } else {
                synonymFile = new LocalSynonymFile(
                        environment, analyzer, expand, lenient, format, location);
            }
            if (scheduledFuture == null) {
                scheduledFuture = pool.scheduleAtFixedRate(new Monitor(synonymFile),
                                interval, interval, TimeUnit.SECONDS);
            }
            return synonymFile;
        } catch (Exception e) {
            logger.error("failed to get synonyms: " + location, e);
            throw new IllegalArgumentException("failed to get synonyms : " + location, e);
        }
    }

    public class Monitor implements Runnable {

        private SynonymFile synonymFile;

        Monitor(SynonymFile synonymFile) {
            this.synonymFile = synonymFile;
            logger.debug("create Monitor of "+location);
        }

        @Override
        public void run() {
            if (synonymFile.isNeedReloadSynonymMap()) {
                Reader monitorReader;
                try {
                    monitorReader = synonymFile.getReader();
                    initReaderStr = IOUtils.toString(monitorReader);
                    logger.info("success reload synonym");
                    //meaningless
                    for (Map.Entry<AbsSynonymFilter,SynonymFile> entry : dynamicSynonymFilters.entrySet()) {
                        AbsSynonymFilter dynamicSynonymFilter = entry.getKey();
                        SynonymFile file = entry.getValue();
                        SynonymMap map = file.reloadSynonymMap(new StringReader(initReaderStr));
                        dynamicSynonymFilter.update(map);
                    }
                } catch (Exception e) {
                    logger.error("reload remote synonym {} error!", location, e);
                    throw new IllegalArgumentException("could not reload remote synonyms file to build synonyms", e);
                }
            }
        }
    }

}
