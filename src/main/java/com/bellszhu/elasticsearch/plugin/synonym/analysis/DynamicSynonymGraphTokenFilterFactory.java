package com.bellszhu.elasticsearch.plugin.synonym.analysis;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.*;

public class DynamicSynonymGraphTokenFilterFactory extends DynamicSynonymTokenFilterFactory {
    private static final Logger logger = LogManager.getLogger("dynamic-synonym");

    private static ConcurrentMap<String, DynamicSynonymGraphTokenFilterFactory> instanceMap = new ConcurrentHashMap<>();

    public synchronized static DynamicSynonymGraphTokenFilterFactory getInstance(
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

        DynamicSynonymGraphTokenFilterFactory instance = instanceMap.get(location);
        if( instance != null ){
            return instance;
        }
        instance = new DynamicSynonymGraphTokenFilterFactory(
                indexSettings,
                env,
                name,
                settings
        );
        instanceMap.put(location,instance);
        return instance;
    }
    public DynamicSynonymGraphTokenFilterFactory(
            IndexSettings indexSettings, Environment env, String name, Settings settings
    ) throws IOException {
        super(indexSettings, env, name, settings);
    }

    @Override
    public TokenStream create(TokenStream tokenStream) {
        throw new IllegalStateException(
                "Call createPerAnalyzerSynonymGraphFactory to specialize this factory for an analysis chain first"
        );
    }

    @Override
    public TokenFilterFactory getChainAwareTokenFilterFactory(
            TokenizerFactory tokenizer, List<CharFilterFactory> charFilters,
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
                DynamicSynonymGraphFilter dynamicSynonymGraphFilter = new DynamicSynonymGraphFilter(
                        tokenStream, synonymMap, false);
                dynamicSynonymFilters.put(dynamicSynonymGraphFilter, synonymFile);
                return dynamicSynonymGraphFilter;
            }

            @Override
            public AnalysisMode getAnalysisMode() {
                return analysisMode;
            }
        };
    }
}
