package org.fcrepo.migration.idmappers;

import org.apache.commons.io.FileUtils;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;
import org.fcrepo.migration.Fedora4Client;
import org.fcrepo.migration.MigrationIDMapper;
import org.slf4j.Logger;

import java.io.File;
import java.io.IOException;

import static org.slf4j.LoggerFactory.getLogger;

/**
 * An IDMapper implementation that maps identifiers by simply creating new
 * objects in the Fedora 4 instance and thus delegating to whatever pid minter
 * is configured for the repository.  Unlike other ID mappers, just getting an
 * ID through mapObjectPath creates an object in the repository!
 *
 * Meanwhile, it maintains a persistent copy of the mapping in a Lucene Index
 * on the filesystem.
 *
 * @author Mike Durbin
 */
public class OpaqueIDMapper implements MigrationIDMapper {

    private static final Logger ID_LOGGER = getLogger("id-mapper");
    private static final Logger LOGGER = getLogger(OpaqueIDMapper.class);

    private Fedora4Client f4Client;

    /**
     * A lucene SearcherManager over an index maintained by this class.
     * For object created as part of this migration operation a document
     * exists in this index that contains a "pid" field and a "path" field.
     * The pid field is the original fedora 3 pid, the path field is the
     * path within the destination repository for that pid.
     */
    private SearcherManager searcherManager;

    /**
     * A lucene IndexWriter for the index exposed by 'searcher'.
     */
    private IndexWriter writer;

    /**
     * A constructor.
     * @param cachedIDIndexDir the directory (or null) where the index of generated pids should be maintained
     * @param f4Client a Fedora 4 client to mediate interactions with the repository
     * @throws IOException
     */
    public OpaqueIDMapper(final File cachedIDIndexDir, final Fedora4Client f4Client) throws IOException {
        this.f4Client = f4Client;
        final File indexDir;
        if (cachedIDIndexDir == null) {
            final File temp = File.createTempFile("tempfile", "basedir");
            temp.delete();
            temp.mkdir();
            indexDir = new File(temp, "index");
            LOGGER.info("No generated ID index directory specified.  Creating temporary index at \""
                    + indexDir.getAbsolutePath() + "\".");
            Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        LOGGER.info("Deleting generated ID index directory at \"" + indexDir.getAbsolutePath()
                                + "\"...");
                        FileUtils.deleteDirectory(indexDir);
                    } catch (IOException e) {
                        LOGGER.error("Unable to delete generated ID index directory at \"" + indexDir.getAbsolutePath()
                                + "\"!", e);
                        e.printStackTrace();
                    }
                }
            }));
        } else {
            indexDir = cachedIDIndexDir;
        }

        final Directory dir = FSDirectory.open(indexDir);
        if (indexDir.exists()) {
            LOGGER.warn("Index exists at \"" + indexDir.getPath() + "\" and will be used.  "
                    + "To clear index, simply delete this directory and re-run the application.");
        }
        final Analyzer analyzer = new StandardAnalyzer();
        final IndexWriterConfig iwc = new IndexWriterConfig(Version.LUCENE_4_10_3, analyzer);
        iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);
        writer = new IndexWriter(dir, iwc);
        writer.commit();

        searcherManager = new SearcherManager(writer, false, null);
    }

    /**
     *
     * @param pid a PID for a Fedora 3 object.
     * @return
     */
    @Override
    public String mapObjectPath(final String pid) {
        final String cachedPath = getCachedObjectPath(pid);
        if (cachedPath != null) {
            return cachedPath;
        }
        final String path = f4Client.createPlaceholder(null);
        this.cacheObjectPath(pid, path);
        return path;

    }

    @Override
    public String mapDatastreamPath(final String pid, final String dsid) {
        final String cachedObjectPath = getCachedObjectPath(pid);
        if (cachedObjectPath != null) {
            return cachedObjectPath + "/" + dsid;
        }
        final String path = f4Client.createPlaceholder(null);
        this.cacheObjectPath(pid, path);
        return path + "/" + dsid;
    }

    @Override
    public String getBaseURL() {
        return null;
    }

    private String getCachedObjectPath(final String pid) {
        try {
            final IndexSearcher s = searcherManager.acquire();
            try {
                final TopDocs result = s.search(new TermQuery(new Term("pid", pid)), 2);
                LOGGER.trace("Found " + result.totalHits + " hit(s) for pid=" + pid);
                if (result.totalHits == 1) {
                    return s.doc(result.scoreDocs[0].doc).get("path");
                } else if (result.totalHits < 1) {
                    return null;
                } else {
                    throw new IllegalStateException(result.totalHits
                            + " paths registered for the pid \"" + pid + "\".  ("
                            + s.doc(result.scoreDocs[0].doc).get("path") + ", "
                            + s.doc(result.scoreDocs[1].doc).get("path") + "...)");
                }
            } finally {
                searcherManager.release(s);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void cacheObjectPath(final String pid, final String path) {
        ID_LOGGER.info(pid + " --> " + f4Client.getRepositoryUrl() + path);
        try {
            final Document doc = new Document();
            doc.add(new StringField("path", path, Field.Store.YES));
            doc.add(new StringField("pid", pid, Field.Store.YES));
            LOGGER.trace("Added \"" + pid + "\" --> \"" + path + "\" to ID cache.");
            writer.addDocument(doc);
            writer.commit();
            searcherManager.maybeRefreshBlocking();
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

}
